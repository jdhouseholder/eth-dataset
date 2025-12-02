package ethdataset

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type Metrics struct {
	Accounts uint64
	Slots    uint64
}

type ExportConfig struct {
	Code    bool `toml:"code"`
	Storage bool `toml:"storage"`
}

type RunConfig struct {
	WorkDir      string       `toml:"work_dir"`
	NAccounts    uint64       `toml:"n_accounts"`
	ChainConfig  ChainConfig  `toml:"chain_config"`
	ExportConfig ExportConfig `toml:"export_config"`
}

func Run(cfg RunConfig, analysisPass AnalysisPass) {
	os.MkdirAll(cfg.WorkDir, os.ModePerm)

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	header := rawdb.ReadHeadHeader(chainDB)
	stateRoot := header.Root

	accountIt := newTrieIter(trie.StateTrieID(stateRoot), nil, trieDB)

	start := time.Now()
	//proofTopDeduper := NewProofTopDeduper()

	proofDeduper := NewProofDeduper(cfg.WorkDir)
	defer proofDeduper.Close()

	accountToProof := NewAccountToProof(cfg.WorkDir)
	defer accountToProof.Close()

	codeDeduper := NewCodeDeduper(cfg.WorkDir)
	metrics := Metrics{}

	analysisPass.OnStart(StateInfo{RootHash: stateRoot})

	for accountIt.Next() {
		var stateAccount types.StateAccount
		if err := rlp.DecodeBytes(accountIt.Value, &stateAccount); err != nil {
			log.Fatal(err)
		}
		addressHashBytes := accountIt.Key

		var (
			code   []byte
			codeId uint64
		)
		if cfg.ExportConfig.Code {
			if !bytes.Equal(stateAccount.CodeHash, types.EmptyCodeHash.Bytes()) {
				code = rawdb.ReadCode(chainDB, common.BytesToHash(stateAccount.CodeHash))
			}
			codeId = codeDeduper.Dedup(stateAccount.CodeHash, code)
		}

		accountProof := accountIt.Prove()

		p := proofDeduper.NewProofContainer()
		p.DedupAll(accountProof)
		proofIds := p.AsIds()
		accountToProof.Save(addressHashBytes, proofIds)

		// p.DedupAll(accountProof)
		// proofIds := p.AsIds()

		// proofTop := accountProof[:3]
		// proofIds := proofTopDeduper.Dedup(proofTop)

		// denseElements := accountProof[3:len(accountProof)-1]
		// p.DedupAll(denseElements)
		// proofIds = append(proofIds, p.AsIds()...)

		// accountToProof.Save(addressHashBytes, proofIds)

		// Last node is always a leaf node because gt 32 bytes so can't embed.
		// accountNode := accountProof[:len(accountProof)-1]

		// // TODO: reconstruct proof and verify.
		// if err := trie.VerifyProof(stateRoot, addressHashBytes, reconstructedProof); err != nil {
		// 	log.Fatal(err)
		// }

		accountAnalysisPass := analysisPass.OnAccount(AccountInfo{
			StateAccount:    stateAccount,
			Code:            code,
			CodeId:          codeId,
			CompressedProof: p.AsIds(),
		})

		if cfg.ExportConfig.Storage {
			addressHash := common.Hash(addressHashBytes)
			storageIt := newTrieIter(trie.StorageTrieID(stateRoot, addressHash, stateAccount.Root), nil, trieDB)

			for storageIt.Next() {
				_, content, _, err := rlp.Split(storageIt.Value)
				if err != nil {
					log.Fatal(err)
				}
				keyBytes := storageIt.Key

				p := proofDeduper.NewProofContainer()
				storageProof := storageIt.Prove()
				p.DedupAll(storageProof)

				accountAnalysisPass.OnSlot(SlotInfo{
					Key:             keyBytes,
					Value:           content,
					CompressedProof: p.AsIds(),
				})

				metrics.Slots += 1
			}
		}
		accountAnalysisPass.OnComplete()

		if metrics.Accounts > 0 && metrics.Accounts%10_000 == 0 {
			elapsed := time.Since(start)
			accountsPerSec := float64(metrics.Accounts) / elapsed.Seconds()
			fmt.Printf("Accounts=%v Slots=%v Elapsed=%v AccountsPerSec=%v\n", metrics.Accounts, metrics.Slots, elapsed, accountsPerSec)
			fmt.Printf("Total=%v Unique=%v Deduped=%v\n", proofDeduper.Total(), proofDeduper.Unique(), proofDeduper.Deduped())
		}
		metrics.Accounts += 1
		if cfg.NAccounts != 0 && metrics.Accounts == cfg.NAccounts {
			break
		}
	}

	analysisPass.OnComplete(*codeDeduper, proofDeduper)
}
