package ethdataset

import (
	"bytes"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type VerifyAllConfig struct {
	StateRoot   string      `toml:"state_root"`
	WorkDir     string      `toml:"work_dir"`
	ChainConfig ChainConfig `toml:"chain_config"`
}

func VerifyAll(cfg VerifyAllConfig) {
	log.Println("Opening Node")
	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	newTrieDB(stack, chainDB)
	log.Println("Opened Node")

	header := rawdb.ReadHeadHeader(chainDB)
	gotStateRoot := header.Root

	stateRoot := common.HexToHash(cfg.StateRoot)
	if gotStateRoot != stateRoot {
		log.Fatalf("Got unexpected StateRoot, got=%v, want=%v\n", gotStateRoot, stateRoot)
	}
	log.Printf("Verified StateRoot=%v.\n", stateRoot)

	log.Println("Opening DBs")

	accountDB := openPebbleDB(cfg.WorkDir, "accounts")
	codeDB := openPebbleDB(cfg.WorkDir, "code")
	storageDB := openPebbleDB(cfg.WorkDir, "storage")
	accountToProof := openPebbleDB(cfg.WorkDir, "accountToProof")
	proofDB := NewProofDeduper(cfg.WorkDir)

	iter, err := accountDB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Starting iteration")

	nVerified := 0
	for iter.First(); iter.Valid(); iter.Next() {
		addressHashBytes := iter.Key()
		accountBytes := iter.Value()

		var account SlimAccount
		if err := rlp.DecodeBytes(accountBytes, &account); err != nil {
			log.Fatal(err)
		}

		if !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
			b, c, err := codeDB.Get(account.CodeHash)
			if err != nil {
				log.Fatal(err)
			}
			if len(b) == 0 {
				log.Fatalf("Missing code @ %v\n", account.CodeHash)
			}
			c.Close()
		}

		if !bytes.Equal(account.Root, types.EmptyRootHash.Bytes()) {
			storageIter, err := storageDB.NewIter(PrefixIterOptions(addressHashBytes))
			if err != nil {
				log.Fatal(err)
			}
			ok := false
			for storageIter.First(); storageIter.Valid(); storageIter.Next() {
				ok = true
			}
			if !ok {
				log.Fatal("Missing storage slots!")
			}
		}

		proofIdBytes, c, err := accountToProof.Get(addressHashBytes)
		if err != nil {
			log.Fatal(err)
		}
		proofIds := bytesToUint64(proofIdBytes)
		c.Close()

		proofBytes := proofDB.RecoverProof(proofIds)

		if _, err := trie.VerifyProof(stateRoot, addressHashBytes, NewProofKV(proofBytes)); err != nil {
			log.Fatal(err)
		}

		nVerified += 1

		if nVerified != 0 && nVerified%10_000 == 0 {
			log.Printf("Verified nVerified=%v\n", nVerified)
		}
	}

	log.Printf("Verification complete nVerified=%v\n", nVerified)

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
