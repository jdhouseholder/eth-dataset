package ethdataset

import (
	"bytes"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type ExportStorageProofsConfig struct {
	InputDir    string      `toml:"input_dir"`
	OutputDir   string      `toml:"output_dir"`
	ChainConfig ChainConfig `toml:"chain_config"`
}

func ExportStorageProofs(cfg ExportStorageProofsConfig) {
	os.MkdirAll(cfg.OutputDir, os.ModePerm)

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	header := rawdb.ReadHeadHeader(chainDB)
	stateRoot := header.Root

	proofDB := NewScopedProofDB(cfg.OutputDir, "storage")
	slotAndIndexToProofIds := NewTable(cfg.OutputDir, "slotAndIndexToProofIds")
	defer slotAndIndexToProofIds.Close()

	accountTable := NewAccountTable(cfg.InputDir)
	defer accountTable.Close()

	var (
		wg    sync.WaitGroup
		total atomic.Uint64
	)

	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			i := 0
			nSlots := 0

			start := time.Now()

			log.Printf("Starting iteration on worker %v\n", w)

			for p := 0; p < 16; p++ {
				prefix := []byte{byte(w<<4) | byte(p)}
				log.Printf("Worker=%v starting on prefix=%x\n", w, prefix)
				iter, err := accountTable.DB.NewIter(PrefixIterOptions(prefix))
				if err != nil {
					log.Fatal(err)
				}
				defer iter.Close()

				for iter.First(); iter.Valid(); iter.Next() {
					var slim SlimAccount
					addressHashBytes := iter.Key()
					buf := iter.Value()
					if err := rlp.DecodeBytes(buf, &slim); err != nil {
						log.Fatal(err)
					}
					if !bytes.Equal(slim.Root, types.EmptyCodeHash.Bytes()) {
						storageRoot := common.BytesToHash(slim.Root)
						addressHash := common.Hash(addressHashBytes)
						storageIt := newTrieIter(trie.StorageTrieID(stateRoot, addressHash, storageRoot), nil, trieDB)

						for storageIt.Next() {
							key := make([]byte, 32+32)
							copy(key, addressHashBytes)
							copy(key[32:], storageIt.Key)

							if !slotAndIndexToProofIds.Contains(key) {
								proof := storageIt.Prove()
								pc := proofDB.NewProofContainer()
								pc.DedupAll(proof)

								proofIds := pc.AsIds()
								slotAndIndexToProofIds.Set(key, uint64SliceToBytesUnsafe(proofIds))

								nSlots += 1
							}
						}
					}

					i += 1
					t := total.Add(1)
					if i > 0 && i%100_000 == 0 {
						elapsed := time.Since(start)
						log.Printf("%v %v %v total=%v nSlots=%v\n", p, i, elapsed, t, nSlots)
					}
				}
			}
			wg.Done()
		}(w)
	}

	wg.Wait()

	log.Printf("Finished. %v\n", total.Load())
}
