package ethdataset

import (
	"bytes"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type ExportStorageConfig struct {
	WorkDir     string      `toml:"work_dir"`
	ChainConfig ChainConfig `toml:"chain_config"`
}

func ExportStorage(cfg ExportStorageConfig) {
	os.MkdirAll(cfg.WorkDir, os.ModePerm)

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	header := rawdb.ReadHeadHeader(chainDB)
	stateRoot := header.Root

	storageTable := NewStorageTable(cfg.WorkDir)
	defer storageTable.Close()

	accountTable := NewAccountTable(cfg.WorkDir)
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	log.Println("Starting iteration")

	i := 0
	nSlots := 0

	start := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		var slim SlimAccount
		addressHashBytes := iter.Key()
		buf := iter.Value()
		if err := rlp.DecodeBytes(buf, &slim); err != nil {
			log.Fatal(err)
		}
		storageRoot := common.BytesToHash(slim.Root)

		if !bytes.Equal(slim.Root, types.EmptyCodeHash.Bytes()) {
			addressHash := common.Hash(addressHashBytes)
			storageIt := newTrieIter(trie.StorageTrieID(stateRoot, addressHash, storageRoot), nil, trieDB)

			for storageIt.Next() {
				key := make([]byte, 32+32)
				copy(key, addressHashBytes)
				copy(key[32:], storageIt.Key)

				valueBytes := storageIt.Value

				storageTable.Save(key, valueBytes)
				nSlots += 1
			}
		}

		i += 1
		if i > 0 && i%100_000 == 0 {
			elapsed := time.Since(start)
			log.Printf("%v %v %v\n", i, elapsed, nSlots)
		}
	}
}
