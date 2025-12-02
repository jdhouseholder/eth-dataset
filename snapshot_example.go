package ethdataset

import (
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

type DumpConfig struct {
	Accounts bool
	Code     bool
	State    bool
}

type Dump struct {
	Account *KeyValue
	Code    *KeyValue
	State   *KeyValue
}

// TODO: Note that we can't use snapshot bc it doesn't match current state trie
func IterSnapshot(cfg RunConfig) {
	// dumpConfig := DumpConfig {
	// 	Code: true,
	// }

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	header := rawdb.ReadHeadHeader(chainDB)
	stateRoot := header.Root

	log.Printf("Opening snapshot at stateRoot=%v", stateRoot)

	snapshotConfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   true,
		NoBuild:    false,
		AsyncBuild: false,
	}
	snaptree, err := snapshot.New(snapshotConfig, chainDB, trieDB, stateRoot)
	if err != nil {
		log.Fatalf("Unable to build snapshot: %v\n", err)
	}

	log.Println("Opening iteration")
	accountIt, err := snaptree.AccountIterator(stateRoot, common.Hash{})
	if err != nil {
		log.Fatalf("Unable to build AccountIterator: %v\n", err)
	}

	log.Printf("Starting iteration\n")

	accountKV := NewKeyValue("./accounts")
	defer accountKV.Close()

	i := 0
	nHashBytes := 0
	nAccountBytes := 0
	start := time.Now()

	for accountIt.Next() {
		addressHashBytes := accountIt.Hash().Bytes()

		accountBytes := accountIt.Account()
		var slim SlimAccount
		if err := rlp.DecodeBytes(accountBytes, &slim); err != nil {
			log.Fatal(err)
		}

		if slim.Root == nil || len(slim.Root) == 0 {
			slim.Root = types.EmptyRootHash.Bytes()
		}
		if slim.CodeHash == nil || len(slim.CodeHash) == 0 {
			slim.CodeHash = types.EmptyCodeHash.Bytes()
		}

		b, err := rlp.EncodeToBytes(slim)
		if err != nil {
			log.Fatal(err)
		}

		accountKV.Write(addressHashBytes, b)

		nHashBytes += len(addressHashBytes)
		nAccountBytes += len(b)

		i += 1
		if i%10_000 == 0 {
			log.Printf("%v nHashBytes=%v nAccountBytes=%v @ %v\n", i, nHashBytes, nAccountBytes, time.Since(start))
		}
	}
	log.Printf("Total=%v\n", i)
}
