package ethdataset

/*
import (
	"io"
	"log"
	"os"
	"time"

	"github.com/golang/snappy"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

type DumpConfig struct {
	Accounts bool
	Code     bool
	State    bool
}

type ColumnFile struct {
	File *os.File
	io.WriteCloser
}

func NewColumnFile(path string) *ColumnFile {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return &ColumnFile{
		File:        file,
		WriteCloser: snappy.NewBufferedWriter(file),
	}
}

type KeyValue struct {
	Key   *ColumnFile
	Value *ColumnFile
}

func NewKeyValue(path string) *KeyValue {
	return &KeyValue{
		Key:   NewColumnFile(path + "_key.bin.sz"),
		Value: NewColumnFile(path + "_value.bin.sz"),
	}
}

func (kv *KeyValue) Write(key, value []byte) (int, error) {
	if _, err := kv.Key.Write(key); err != nil {
		log.Fatal(err)
	}
	if _, err := kv.Value.Write(value); err != nil {
		log.Fatal(err)
	}
	return 0, nil
}

func (kv *KeyValue) Close() error {
	if err := kv.Key.Close(); err != nil {
		log.Fatal(err)
	}
	if err := kv.Value.Close(); err != nil {
		log.Fatal(err)
	}
	return nil
}

type Dump struct {
	Account *KeyValue
	Code    *KeyValue
	State   *KeyValue
}

type SlimAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     []byte
	CodeHash []byte
}

func CollectKeys(cfg RunConfig) {
	// dumpConfig := DumpConfig {
	// 	Code: true,
	// }

	proofDeduper := NewProofDeduper("./ohno")

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	// header := rawdb.ReadHeadHeader(chainDB)
	// stateRoot := header.Root
	stateRoot := rawdb.ReadSnapshotRoot(chainDB)

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

	tr, err := trie.New(trie.StateTrieID(stateRoot), trieDB)
	if err != nil {
		log.Fatal(err)
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

		if slim.Balance == nil {
			slim.Balance = uint256.NewInt(0)
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

		// c := proofDeduper.NewProofContainer()
		// if err := tr.Prove(addressHashBytes, &c); err != nil {
		// 	log.Fatal(err)
		// }

		nHashBytes += len(addressHashBytes)
		nAccountBytes += len(b)

		i += 1
		if i%10_000 == 0 {
			log.Printf("%v nHashBytes=%v nAccountBytes=%v @ %v\n", i, nHashBytes, nAccountBytes, time.Since(start))
		}
	}
	log.Printf("Total=%v\n", i)
}
*/
