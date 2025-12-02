package ethdataset

import (
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

func newNode(dataDir string) *node.Node {
	stack, err := node.New(&node.Config{
		DataDir: dataDir,
		Name:    "geth",

		// Disable all RPC servers
		IPCPath:  "", // empty disables IPC
		HTTPHost: "", // empty disables HTTP
		WSHost:   "", // empty disables WS

		// Disable P2P completely
		P2P: p2p.Config{
			MaxPeers:    0,
			NoDiscovery: true,
			NoDial:      true,
			ListenAddr:  "", // empty means no listener
		},
	})
	if err != nil {
		log.Fatalf("Failed to create the protocol stack: %v", err)
	}
	return stack
}

func numDBHandles() int {
	limit, err := fdlimit.Maximum()
	if err != nil {
		log.Fatalf("Failed to retrieve file descriptor allowance: %v", err)
	}
	raised, err := fdlimit.Raise(uint64(limit))
	if err != nil {
		log.Fatalf("Failed to raise file descriptor allowance: %v", err)
	}
	return int(raised / 2) // Leave half for networking and other stuff
}

func newChainDB(ancientsDir string, stack *node.Node) ethdb.Database {
	options := node.DatabaseOptions{
		ReadOnly:          false,
		Cache:             1024 * 50 / 100,
		Handles:           numDBHandles(),
		AncientsDirectory: ancientsDir,
		MetricsNamespace:  "eth/db/chaindata/",
		EraDirectory:      "ancient/chain",
	}
	chainDB, err := stack.OpenDatabaseWithOptions("chaindata", options)
	if err != nil {
		log.Fatalf("Could not open database: %v", err)
	}
	return chainDB
}

func newTrieDB(stack *node.Node, chainDB ethdb.Database) *triedb.Database {
	config := &triedb.Config{
		Preimages: false,
		IsVerkle:  false,
	}

	scheme := rawdb.ReadStateScheme(chainDB)
	if scheme == rawdb.PathScheme {
		pc := *pathdb.ReadOnly
		pc.JournalDirectory = stack.ResolvePath("triedb")
		config.PathDB = &pc
	} else {
		config.HashDB = hashdb.Defaults
	}

	return triedb.NewDatabase(chainDB, config)
}

func newTrieIter(id *trie.ID, prefix []byte, trieDB *triedb.Database) *trie.Iterator {
	tr, err := trie.New(id, trieDB)
	if err != nil {
		log.Fatal(err)
	}

	var trieIt trie.NodeIterator
	if prefix != nil {
		trieIt, err = tr.NodeIteratorWithPrefix(prefix)
	} else {
		trieIt, err = tr.NodeIterator(nil)
	}
	if err != nil {
		log.Fatal(err)
	}

	it := trie.NewIterator(trieIt)
	return it
}

func newStateDBForRoot(chainDB ethdb.Database, trieDB *triedb.Database, snaptree *snapshot.Tree, root common.Hash) *state.StateDB {
	db := state.NewDatabase(trieDB, snaptree)
	statedb, err := state.New(root, db)
	if err != nil {
		log.Fatal(err)
	}
	return statedb
}
