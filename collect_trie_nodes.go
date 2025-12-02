package ethdataset

/*

import (
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

type NodeTracker struct {
	nextId uint64
	m map[common.Hash]uint64
}

func (nt *NodeTracker) Track(nodeHash common.Hash) uint64 {
	id, ok := nt.m[nodeHash]
	if !ok {
		id = nt.nextId
		nt.nextId += 1
		nt.m[nodeHash] = nt.nextId
	}
	return id
}

func CollectTrieNodes(cfg RunConfig) {
	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	head := rawdb.ReadHeadBlock(chainDB)
	root := head.Header().Root

	t, err := trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		log.Fatal(err)
	}

	m := make(map[common.Hash]uint64)
	var (
		nextId uint64
		v [][]byte
	)
	for nodeIterator.Next(true) {
		hash := nodeIterator.Hash()
		_, ok := m[hash]
		if !ok {
			id := nextId
			nextId += 1

			m[hash] = id

			blob := nodeIterator.NodeBlob()
			v = append(v, blob)
		}
	}
}
*/
