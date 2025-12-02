package ethdataset

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type ProofTopDeduper struct {
	nextId   uint64
	m        map[common.Hash]uint64
	segments [][]byte
}

func NewProofTopDeduper() *ProofTopDeduper {
	return &ProofTopDeduper{
		m: make(map[common.Hash]uint64),
	}
}

func (d *ProofTopDeduper) Dedup(segments [][]byte) []uint64 {
	var ids []uint64
	for _, s := range segments {
		h := common.Hash(crypto.Keccak256(s))

		id, ok := d.m[h]
		if !ok {
			id = d.nextId
			d.nextId += 1
			d.m[h] = id
			d.segments = append(d.segments, s)
		}

		ids = append(ids, id)
	}
	return ids
}
