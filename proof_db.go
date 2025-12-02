package ethdataset

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type ProofKV struct {
	proof [][]byte
	m     map[common.Hash][]byte
}

func NewProofKV(proof [][]byte) *ProofKV {
	m := make(map[common.Hash][]byte, len(proof)+1)

	for _, s := range proof {
		key := crypto.Keccak256Hash(s)
		m[key] = s
	}

	return &ProofKV{
		proof,
		m,
	}
}

func (p *ProofKV) Has(key []byte) (bool, error) {
	_, ok := p.m[common.BytesToHash(key)]
	return ok, nil
}

func (p *ProofKV) Get(key []byte) ([]byte, error) {
	b, _ := p.m[common.BytesToHash(key)]
	return b, nil
}
