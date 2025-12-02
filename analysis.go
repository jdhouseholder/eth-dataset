package ethdataset

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type StateInfo struct {
	BlockID     common.Hash
	BlockNumber uint64
	RootHash    common.Hash
}

type AccountInfo struct {
	StateAccount types.StateAccount

	CodeId uint64
	Code   []byte

	CompressedProof []uint64
}

type SlotInfo struct {
	Key             []byte
	Value           []byte
	CompressedProof []uint64
}

type AnalysisPass interface {
	OnStart(StateInfo)
	OnAccount(AccountInfo) AccountAnalysisPass
	OnComplete(CodeDeduper, ProofDB)
}

type AccountAnalysisPass interface {
	OnSlot(SlotInfo)
	OnComplete()
}

type NopAnalysis struct{}

var _ AnalysisPass = (*NopAnalysis)(nil)

func (n *NopAnalysis) OnStart(StateInfo) {}

func (n *NopAnalysis) OnAccount(AccountInfo) AccountAnalysisPass {
	return &NopAccountAnalysis{}
}

func (n *NopAnalysis) OnComplete(CodeDeduper, ProofDB) {}

type NopAccountAnalysis struct{}

var _ AccountAnalysisPass = (*NopAccountAnalysis)(nil)

func (n *NopAccountAnalysis) OnSlot(SlotInfo) {}

func (n *NopAccountAnalysis) OnComplete() {}
