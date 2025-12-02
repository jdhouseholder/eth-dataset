package ethdataset

import "fmt"

type TableStats struct {
	NEntries uint64
	Total    uint64
	Max      uint64
}

func (ts *TableStats) Measure(size uint64) {
	ts.NEntries += 1
	ts.Total += size
	ts.Max = max(ts.Max, size)
}

type SizeAnalysis struct {
	Account TableStats
	Code    TableStats
	Slot    TableStats
	Proof   TableStats
}

var _ AnalysisPass = (*SizeAnalysis)(nil)

func (sa *SizeAnalysis) OnStart(StateInfo) {}

func (sa *SizeAnalysis) OnAccount(ai AccountInfo) AccountAnalysisPass {
	sizeOfNonce := 8
	sizeOfBalance := 32
	sizeOfRoot := 32
	sizeOfCodeHash := 32

	total := uint64(sizeOfNonce + sizeOfBalance + sizeOfRoot + sizeOfCodeHash)
	sa.Account.Measure(total)

	if len(ai.Code) > 0 {
		sizeOfCode := uint64(len(ai.Code))
		sa.Code.Measure(sizeOfCode)
	}

	sa.Proof.Measure(uint64(32 * len(ai.CompressedProof)))

	return &SizeAccountAnalysis{
		SizeAnalysis: sa,
	}
}
func (sa *SizeAnalysis) OnComplete(CodeDeduper, ProofDB) {
	fmt.Printf("%+v\n", sa)
}

type SizeAccountAnalysis struct {
	SizeAnalysis *SizeAnalysis
}

var _ AccountAnalysisPass = (*SizeAccountAnalysis)(nil)

func (sa *SizeAccountAnalysis) OnSlot(si SlotInfo) {
	total := uint64(len(si.Value))
	sa.SizeAnalysis.Slot.Measure(total)

	sa.SizeAnalysis.Proof.Measure(uint64(32 * len(si.CompressedProof)))
}

func (sa *SizeAccountAnalysis) OnComplete() {
}
