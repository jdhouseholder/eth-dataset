package ethdataset

import "math"

func dedupeUint64(elements []uint64) []uint64 {
	encountered := map[uint64]bool{}
	var result []uint64
	for _, v := range elements {
		if !encountered[v] {
			encountered[v] = true
			result = append(result, v)
		}
	}
	return result
}

type FakeBucketMapper struct {
	accountToProofIds *Table
	idToProofSegment  *Table

	proofIdToRowId map[uint64]uint32

	all *FileTable
}

func NewFakeBucketMapper(
	accountToProofIds *Table,
	idToProofSegment *Table,
	outDir string,
	bucketsMetadata Metadata,
) *FakeBucketMapper {
	all := OpenFileTable(outDir, "ablation", bucketsMetadata.RecordLen, 0)
	return &FakeBucketMapper{
		accountToProofIds: accountToProofIds,
		idToProofSegment:  idToProofSegment,
		proofIdToRowId:    make(map[uint64]uint32),
		all:               &all,
	}
}

func (b *FakeBucketMapper) MapAccountProofToBucketIndexes(addressHashBytes []byte) []BucketIndex {
	proofIdBytes := b.accountToProofIds.Get(addressHashBytes)
	proofIds := bytesToUint64(proofIdBytes)
	proofIds = dedupeUint64(proofIds)

	var bucketIndexes []BucketIndex
	for _, proofId := range proofIds {
		rowId, ok := b.proofIdToRowId[proofId]
		if !ok {
			proofSegment := b.idToProofSegment.Get(uint64ToKey(proofId))
			rowId = b.all.Append(proofSegment)
			b.proofIdToRowId[proofId] = uint32(rowId)
		}
		bucketIndexes = append(bucketIndexes, BucketIndex{RowId: rowId})
	}
	for i := 0; i < 64-len(proofIds); i++ {
		bucketIndexes = append(bucketIndexes, BucketIndex{RowId: math.MaxUint32})
	}
	return bucketIndexes
}

func (b *FakeBucketMapper) Close() {
	b.all.Close()
}
