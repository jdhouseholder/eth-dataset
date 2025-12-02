package ethdataset

import (
	"bufio"
	"math"
	// "bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	// "github.com/ethereum/go-ethereum/common"
	//"github.com/ethereum/go-ethereum/trie"
)

type GeneratePIRDatasetConfig struct {
	WorkDir string `toml:"work_dir"`
	OutDir  string `toml:"out_dir"`

	StateRoot string `toml:"state_root"`
	// NAccountShards int    `toml:"n_account_shards"`
}

type Metadata struct {
	NRecords  int `json:"n_records"`
	RecordLen int `json:"record_len"`
}

const (
	nTreeTop        int  = 5
	nBuckets        int  = 64
	computeMetadata bool = false

	sizeOfBucketIndex = 1 + 4

	maxProofLen int = 64

	sizeOfPaddingCounter = 2
)

var (
	// Don't recompute bc demo uses static dataset.
	accountMetadata      Metadata = Metadata{NRecords: 326036511, RecordLen: 83}
	proofSegmentMetadata Metadata = Metadata{NRecords: 448237052, RecordLen: 532}

	sizeOfAddressHash      int = 32
	sizeOfAccount          int = accountMetadata.RecordLen
	sizeOfAccountPirRecord int = sizeOfAddressHash + sizeOfAccount + maxProofLen*sizeOfBucketIndex
)

func WriteMetadataToFile(path, file string, metadata Metadata) {
	metadataFile, err := os.OpenFile(filepath.Join(path, file), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer metadataFile.Close()
	enc := json.NewEncoder(metadataFile)

	if err := enc.Encode(metadata); err != nil {
		log.Fatal(err)
	}
}

type FileTableMetadata struct {
	NRecords    uint32 `json:"n_records"`
	RecordSize  int    `json:"record_size"`
	StartOffset int    `json:"start_offset"`
}

type FileTable struct {
	nextId         uint32
	recordSize     int
	fullRecordSize int
	alwaysZeros    int
	metadataFile   *os.File
	dataFile       *os.File
	dataWriter     *bufio.Writer

	// Used for partitioning across column shards.
	startOffset int
}

func OpenFileTable(path, name string, recordSize, startOffset int) FileTable {
	metadataFilePath := filepath.Join(path, fmt.Sprintf("%v.metadata.json", name))
	metadataFile, err := os.OpenFile(metadataFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	dataFilePath := filepath.Join(path, fmt.Sprintf("%v.bin", name))
	dataFile, err := os.OpenFile(dataFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	dataWriter := bufio.NewWriter(dataFile)

	fullRecordSize := int(math.Ceil(float64(recordSize+sizeOfPaddingCounter)/8.0)) * 8
	alwaysZeros := fullRecordSize - recordSize - 2

	return FileTable{
		nextId:         0,
		recordSize:     recordSize,
		fullRecordSize: fullRecordSize,
		alwaysZeros:    alwaysZeros,
		metadataFile:   metadataFile,
		dataFile:       dataFile,
		dataWriter:     dataWriter,
		startOffset:    startOffset,
	}
}

func (f *FileTable) NextId() uint32 {
	return f.nextId
}

func (f *FileTable) Append(b []byte) uint32 {
	id := f.nextId
	f.nextId += 1

	if len(b) < f.recordSize {
		nPaddingInt := f.recordSize - len(b) + f.alwaysZeros
		if nPaddingInt > 65535 {
			log.Fatalf("requires more than 65535 bytes of padding, got=%v\n", nPaddingInt)
		}
		p := make([]byte, 2)
		binary.LittleEndian.PutUint16(p, uint16(nPaddingInt))
		if _, err := f.dataWriter.Write(p); err != nil {
			log.Fatal(err)
		}
		if _, err := f.dataWriter.Write(b); err != nil {
			log.Fatal(err)
		}
		if _, err := f.dataWriter.Write(make([]byte, nPaddingInt)); err != nil {
			log.Fatal(err)
		}
	} else if len(b) > f.recordSize {
		log.Fatalf("FileTable.Append: invalid record size got=%v, want=%v\n", len(b), f.recordSize)
	} else {
		p := make([]byte, 2)
		binary.LittleEndian.PutUint16(p, uint16(f.alwaysZeros))
		if _, err := f.dataWriter.Write(p); err != nil {
			log.Fatal(err)
		}
		if _, err := f.dataWriter.Write(b); err != nil {
			log.Fatal(err)
		}
		if _, err := f.dataWriter.Write(make([]byte, f.alwaysZeros)); err != nil {
			log.Fatal(err)
		}
	}

	return id
}

func (f *FileTable) WriteBlank() {
	f.nextId += 1
	b := make([]byte, f.fullRecordSize)
	if _, err := f.dataWriter.Write(b); err != nil {
		log.Fatal(err)
	}
}

func (f *FileTable) Get(rowId uint32) []byte {
	// This will interact poorly with the buffered writer, this is just for testing.
	if err := f.dataWriter.Flush(); err != nil {
		log.Fatal(err)
	}
	if _, err := f.dataFile.Seek(int64(int(rowId)*f.fullRecordSize), os.SEEK_SET); err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, f.fullRecordSize)
	if _, err := f.dataFile.Read(buf); err != nil {
		log.Fatal(err)
	}
	padding := binary.LittleEndian.Uint16(buf)

	// reset fd for write
	if _, err := f.dataFile.Seek(0, os.SEEK_END); err != nil {
		log.Fatal(err)
	}

	return buf[2 : f.fullRecordSize-int(padding)]
}

func (f *FileTable) Size() int {
	return int(f.nextId)
}

func (f *FileTable) Close() {
	if err := json.NewEncoder(f.metadataFile).Encode(FileTableMetadata{
		NRecords:    f.nextId,
		RecordSize:  f.fullRecordSize,
		StartOffset: f.startOffset,
	}); err != nil {
		log.Fatal(err)
	}
	if err := f.metadataFile.Close(); err != nil {
		log.Fatal(err)
	}

	f.dataWriter.Flush()

	if err := f.dataFile.Close(); err != nil {
		log.Fatal(err)
	}
}

func metadataOfTable(t *Table) Metadata {
	iter, err := t.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	m := Metadata{}
	start := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		buf := iter.Value()

		if len(buf) > m.RecordLen {
			m.RecordLen = len(buf)
		}

		m.NRecords += 1
		if m.NRecords > 0 && m.NRecords%100_000 == 0 {
			elapsed := time.Since(start)
			log.Printf("%v %v %v\n", m.NRecords, elapsed, m.RecordLen)
		}
	}
	return m
}

type BucketIndex struct {
	BucketId uint8
	RowId    uint32
}

func bucketIndexToBytes(bucketIndex BucketIndex) []byte {
	b := make([]byte, sizeOfBucketIndex)
	b[0] = bucketIndex.BucketId
	binary.LittleEndian.PutUint32(b[1:], bucketIndex.RowId)
	return b
}

func bucketIndexFromBytes(b []byte) BucketIndex {
	if len(b) < sizeOfBucketIndex {
		log.Fatalf("buffer too small: got %d, want %d", len(b), sizeOfBucketIndex)
	}
	return BucketIndex{
		BucketId: b[0],
		RowId:    binary.LittleEndian.Uint32(b[1:5]),
	}
}

func bucketIndexesToBytes(bucketIndexes []BucketIndex, maxBuckets int) []byte {
	b := make([]byte, maxBuckets*sizeOfBucketIndex)
	for i, bucketIndex := range bucketIndexes {
		offset := i * sizeOfBucketIndex
		b[offset] = bucketIndex.BucketId
		binary.LittleEndian.PutUint32(b[offset+1:], bucketIndex.RowId)
	}
	return b
}

func bucketIndexesFromBytes(b []byte, maxBuckets int) []BucketIndex {
	bucketIndexes := make([]BucketIndex, maxBuckets)
	for i := 0; i < maxBuckets; i++ {
		bucketIndexes[i] = BucketIndex{
			BucketId: b[i*sizeOfBucketIndex],
			RowId:    binary.LittleEndian.Uint32(b[i*sizeOfBucketIndex+1 : i*sizeOfBucketIndex+5]),
		}
	}
	return bucketIndexes
}

type BucketMapperStats struct {
	TreeTopSize int
	Max    int
	Min    int
	Spread int
}

type BucketMapper struct {
	nTreeTop int
	nBuckets int

	accountToProofIds *Table
	idToProofSegment  *Table

	treeTop              *FileTable
	treeTopProofIdToRow  map[uint64]uint32
	proofIdToBucketIndex *Table
	buckets              []FileTable

	initialRemainingBuckets []uint8
	remainingBuckets        []uint8
	roundRobinStart         int

	proofsProcessed   int
	segmentsProcessed int
	stats             BucketMapperStats
}

func NewBucketMapper(
	workDir string,
	outDir string,
	nTreeTop int,
	nBuckets int,
	bucketsMetadata Metadata,
) *BucketMapper {
	accountToProofIds := NewTable(workDir, "accountToProof")
	idToProofSegment := NewTable(workDir, "idToProofSegment")

	treeTop := OpenFileTable(outDir, "treeTop", bucketsMetadata.RecordLen, 0)
	proofIdToBucketIndex := NewTable(outDir, "proofIdToBucketIndex")
	buckets := make([]FileTable, nBuckets)
	for i := 0; i < nBuckets; i++ {
		b := OpenFileTable(outDir, fmt.Sprintf("account-proofs-%v", i), bucketsMetadata.RecordLen, 0)

		buckets[i] = b
	}

	initialRemainingBuckets := make([]uint8, nBuckets)
	for i := 0; i < nBuckets; i++ {
		initialRemainingBuckets[i] = uint8(i)
	}
	remainingBuckets := make([]uint8, nBuckets)

	return &BucketMapper{
		nTreeTop:                nTreeTop,
		nBuckets:                nBuckets,
		accountToProofIds:       accountToProofIds,
		idToProofSegment:        idToProofSegment,
		treeTop:                 &treeTop,
		treeTopProofIdToRow:     make(map[uint64]uint32),
		proofIdToBucketIndex:    proofIdToBucketIndex,
		buckets:                 buckets,
		initialRemainingBuckets: initialRemainingBuckets,
		remainingBuckets:        remainingBuckets,
	}
}

func (b *BucketMapper) getBucketIndex(proofId uint64) *BucketIndex {
	buf := b.proofIdToBucketIndex.MaybeGet(uint64ToKey(proofId))
	if buf == nil {
		return nil
	} else {
		bucketIndex := bucketIndexFromBytes(buf)
		return &bucketIndex
	}
}

func (b *BucketMapper) setBucketIndex(proofId uint64, bucketIndex *BucketIndex) {
	b.proofIdToBucketIndex.Set(uint64ToKey(proofId), bucketIndexToBytes(*bucketIndex))
}

func (b *BucketMapper) addToBucket(bucketId uint8, proofId uint64) uint32 {
	proofSegment := b.idToProofSegment.Get(uint64ToKey(proofId))
	rowId := b.buckets[bucketId].Append(proofSegment)
	return rowId
}

func (b *BucketMapper) MapAccountProofToBucketIndexes(addressHashBytes []byte) []BucketIndex {
	proofIdBytes := b.accountToProofIds.Get(addressHashBytes)
	proofIds := bytesToUint64(proofIdBytes)
	return b.MapProofToBucketIndexes(proofIds)
}

func (b *BucketMapper) MapProofToBucketIndexes(proofIds []uint64) []BucketIndex {
	copy(b.remainingBuckets, b.initialRemainingBuckets)

	var bucketIndexes []BucketIndex

	for i := 0; i < b.nTreeTop; i++ {
		proofId := proofIds[i]
		rowId, ok := b.treeTopProofIdToRow[proofId]
		if !ok {
			proofSegment := b.idToProofSegment.Get(uint64ToKey(proofId))
			rowId = b.treeTop.Append(proofSegment)
			b.treeTopProofIdToRow[proofId] = rowId
		}
		bucketIndex := BucketIndex{
			BucketId: 255,
			RowId:    rowId,
		}
		bucketIndexes = append(bucketIndexes, bucketIndex)
	}

	for i := 0; i < len(proofIds)-b.nTreeTop; i++ {
		proofId := proofIds[b.nTreeTop+i]
		bucketIndex := b.getBucketIndex(proofId)
		if bucketIndex == nil {
			mod := b.nBuckets - i

			winningJ := b.roundRobinStart % mod
			winningBucketId := b.remainingBuckets[winningJ]
			minRowId := b.buckets[winningBucketId].nextId

			for j := 1; j < mod; j++ {
				k := (b.roundRobinStart + j) % mod
				bucketId := b.remainingBuckets[k]
				rowId := b.buckets[bucketId].NextId()
				if rowId < minRowId {
					winningJ = k
					winningBucketId = bucketId
					minRowId = rowId
				}
			}
			b.roundRobinStart += 1

			rowId := b.addToBucket(winningBucketId, proofId)

			bucketIndex = &BucketIndex{
				BucketId: winningBucketId,
				RowId:    rowId,
			}
			b.setBucketIndex(proofId, bucketIndex)

			b.remainingBuckets[winningJ] = b.remainingBuckets[b.nBuckets-i-1]
		} else {
			for j := 0; j < nBuckets-i; j++ {
				if b.remainingBuckets[j] == bucketIndex.BucketId {
					b.remainingBuckets[j] = b.remainingBuckets[b.nBuckets-i-1]
					break
				}
			}
		}
		bucketIndexes = append(bucketIndexes, *bucketIndex)
	}
	return bucketIndexes
}

func (b *BucketMapper) GetProof(bucketIndexes []BucketIndex) [][]byte {
	var proofBytes [][]byte
	for _, bucketIndex := range bucketIndexes {
		var buf []byte
		if bucketIndex.BucketId == 255 {
			buf = b.treeTop.Get(bucketIndex.RowId)
		} else {
			buf = b.buckets[bucketIndex.BucketId].Get(bucketIndex.RowId)
		}
		proofBytes = append(proofBytes, buf)

	}
	return proofBytes
}

func (b *BucketMapper) Close() {
	b.accountToProofIds.Close()
	b.idToProofSegment.Close()
	b.proofIdToBucketIndex.Close()
	b.treeTop.Close()
	for _, bucket := range b.buckets {
		bucket.Close()
	}
}

func (b *BucketMapper) Stats() BucketMapperStats {
	mmin := b.buckets[0].nextId
	mmax := b.buckets[0].nextId
	for i, b := range b.buckets {
		log.Printf("BucketId=%v size=%v\n", i, b.nextId)
		if b.nextId < mmin {
			mmin = b.nextId
		}
		if b.nextId > mmax {
			mmax = b.nextId
		}
	}
	return BucketMapperStats{
		TreeTopSize: b.treeTop.Size(),
		Min:    int(mmin),
		Max:    int(mmax),
		Spread: int(mmax) - int(mmin),
	}
}

func getMetadata(accountTable, idToProofSegment *Table) (accountMetadata Metadata, proofSegmentMetadata Metadata) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		accountMetadata = metadataOfTable(accountTable)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		proofSegmentMetadata = metadataOfTable(idToProofSegment)
		wg.Done()
	}()

	wg.Wait()
	return accountMetadata, proofSegmentMetadata
}

func GeneratePIRDataset(cfg GeneratePIRDatasetConfig) {
	// stateRoot := common.HexToHash(cfg.StateRoot)

	os.MkdirAll(cfg.OutDir, os.ModePerm)

	accountTable := NewTable(cfg.WorkDir, "accounts")
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	proofBucketMapper := NewBucketMapper(
		cfg.WorkDir,
		cfg.OutDir,
		nTreeTop,
		nBuckets,
		proofSegmentMetadata,
	)
	defer proofBucketMapper.Close()

	nAccountsProcessed := 0

	nAccountShards := 8
	accountChunkSize := int(math.Ceil(float64(accountMetadata.NRecords) / float64(nAccountShards)))
	log.Printf("accountChunkSize=%v\n", accountChunkSize)

	accountShardNumber := 0

	NextAccountTable := func() *FileTable {
		t := OpenFileTable(cfg.OutDir, fmt.Sprintf("accounts-pir-%v", accountShardNumber), sizeOfAccountPirRecord, 0)
		accountShardNumber += 1
		return &t
	}
	accountPirTable := NextAccountTable()

	for iter.First(); iter.Valid(); iter.Next() {
		addressHashBytes := iter.Key()
		slimAccount := iter.Value()

		bucketIndexes := proofBucketMapper.MapAccountProofToBucketIndexes(addressHashBytes)

		buf := make([]byte, sizeOfAccountPirRecord)
		copy(buf, addressHashBytes)
		copy(buf[sizeOfAddressHash:], slimAccount)
		copy(buf[sizeOfAddressHash+sizeOfAccount:], bucketIndexesToBytes(bucketIndexes, nBuckets))

		rowId := accountPirTable.Append(buf)
		_ = rowId
		// got := accountPirTable.Get(rowId)
		// if !bytes.Equal(buf, got) {
		// 	log.Fatal("WOW")
		// }

		// gotBucketIndexes := bucketIndexesFromBytes(got[sizeOfAddressHash+sizeOfAccount:], nBuckets)

		// proofBytes := proofBucketMapper.GetProof(gotBucketIndexes)
		// if _, err := trie.VerifyProof(stateRoot, addressHashBytes, NewProofKV(proofBytes)); err != nil {
		// 	log.Fatal(err)
		// }

		if nAccountsProcessed > 0 && nAccountsProcessed%accountChunkSize == 0 {
			accountPirTable.Close()
			accountPirTable = NextAccountTable()
		}

		nAccountsProcessed += 1
		if nAccountsProcessed > 0 && nAccountsProcessed%100_000 == 0 {
			log.Printf("nAccountsProcessed=%v\n", nAccountsProcessed)
		}
	}
	accountPirTable.Close()
}
