package ethdataset

/*

import (
	"log"
	"sync"
)

type BucketHashSimulationConfig struct {
	StateRoot string `toml:"state_root"`
	WorkDir   string `toml:"work_dir"`
}

type HashWork struct {
	addressHashBytes []byte
	proofIds         []uint64
	nodes            []*MPTNode
}

type BucketInsert struct {
	addressHashBytes []byte
	bucketMapping    []BucketIndex
}

type SparseBucketEntry struct {
	id uint64
}

type SparseBucket struct {
	size uint64
	m    map[uint64]SparseBucketEntry
}

func NewSparseBucket(size uint64) SparseBucket {
	return SparseBucket{
		size: size,
		m:    make(map[uint64]SparseBucketEntry),
	}
}

func (s *SparseBucket) Get(i uint64) (SparseBucketEntry, bool) {
	e, ok := s.m[i]
	return e, ok
}

func (s *SparseBucket) Set(i, id uint64) {
	s.m[i] = SparseBucketEntry{id}
}

func BucketHashSimulation(cfg BucketHashSimulationConfig) {
	nBuckets := uint64(64)
	nRows := uint64(1_000_000_000)

	buckets := make([]SparseBucket, nBuckets)
	for i := uint64(0); i < nBuckets; i++ {
		buckets[i] = NewSparseBucket(uint64(nRows))
	}

	accountToProof := openPebbleDB(cfg.WorkDir, "accountToProof")
	defer accountToProof.Close()

	idToProofSegment := NewTable(cfg.WorkDir, "idToProofSegment")
	defer idToProofSegment.Close()

	iter, err := accountToProof.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	log.Println("Starting iteration")

	var wg sync.WaitGroup

	hashWork := make(chan HashWork)
	bucketInsert := make(chan BucketInsert)

	wg.Add(1)
	go func() {
		for iter.First(); iter.Valid(); iter.Next() {
			addressHashBytes := make([]byte, 32)
			copy(addressHashBytes, iter.Key())

			proofIdBytes := iter.Value()
			proofIds := bytesToUint64(proofIdBytes)
			var nodes []*MPTNode
			for _, id := range proofIds {
				proofSegment := idToProofSegment.Get(uint64ToLEKey(id))
				node, err := ParseNode(proofSegment)
				if err != nil {
					log.Fatal(err)
				}
				nodes = append(nodes, node)
			}

			hashWork <- HashWork{addressHashBytes, proofIds, nodes}
		}
		close(hashWork)
		wg.Done()
	}()

	for i := 0; i < 26; i++ {
		wg.Add(1)
		go func() {
			for work := range hashWork {
				bucketMapping := BucketsForMPTKey3(
					"bucket",
					work.addressHashBytes,
					work.proofIds,
					work.nodes,
					nBuckets,
					nRows,
				)

				bucketInsert <- BucketInsert{
					addressHashBytes: work.addressHashBytes,
					bucketMapping:    bucketMapping,
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(bucketInsert)
	}()

	proofsProcessed := 0
	segmentsProcessed := 0
	for work := range bucketInsert {
		for _, bucketIndex := range work.bucketMapping {
			bucket := buckets[bucketIndex.BucketId]
			got, present := bucket.Get(bucketIndex.RowId)
			if !present {
				segmentsProcessed += 1
				bucket.Set(bucketIndex.RowId, bucketIndex.ProofId)
			} else if got.id != bucketIndex.ProofId {
				gotPs := idToProofSegment.Get(uint64ToLEKey(got.id))
				wantPs := idToProofSegment.Get(uint64ToLEKey(bucketIndex.ProofId))
				log.Printf("%x\n", gotPs)
				log.Printf("%x\n", wantPs)
				log.Printf("Collision at (%v, %v): ProofsProcessed=%v SegmentsProcessed=%v\n", bucketIndex.BucketId, bucketIndex.RowId, proofsProcessed, segmentsProcessed)
			}
		}
		proofsProcessed += 1
		if segmentsProcessed != 0 && segmentsProcessed%1000 == 0 {
			log.Printf("ProofsProcessed=%v SegmentsProcessed=%v\n", proofsProcessed, segmentsProcessed)
		}
	}
}
*/
