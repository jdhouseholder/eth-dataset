package ethdataset

import (
	"log"
)

type BucketSimluationConfig struct {
	StateRoot string `toml:"state_root"`
	WorkDir   string `toml:"work_dir"`
}

type Bucket struct {
	nextId uint32
	// a      []uint32
}

func (s *Bucket) Append(v uint32) uint32 {
	id := s.nextId
	s.nextId += 1
	// s.a = append(s.a, v)
	return id
}

func BucketSimluation(cfg BucketSimluationConfig) {
	nTreeTop := 3
	nBuckets := 64
	buckets := make([]Bucket, nBuckets)

	accountToProof := openPebbleDB(cfg.WorkDir, "accountToProof")
	defer accountToProof.Close()

	iter, err := accountToProof.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	proofsProcessed := 0
	segmentsProcessed := 0

	proofIdToBucketIndex := make([]BucketIndex, 1_884_406_025)

	log.Println("Starting iteration")

	initialRemainingBuckets := make([]uint8, nBuckets)
	for i := 0; i < nBuckets; i++ {
		initialRemainingBuckets[i] = uint8(i)
	}
	remainingBuckets := make([]uint8, nBuckets)

	roundRobinStart := 0

	for iter.First(); iter.Valid(); iter.Next() {
		proofIds := bytesToUint64(iter.Value())

		copy(remainingBuckets, initialRemainingBuckets)

		for i := 0; i < len(proofIds)-nTreeTop; i++ {
			proofId := proofIds[nTreeTop+i]
			bucketIndex := &proofIdToBucketIndex[proofId]
			if bucketIndex.BucketId == 0 {
				mod := nBuckets - i

				winningJ := roundRobinStart % mod
				winningBucketId := remainingBuckets[winningJ]
				minRowId := buckets[winningBucketId].nextId

				for j := 1; j < mod; j++ {
					k := (roundRobinStart + j) % mod
					bucketId := remainingBuckets[k]
					rowId := buckets[bucketId].nextId
					if rowId < minRowId {
						winningJ = k
						winningBucketId = bucketId
						minRowId = rowId
					}
				}

				rowId := buckets[winningBucketId].Append(uint32(proofId))

				bucketIndex.BucketId = winningBucketId + 1
				bucketIndex.RowId = rowId

				remainingBuckets[winningJ] = remainingBuckets[nBuckets-i-1]
			} else {
				for j := 0; j < nBuckets-i; j++ {
					if remainingBuckets[j] == bucketIndex.BucketId-1 {
						remainingBuckets[j] = remainingBuckets[nBuckets-i-1]
						break
					}
				}
			}

			segmentsProcessed += 1
			roundRobinStart = (roundRobinStart + 1) % nBuckets
		}

		proofsProcessed += 1
		if proofsProcessed != 0 && proofsProcessed%10_000_000 == 0 {
			log.Printf("ProofsProcessed=%v SegmentsProcessed=%v\n", proofsProcessed, segmentsProcessed)

			mmin := buckets[0].nextId
			mmax := buckets[0].nextId
			for i, b := range buckets {
				log.Printf("BucketId=%v size=%v\n", i, b.nextId)
				if b.nextId < mmin {
					mmin = b.nextId
				}
				if b.nextId > mmax {
					mmax = b.nextId
				}
			}
			log.Printf("Min=%v Max=%v Spread=%v\n", mmin, mmax, mmax-mmin)
		}
	}

	mmin := buckets[0].nextId
	mmax := buckets[0].nextId
	for i, b := range buckets {
		log.Printf("BucketId=%v size=%v\n", i, b.nextId)
		if b.nextId < mmin {
			mmin = b.nextId
		}
		if b.nextId > mmax {
			mmax = b.nextId
		}
	}
	log.Printf("Min=%v Max=%v Spread=%v\n", mmin, mmax, mmax-mmin)
}
