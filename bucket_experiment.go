package ethdataset

import (
	"log"
	"os"
	"math/rand"
	"time"
)

type BucketExperimentCfg struct {
	WorkDir string `toml:"work_dir"`
	OutDir  string `toml:"out_dir"`
}

func BucketExperiment(cfg BucketExperimentCfg) {
	rand.Seed(time.Now().UnixNano())

	os.MkdirAll(cfg.OutDir, os.ModePerm)

	accountTable := NewTable(cfg.WorkDir, "accounts")
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}

	proofBucketMapper := NewBucketMapper(
		cfg.WorkDir,
		cfg.OutDir,
		3,
		64,
		proofSegmentMetadata,
	)
	defer proofBucketMapper.Close()

	log.Println("Gathering accounts")
	var accounts [][]byte
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if rand.Float64() > 0.99 {
			b := make([]byte, 32)
			copy(b, iter.Key())
			accounts = append(accounts, b)
		}
		i += 1
		if i % 1_000_000 == 0 {
			log.Printf("%v\n", i)
		}
	}
	log.Printf("Gathered %v accounts\n", len(accounts))

	rand.Shuffle(len(accounts), func(i, j int) {
        accounts[i], accounts[j] = accounts[j], accounts[i]
    })

	i = 0
	for _, addressHashBytes := range accounts {
		proofBucketMapper.MapAccountProofToBucketIndexes(addressHashBytes)
		i += 1
		if i % 100_000 == 0 {
			log.Printf("%+v\n", proofBucketMapper.Stats())
		}
	}
	iter.Close()
	log.Printf("%+v\n", proofBucketMapper.Stats())
}
