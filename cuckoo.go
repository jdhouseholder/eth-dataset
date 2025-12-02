package ethdataset

import (
	"runtime"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"log"
	mrand "math/rand"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
)

func newSeeds(numHashes int) []uint64 {
	seeds := make([]uint64, numHashes)
	for i := 0; i < numHashes; i++ {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			log.Fatalf("failed to generate random seed: %w", err)
		}
		seeds[i] = binary.LittleEndian.Uint64(b[:])
	}
	return seeds
}

type HTConfig struct {
	HashSeeds []uint64 `json:"hash_seeds"`
	Capacity  int      `json:"capacity"`
}

type HashTable struct {
	cfg      HTConfig
	maxKicks int

	table  [][]byte
	digest *xxhash.Digest
}

func NewHashTable(k, capacity, maxKicks int) *HashTable {
	return &HashTable{
		cfg: HTConfig{
			HashSeeds: newSeeds(k),
			Capacity:  capacity,
		},
		maxKicks: maxKicks,
		table:    make([][]byte, capacity),
		digest:   xxhash.New(),
	}
}

func (ht *HashTable) Config() HTConfig {
	return ht.cfg
}

func (ht *HashTable) hash(item []byte, seed uint64) int {
	ht.digest.ResetWithSeed(seed)
	_, err := ht.digest.Write(item)
	if err != nil {
		log.Fatal(err)
	}

	return int(ht.digest.Sum64() % uint64(ht.cfg.Capacity))
}

func (ht *HashTable) Get(key []byte) (int, bool) {
	for _, hashSeed := range ht.cfg.HashSeeds {
		i := ht.hash(key, hashSeed)
		v := ht.table[i]
		if v != nil && bytes.Equal(key, v) {
			return i, true
		}
	}
	return 0, false
}

func (ht *HashTable) Insert(key []byte) {
	myKey := make([]byte, len(key))
	copy(myKey, key)
	for {
		if ht.insert(myKey) {
			return
		}
		log.Fatal("Rehash triggered")
	}
}

func (ht *HashTable) nextPos(key []byte, avoid int) int {
	var potentialPos []int
	for _, hashSeed := range ht.cfg.HashSeeds {
		pos := ht.hash(key, hashSeed)
		if pos != avoid {
			potentialPos = append(potentialPos, pos)
		}
	}
	winner := mrand.Intn(len(potentialPos))
	return potentialPos[winner]
}

func (ht *HashTable) insert(key []byte) bool {
	for _, hashSeed := range ht.cfg.HashSeeds {
		pos := ht.hash(key, hashSeed)
		got := ht.table[pos]
		if got != nil && bytes.Equal(key, got) {
			return true
		}
		if got == nil {
			ht.table[pos] = key
			return true
		}
	}

	currentItem := key
	avoid := -1
	for i := 0; i < ht.maxKicks; i++ {
		pos := ht.nextPos(currentItem, avoid)
		kickedItem := ht.table[pos]
		ht.table[pos] = currentItem

		if kickedItem == nil {
			return true
		}

		currentItem = kickedItem
		avoid = pos
	}
	return false
}

func (ht *HashTable) rehash() {
	// Don't increase capacity, just try new seeds. This is intentinoal.
	// This isn't really used rn for simplicity. In general we want to increase
	// capacity, but we can't afford to double it so we'd need a different schedule.
	log.Print("Rehashing")
	ht.cfg.HashSeeds = newSeeds(len(ht.cfg.HashSeeds))
	oldTable := ht.table
	ht.table = make([][]byte, len(oldTable))
	for _, v := range oldTable {
		if v != nil {
			ok := ht.insert(v)
			if !ok {
				log.Fatal("Nested rehash!")
			}
		}
	}
}

func (ht *HashTable) GetTable() [][]byte {
	return ht.table
}

type ExperimentDatasetCfg struct {
	WorkDir string `toml:"work_dir"`
	OutDir  string `toml:"out_dir"`

	K        int `toml:"k"`
	Capacity int `toml:"capacity"`
	MaxKicks int `toml:"max_kicks"`

	NAccounts int `toml:"n_accounts"`
}

func ExperimentDataset(cfg ExperimentDatasetCfg) {
	os.MkdirAll(cfg.OutDir, os.ModePerm)

	accountTable := NewTable(cfg.WorkDir, "accounts")
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Gathering accounts!!")

	prob := float64(cfg.NAccounts) / 320_000_000.0
	accounts := make([][32]byte, cfg.NAccounts)
	nAccounts := 0
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if mrand.Float64() < prob {
			copy(accounts[nAccounts][:], iter.Key())
			nAccounts += 1
			if nAccounts >= cfg.NAccounts {
				log.Printf("Finished early at iteration %v\n", i)
				break
			}
		}
		if i % 1_000_000 == 0 {
			log.Printf("%v\n", i)
		}
		i += 1
	}
	iter.Close()

	mrand.Shuffle(len(accounts), func(i, j int) {
        accounts[i], accounts[j] = accounts[j], accounts[i]
    })

	ht := NewHashTable(cfg.K, cfg.Capacity, cfg.MaxKicks)

	log.Printf("Gathered %v accounts\n", len(accounts))

	log.Println("Building hash table")
	for _, addressHashBytes := range accounts {
		ht.Insert(addressHashBytes[:])

		nAccounts += 1
		if nAccounts%10_000_000 == 0 {
			log.Printf("%v: Inserting %x", nAccounts, addressHashBytes)
		}
	}
	log.Println("Hash table complete")

	accounts = nil
	runtime.GC()

	hashTableMetadataFile, err := os.OpenFile(filepath.Join(cfg.OutDir, "hash-table.metadata.json"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer hashTableMetadataFile.Close()
	if err := json.NewEncoder(hashTableMetadataFile).Encode(ht.Config()); err != nil {
		log.Fatal(err)
	}

	debugFile, err := os.OpenFile(filepath.Join(cfg.OutDir, "debug.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer debugFile.Close()
	enc := json.NewEncoder(debugFile)

	type DeubgInput struct {
		RowId            int    `json:"row_id"`
		AddressHashBytes []byte `json:"address_hash_bytes"`
		Value            []byte `json:"value"`
	}

	proofBucketMapper := NewBucketMapper(
		cfg.WorkDir,
		cfg.OutDir,
		nTreeTop,
		nBuckets,
		proofSegmentMetadata,
	)
	defer proofBucketMapper.Close()

	fakeBucketMapper := NewFakeBucketMapper(
		proofBucketMapper.accountToProofIds,
		proofBucketMapper.idToProofSegment,
		cfg.OutDir,
		proofSegmentMetadata,
	)
	defer fakeBucketMapper.Close()

	oneBucketAccountFileTable := OpenFileTable(cfg.OutDir, "f-accounts", sizeOfAccountPirRecord, 0)
	defer oneBucketAccountFileTable.Close()

	accountFileTable := OpenFileTable(cfg.OutDir, "accounts", sizeOfAccountPirRecord, 0)
	defer accountFileTable.Close()

	for _, addressHashBytes := range ht.GetTable() {
		if addressHashBytes != nil {
			slimAccount := accountTable.Get(addressHashBytes)

			buf := make([]byte, sizeOfAccountPirRecord)
			copy(buf, addressHashBytes)
			copy(buf[sizeOfAddressHash:], slimAccount)
			bucketIndexes := proofBucketMapper.MapAccountProofToBucketIndexes(addressHashBytes)
			copy(buf[sizeOfAddressHash+sizeOfAccount:], bucketIndexesToBytes(bucketIndexes, nBuckets))
			rowId := accountFileTable.Append(buf)

			if err := enc.Encode(DeubgInput{
				RowId:            int(rowId),
				AddressHashBytes: addressHashBytes,
				Value:            buf,
			}); err != nil {
				log.Fatal(err)
			}

			buf = make([]byte, sizeOfAccountPirRecord)
			copy(buf, addressHashBytes)
			copy(buf[sizeOfAddressHash:], slimAccount)
			fakeBucketIndexes := fakeBucketMapper.MapAccountProofToBucketIndexes(addressHashBytes)
			copy(buf[sizeOfAddressHash+sizeOfAccount:], bucketIndexesToBytes(fakeBucketIndexes, nBuckets))
			oneBucketAccountFileTable.Append(buf)
		} else {
			accountFileTable.WriteBlank()
			oneBucketAccountFileTable.WriteBlank()
		}
	}
}
