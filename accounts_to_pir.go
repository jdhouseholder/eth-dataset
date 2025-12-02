package ethdataset

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
)

type AccountsToPIRConfig struct {
	WorkDir string `toml:"work_dir"`
	OutDir  string `toml:"out_dir"`
}

func AccountsToPIR(cfg AccountsToPIRConfig) {
	os.MkdirAll(cfg.OutDir, os.ModePerm)

	accountTable := NewAccountTable(cfg.WorkDir)
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	nRecords := 0
	maxLen := 0
	start := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		// addressHashBytes := iter.Key()
		buf := iter.Value()

		if len(buf) > maxLen {
			maxLen = len(buf)
		}

		nRecords += 1
		if nRecords > 0 && nRecords%100_000 == 0 {
			elapsed := time.Since(start)
			log.Printf("%v %v %v\n", nRecords, elapsed, maxLen)
		}
	}

	metadataFile, err := os.OpenFile(filepath.Join(cfg.OutDir, "accounts-pir-metadata.json"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer metadataFile.Close()
	enc := json.NewEncoder(metadataFile)

	if err := enc.Encode(struct {
		NRecords  int `json:"n_records"`
		RecordLen int `json:"record_len"`
	}{
		NRecords:  nRecords,
		RecordLen: maxLen,
	}); err != nil {
		log.Fatal(err)
	}

	dataFile, err := os.OpenFile(filepath.Join(cfg.OutDir, "accounts-pir.bin"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer dataFile.Close()

	w := bufio.NewWriter(dataFile)

	buf := make([]byte, 32+maxLen)
	for iter.First(); iter.Valid(); iter.Next() {
		addressHashBytes := iter.Key()
		copy(buf, addressHashBytes)
		slimAccount := iter.Value()
		copy(buf[32:], slimAccount)

		if _, err := w.Write(buf); err != nil {
			log.Fatal(err)
		}
	}
}
