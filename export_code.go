package ethdataset

import (
	"bytes"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

type ExportCodeConfig struct {
	WorkDir        string      `toml:"work_dir"`
	AccountWorkDir string      `toml:"account_work_dir"`
	ChainConfig    ChainConfig `toml:"chain_config"`
}

func ExportCode(cfg ExportCodeConfig) {
	os.MkdirAll(cfg.WorkDir, os.ModePerm)

	log.Println("Opening stack")
	stack := newNode(cfg.ChainConfig.DataDir)
	log.Println("Opening chainDB")
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)

	log.Println("Opening tables")
	codeTable := NewCodeTable(cfg.WorkDir)
	defer codeTable.Close()

	accountTable := NewAccountTable(cfg.AccountWorkDir)
	defer accountTable.Close()

	iter, err := accountTable.DB.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	codeHashes := make(map[common.Hash]bool)

	log.Println("Starting iteration")

	i := 0
	start := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		var slim SlimAccount
		buf := iter.Value()
		if err := rlp.DecodeBytes(buf, &slim); err != nil {
			log.Fatal(err)
		}
		codeHashHash := common.BytesToHash(slim.CodeHash)
		if _, ok := codeHashes[codeHashHash]; !ok {
			if !bytes.Equal(slim.CodeHash, types.EmptyCodeHash.Bytes()) {
				code := rawdb.ReadCode(chainDB, common.BytesToHash(slim.CodeHash))
				codeTable.Save(slim.CodeHash, code)
			}
			codeHashes[codeHashHash] = true
		}

		i += 1
		if i > 0 && i%1_000_000 == 0 {
			elapsed := time.Since(start)
			log.Printf("%v %v %v\n", i, elapsed, len(codeHashes))
		}
	}
}
