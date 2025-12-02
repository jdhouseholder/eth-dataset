package ethdataset

import (
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

type ExportAccountsConfig struct {
	WorkDir     string      `toml:"work_dir"`
	NAccounts   uint64      `toml:"n_accounts"`
	ChainConfig ChainConfig `toml:"chain_config"`
}

func ExportAccounts(cfg ExportAccountsConfig) {
	os.MkdirAll(cfg.WorkDir, os.ModePerm)

	stack := newNode(cfg.ChainConfig.DataDir)
	chainDB := newChainDB(cfg.ChainConfig.AncientsDir, stack)
	trieDB := newTrieDB(stack, chainDB)

	header := rawdb.ReadHeadHeader(chainDB)
	stateRoot := header.Root

	log.Println("Starting iteration")

	accountTable := NewAccountTable(cfg.WorkDir)
	defer accountTable.Close()

	accountIt := newTrieIter(trie.StateTrieID(stateRoot), nil, trieDB)

	start := time.Now()
	epochStart := time.Now()
	nAccounts := uint64(0)

	for accountIt.Next() {
		addressHashBytes := accountIt.Key
		accountBytes := accountIt.Value

		accountTable.Save(addressHashBytes, accountBytes)

		if nAccounts > 0 && nAccounts%1_000_000 == 0 {
			elapsed := time.Since(start)
			epochElapsed := time.Since(epochStart)
			epochStart = time.Now()
			accountsPerSec := float64(nAccounts) / elapsed.Seconds()
			log.Printf("Accounts=%v Elapsed=%v EpochElapsed=%v AccountsPerSec=%v\n", nAccounts, elapsed, epochElapsed, accountsPerSec)
		}
		nAccounts += 1
		if cfg.NAccounts != 0 && nAccounts == cfg.NAccounts {
			break
		}
	}
}
