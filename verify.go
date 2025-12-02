package ethdataset

import (
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

type VerifyConfig struct {
	StateRoot string `toml:"state_root"`
	WorkDir   string `toml:"work_dir"`
}

func Verify(cfg VerifyConfig) {
	stateRoot := common.HexToHash(cfg.StateRoot)
	log.Printf("StateRoot=%v\n", stateRoot)

	log.Println("Opening DBs")

	accountToProof := openPebbleDB(cfg.WorkDir, "accountToProof")
	iter, err := accountToProof.NewIter(nil)
	if err != nil {
		log.Fatal(err)
	}

	proofDeduper := NewProofDeduper(cfg.WorkDir)

	log.Println("Starting iteration")

	nVerified := 0
	for iter.First(); iter.Valid(); iter.Next() {
		addressHashBytes := iter.Key()

		proofIdBytes := iter.Value()
		proofIds := bytesToUint64(proofIdBytes)

		proofBytes := proofDeduper.RecoverProof(proofIds)
		pdb := NewProofKV(proofBytes)

		if _, err := trie.VerifyProof(stateRoot, addressHashBytes, pdb); err != nil {
			log.Fatal(err)
		}

		nVerified += 1

		if nVerified != 0 && nVerified%10_000 == 0 {
			log.Printf("Verified nVerified=%v\n", nVerified)
		}
	}

	log.Printf("Verification complete nVerified=%v\n", nVerified)

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
