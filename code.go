package ethdataset

import (
	"log"

	"github.com/cockroachdb/pebble"
)

type CodeDeduper struct{}

func NewCodeDeduper(path string) *CodeDeduper {
	return nil
}

func (c *CodeDeduper) Dedup(key, code []byte) uint64 { return 0 }

// TODO: Store Code.
type CodeTable struct {
	DB *pebble.DB
}

func NewCodeTable(path string) *CodeTable {
	return &CodeTable{
		DB: openPebbleDB(path, "code"),
	}
}

func (c *CodeTable) Save(key, code []byte) {
	if err := c.DB.Set(key, code, pebble.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (c *CodeTable) Close() {
	if err := c.DB.Close(); err != nil {
		log.Fatal(err)
	}
}
