package ethdataset

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

func FastUnsafeOptions() *pebble.Options {
	// Large unified cache (data/index/filter blocks). Adjust for your RAM budget.
	cache := pebble.NewCache(8 << 30) // 8 GiB

	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                256 << 20,
		MemTableStopWritesThreshold: 64,
		L0CompactionThreshold:       40,
		L0StopWritesThreshold:       200,
		Levels: []pebble.LevelOptions{
			{
				FilterPolicy: bloom.FilterPolicy(10),
			},
		},
		BytesPerSync:    0,
		WALBytesPerSync: 0,
		MaxOpenFiles:    100_000,
	}

	return opts
}

func KeyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func PrefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: KeyUpperBound(prefix),
	}
}
