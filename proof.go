package ethdataset

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble"
)

const TOP_THRESHOLD int = 4

type ProofDB struct {
	// nextId uint64
	nextId atomic.Uint64

	path             string
	proofSegmentToId *pebble.DB
	idToProofSegment *pebble.DB

	//topCache map[string]uint64
	disableTopCache bool
	topCache        sync.Map
	keyLocker       *KeyLocker

	total   atomic.Uint64
	unique  atomic.Uint64
	deduped atomic.Uint64
}

// TODO: rename
func NewProofDeduper(path string) ProofDB {
	return ProofDB{
		path:             path,
		proofSegmentToId: openPebbleDB(path, "proofSegmentToId"),
		idToProofSegment: openPebbleDB(path, "idToProofSegment"),
		//topCache:         make(map[string]uint64),
		topCache:  sync.Map{},
		keyLocker: NewKeyLocker(256),
	}
}

func NewScopedProofDB(path, scope string) ProofDB {
	return ProofDB{
		path:             path,
		proofSegmentToId: openPebbleDB(path, fmt.Sprintf("%s-proofSegmentToId", scope)),
		idToProofSegment: openPebbleDB(path, fmt.Sprintf("%s-idToProofSegment", scope)),
		disableTopCache:  true,
		topCache:         sync.Map{},
		keyLocker:        NewKeyLocker(256),
	}
}

func (pd *ProofDB) NewProofContainer() ProofContainer {
	return ProofContainer{
		pd: pd,
	}
}

func (pd *ProofDB) Total() uint64 {
	return pd.total.Load()
}

func (pd *ProofDB) Unique() uint64 {
	return pd.unique.Load()
}

func (pd *ProofDB) Deduped() uint64 {
	return pd.deduped.Load()
}

func (pd *ProofDB) GetId(b []byte) (uint64, bool) {
	value, closer, err := pd.proofSegmentToId.Get(b)
	if err == pebble.ErrNotFound {
		return 0, false
	}
	if err != nil {
		log.Fatal(err)
	}
	id := binary.LittleEndian.Uint64(value)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	return id, true

}

func (pd *ProofDB) getOrCreateId(i int, ps []byte) uint64 {
	pd.total.Add(1)

	var isInTop bool
	if !pd.disableTopCache {
		isInTop = i <= TOP_THRESHOLD
		if isInTop {
			if len(ps) == 0 {
				panic("Expected non empty proof segment")
			}
			key := unsafe.String(&ps[0], len(ps))
			// if id, ok := pd.topCache[key]; ok {
			if id, ok := pd.topCache.Load(key); ok {
				pd.deduped.Add(1)
				return id.(uint64)
			}
		}
	}

	pd.keyLocker.Lock(ps)
	defer pd.keyLocker.Unlock(ps)

	if id, ok := pd.GetId(ps); ok {
		pd.deduped.Add(1)
		return id
	}

	pd.unique.Add(1)

	id := pd.nextId.Add(1)

	idBytes := uint64ToKey(id)
	if err := pd.proofSegmentToId.Set(ps, idBytes, pebble.NoSync); err != nil {
		log.Fatal(err)
	}
	if err := pd.idToProofSegment.Set(idBytes, ps, pebble.NoSync); err != nil {
		log.Fatal(err)
	}

	if !pd.disableTopCache && isInTop {
		//pd.topCache[string(ps)] = id
		pd.topCache.Store(string(ps), id)
	}

	return id
}

func (pd *ProofDB) Close() {
	pd.proofSegmentToId.Close()
	pd.idToProofSegment.Close()
}

func (pd *ProofDB) RecoverProof(ids []uint64) [][]byte {
	var proof [][]byte

	for _, id := range ids {
		b, c, err := pd.idToProofSegment.Get(uint64ToKey(id))
		if err != nil {
			log.Fatal(err)
		}

		dst := make([]byte, len(b))
		copy(dst, b)

		if err := c.Close(); err != nil {
			log.Fatal(err)
		}

		proof = append(proof, dst)
	}

	return proof
}

type ProofContainer struct {
	pd  *ProofDB
	ids []uint64
}

func (pc *ProofContainer) AsIds() []uint64 {
	return pc.ids
}

func (pc *ProofContainer) DedupAll(ps [][]byte) {
	for i, b := range ps {
		id := pc.pd.getOrCreateId(i, b)
		pc.ids = append(pc.ids, id)
	}
}
