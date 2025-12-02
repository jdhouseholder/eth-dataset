package ethdataset

import (
	"hash/fnv"
	"sync"
)

type KeyLocker struct {
	locks []sync.Mutex
}

func NewKeyLocker(shards uint32) *KeyLocker {
	if shards == 0 {
		shards = 256
	}
	return &KeyLocker{
		locks: make([]sync.Mutex, shards),
	}
}

func (kl *KeyLocker) getShardIndex(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32() % uint32(len(kl.locks))
}

func (kl *KeyLocker) Lock(key []byte) {
	index := kl.getShardIndex(key)
	kl.locks[index].Lock()
}

func (kl *KeyLocker) Unlock(key []byte) {
	index := kl.getShardIndex(key)
	kl.locks[index].Unlock()
}
