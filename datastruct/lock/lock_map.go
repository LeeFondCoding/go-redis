package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

type Locks struct {
	table []*sync.RWMutex
}

func New(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (l *Locks) spread(hashCode uint32) uint32 {
	if l == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(l.table))
	return (tableSize - 1) & hashCode
}

func (l *Locks) Lock(key string) {
	index := l.spread(fnv32(key))
	l.table[index].Lock()
}

func (l *Locks) RLock(key string) {
	index := l.spread(fnv32(key))
	l.table[index].RLock()
}

func (l *Locks) UnLock(key string) {
	index := l.spread(fnv32(key))
	l.table[index].Unlock()
}

func (l *Locks) RunLock(key string) {
	index := l.spread(fnv32(key))
	l.table[index].RUnlock()
}

func (l *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, k := range keys {
		index := l.spread(fnv32(k))
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for k := range indexMap {
		indices = append(indices, k)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

func (l *Locks) Locks(keys ...string) {
	indices := l.toLockIndices(keys, false)
	for _, index := range indices {
		l.table[index].Lock()
	}
}

func (l *Locks) RLocks(keys ...string) {
	indices := l.toLockIndices(keys, false)
	for _, index := range indices {
		l.table[index].RLock()
	}
}

func (l *Locks) UnLocks(keys ...string) {
	indices := l.toLockIndices(keys, false)
	for _, index := range indices {
		l.table[index].Unlock()
	}
}

func (l *Locks) RunLocks(keys ...string) {
	indices := l.toLockIndices(keys, true)
	for _, index := range indices {
		l.table[index].RUnlock()
	}
}

func (l *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, w := range writeKeys {
		idx := l.spread(fnv32(w))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := l.table[index]
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (l *Locks) RWUnLocks(writeKeys, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := l.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, w := range writeKeys {
		idx := l.spread(fnv32(w))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := l.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}

}
