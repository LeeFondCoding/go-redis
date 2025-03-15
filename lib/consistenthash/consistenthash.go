package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

type HashFunc func(key []byte) uint32

type Map struct {
	hashFunc HashFunc
	replicas int
	keys     []int // sorted
	hashMap  map[int]string
}

func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func getPartitionKey(key string) string {
	begin := strings.Index(key, "{")
	if begin == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == begin+1 {
		return key
	}
	return key[begin+1 : end]
}

func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if index == len(m.keys) {
		index = 0
	}
	return m.hashMap[m.keys[index]]
}
