package dict

type SimpleDict struct {
	m map[string]interface{}
}

func NewSimple() *SimpleDict {
	return &SimpleDict{m: make(map[string]interface{})}
}

func (s *SimpleDict) Get(key string) (interface{}, bool) {
	val, ok := s.m[key]
	return val, ok
}

// Len returns the number of dict
func (s *SimpleDict) Len() int {
	if s.m == nil {
		panic("m is nil")
	}
	return len(s.m)
}

// Put puts key value into dict and returns the number of new inserted key-value
func (s *SimpleDict) Put(key string, val interface{}) (result int) {
	_, existed := s.m[key]
	s.m[key] = val
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (s *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := s.m[key]
	if existed {
		return 0
	}
	s.m[key] = val
	return 1
}

// PutIfExists puts value if the key is existed and returns the number of inserted key-value
func (s *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := s.m[key]
	if existed {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (s *SimpleDict) Remove(key string) (result int) {
	_, existed := s.m[key]
	delete(s.m, key)
	if existed {
		return 1
	}
	return 0
}

// Keys returns all keys in dict
func (s *SimpleDict) Keys() []string {
	result := make([]string, len(s.m))
	i := 0
	for k := range s.m {
		result[i] = k
		i++
	}
	return result
}

// ForEach traversal the dict
func (s *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range s.m {
		if !consumer(k, v) {
			break
		}
	}
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (s *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range s.m {
			result[i] = k
			break
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (s *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(s.m) {
		size = len(s.m)
	}
	result := make([]string, size)
	i := 0
	for k := range s.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

// Clear removes all keys in dict
func (s *SimpleDict) Clear() {
	*s = *NewSimple()
}
