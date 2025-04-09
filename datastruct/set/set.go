package set

type Set interface {
	Add(val string) int
	Remove(val string) int
	Has(val string) bool
	Len() int
	ToSlice() []string
	ForEach(consumer func(member string) bool)
	Copy() Set
	RandomMembers(limit int) []string
	RandomDistinctMembers(limit int) []string
}

type set struct {
	set map[string]struct{}
}

// New 创建一个新的集合实例
func New(members ...string) Set {
	s := make(map[string]struct{}, len(members))
	for _, member := range members {
		s[member] = struct{}{}
	}
	return &set{set: s}
}

func (s *set) Add(val string) int {
	_, ok := s.set[val]
	if ok {
		return 0
	}
	s.set[val] = struct{}{}
	return 1
}

func (s *set) Remove(val string) int {
	_, ok := s.set[val]
	if ok {
		delete(s.set, val)
		return 1
	}
	return 0
}

func (s *set) Has(val string) bool {
	if s == nil {
		return false
	}
	_, ok := s.set[val]
	return ok
}

func (s *set) Len() int {
	return len(s.set)
}

func (s *set) ToSlice() []string {
	slice := make([]string, 0, len(s.set))
	for member := range s.set {
		slice = append(slice, member)
	}
	return slice
}

func (s *set) ForEach(consumer func(member string) bool) {
	for member := range s.set {
		if !consumer(member) {
			break
		}
	}
}

// Copy 返回一个新的set，包含当前set的所有元素
func (s *set) Copy() Set {
	newSet := New()
	for member := range s.set {
		newSet.Add(member)
	}
	return newSet
}

func (s *set) RandomMembers(limit int) []string {
	if s == nil {
		return nil
	}
	randomMembers := make([]string, 0, limit)
	for s := range s.set {
		randomMembers = append(randomMembers, s)
		if len(randomMembers) == limit {
			break
		}
	}
	return randomMembers
}

func (s *set) RandomDistinctMembers(limit int) []string {
	return s.RandomMembers(limit)
}

// Intersect 返回多个set的交集
func Intersect(sets ...Set) Set {
	newSet := New()
	if len(sets) == 0 {
		return newSet
	}

	countMap := make(map[string]int)
	for _, s := range sets {
		s.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	totalLen := len(sets)
	for k, v := range countMap {
		if v == totalLen {
			newSet.Add(k)
		}
	}
	return newSet
}

// Union 返回多个set的并集
func Union(sets ...Set) Set {
	newSet := New()
	for _, s := range sets {
		s.ForEach(func(member string) bool {
			newSet.Add(member)
			return true
		})
	}
	return newSet
}

// Diff 返回多个set的差集
func Diff(sets ...Set) Set {
	if len(sets) == 0 {
		return New()
	}
	newSet := sets[0].Copy()
	for _, s := range sets[1:] {
		s.ForEach(func(member string) bool {
			newSet.Remove(member)
			return true
		})
		if newSet.Len() == 0 {
			break
		}
	}
	return newSet
}
