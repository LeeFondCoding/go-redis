package sortedset

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skipList
}

func New() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: newSkipList(),
	}
}

func (s *SortedSet) Add(member string, score float64) bool {
	elem, ok := s.dict[member]
	s.dict[member] = &Element{member, score}
	if ok {
		if score != elem.Score {
			s.skiplist.remove(member, elem.Score)
			s.skiplist.insert(member, score)
		}
		return false
	}
	s.skiplist.insert(member, score)
	return true
}

func (s *SortedSet) Len() int64 {
	return int64(len(s.dict))
}

func (s *SortedSet) Get(member string) (elem *Element, ok bool) {
	elem, ok = s.dict[member]
	return
}

func (s *SortedSet) Remove(member string) bool {
	v, ok := s.dict[member]
	if ok {
		s.skiplist.remove(member, v.Score)
		delete(s.dict, member)
		return true
	}
	return false
}

func (s *SortedSet) GetRank(member string, desc bool) int64 {
	elem, ok := s.dict[member]
	if !ok {
		return -1
	}
	r := s.skiplist.getRank(member, elem.Score)
	if desc {
		r = s.skiplist.length - r
	} else {
		r--
	}
	return r
}

func (s *SortedSet) ForEach(start, stop int64, desc bool, consumer func(element *Element) bool)) {

}

func (s *SortedSet) Range(start, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	s.ForEach(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

func (s *SortedSet) Count(min, max *ScoreBorder) int64 {
	var i int64
	s.ForEach(0, s.Len(), false, func(element *Element) bool {
		gtMin := min.less(element.Score)
		if !gtMin {
			return true
		}
		ltMax := max.greater(element.Score)
		if !ltMax {
			return false
		}
		i++
		return true
	})
	return i
}

func (s *SortedSet) ForEachByScore(min, max *ScoreBorder, offset, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *node
	if desc {
		node = s.skiplist.getLastInScoreRange(min, max)
	} else {
		node = s.skiplist.getFirstInScoreRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}


}

func (s *SortedSet) RemoveByScore(min, max *ScoreBorder) int64 {
	removed := s.skiplist.RemoveRangeByScore(min, max, 0)
	for _, elem := range removed {
		delete(s.dict, elem.Member)
	}
	return int64(len(removed))
}

func (s *SortedSet) PopMin(count int) []*Element {
	first := s.skiplist.getFirstInScoreRange(negativeInfBorder, positiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := s.skiplist.RemoveRangeByScore(border, positiveInfBorder, count)
	for _, elem := range removed {
		delete(s.dict, elem.Member)
	}
	return removed
}

func (s *SortedSet) RemoveByRank(start, stop int64) int64 {
	removed := s.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, elem := range removed {
		delete(s.dict, elem.Member)
	}
	return int64(len(removed))
}
