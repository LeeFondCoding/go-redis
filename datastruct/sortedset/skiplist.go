package sortedset

import (
	"math/bits"
	"math/rand/v2"
)

const (
	maxLevel = 16
)

type Element struct {
	Member string
	Score  float64
}

type Level struct {
	forward *node
	span    int64
}

type node struct {
	Element
	backward *node
	level    []*Level
}

type skipList struct {
	header, tail *node
	length       int64
	level        int16
}

func newNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range level {
		n.level[i] = new(Level)
	}
	return n
}

func newSkipList() *skipList {
	return &skipList{
		level:  1,
		header: newNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	total := uint64(1) << uint64(maxLevel)
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

func (s *skipList) insert(member string, score float64) *node {
	update, rank := make([]*node, maxLevel), make([]int64, maxLevel)

	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score) ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()
	if level > s.level {
		for i := range s.level {
			rank[i] = 0
			update[i] = s.header
			update[i].level[i].span = s.length
		}
		s.level = level
	}

	node = newNode(level, score, member)
	for i := range s.level {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}

	for i := level; i < s.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == s.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		s.tail = node
	}
	s.length++
	return node
}

func (s *skipList) removeNode(node *node, update []*node) {
	for i := range s.level {
		if update[i].level[i].forward == node {
			// 前驱节点上有目标节点，则更新跨度
			update[i].level[i].span += node.level[i].span - 1
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		s.tail = node.backward
	}
	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}
	s.length--
}

func (s *skipList) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && node.Score == score && node.Member == member {
		s.removeNode(node, update)
		return true
	}
	return false
}

func (s *skipList) getRank(member string, score float64) int64 {
	var rank int64
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		if node.Member == member {
			return rank
		}
	}
	return 0
}

func (s *skipList) getByRank(rank int64) *node {
	var i int64
	node := s.header
	for level := s.level - 1; i >= 0; i-- {
		for node.level[level].forward != nil &&
			(node.level[level].span+i <= rank) {
			i += node.level[level].span
			node = node.level[level].forward
		}
		if i == rank {
			return node
		}
	}
	return nil
}

func (s *skipList) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	n := s.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	n = s.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

func (s *skipList) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !s.hasInRange(min, max) {
		return nil
	}
	n := s.header
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (s *skipList) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !s.hasInRange(min, max) {
		return nil
	}
	n := s.header
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	if !min.less(n.Score) {
		return nil
	}
	return n
}

func (s *skipList) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(node.level[i].forward.Score) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward
	for node != nil {
		if !max.greater(node.Score) {
			break
		}
		next := node.level[0].forward
		removedElem := &node.Element
		removed = append(removed, removedElem)
		s.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// RemoveRangeByRank [start, stop)
func (s *skipList) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := s.header
	for level := s.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}
	i++
	node = node.level[0].forward
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElem := node.Element
		removed = append(removed, &removedElem)
		s.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
