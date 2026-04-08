package index

import "fmt"

// MultiIndex is a hierarchical index composed of multiple levels.
type MultiIndex struct {
	levels [][]any
	nrows  int
	name   string
}

func NewMultiIndex(levels [][]any, name string) *MultiIndex {
	if len(levels) == 0 {
		panic("index: NewMultiIndex called with no levels")
	}
	nrows := len(levels[0])
	for i, lv := range levels {
		if len(lv) != nrows {
			panic(fmt.Sprintf("index: NewMultiIndex level %d has length %d, expected %d", i, len(lv), nrows))
		}
	}
	cp := make([][]any, len(levels))
	for i, lv := range levels {
		cp[i] = make([]any, len(lv))
		copy(cp[i], lv)
	}
	return &MultiIndex{levels: cp, nrows: nrows, name: name}
}

func (m *MultiIndex) Len() int { return m.nrows }

func (m *MultiIndex) Labels() []any {
	out := make([]any, m.nrows)
	for row := 0; row < m.nrows; row++ {
		tuple := make([]any, len(m.levels))
		for lv := range m.levels {
			tuple[lv] = m.levels[lv][row]
		}
		out[row] = tuple
	}
	return out
}

func (m *MultiIndex) Loc(label any) (int, bool) {
	key, ok := label.([]any)
	if !ok {
		return -1, false
	}
	if len(key) != len(m.levels) {
		return -1, false
	}
	for row := 0; row < m.nrows; row++ {
		match := true
		for lv := range m.levels {
			if m.levels[lv][row] != key[lv] {
				match = false
				break
			}
		}
		if match {
			return row, true
		}
	}
	return -1, false
}

func (m *MultiIndex) Slice(start, end int) Index {
	if start < 0 {
		start = 0
	}
	if end > m.nrows {
		end = m.nrows
	}
	if start >= end {
		empty := make([][]any, len(m.levels))
		for i := range empty {
			empty[i] = []any{}
		}
		return NewMultiIndex(empty, m.name)
	}
	sliced := make([][]any, len(m.levels))
	for i, lv := range m.levels {
		sliced[i] = lv[start:end]
	}
	return NewMultiIndex(sliced, m.name)
}

func (m *MultiIndex) Name() string { return m.name }
