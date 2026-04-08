package index

import "fmt"

// RangeIndex is the default index: integer labels 0..n-1.
type RangeIndex struct {
	len  int
	name string
}

func NewRangeIndex(n int, name string) *RangeIndex {
	if n < 0 {
		panic(fmt.Sprintf("index: NewRangeIndex called with negative length %d", n))
	}
	return &RangeIndex{len: n, name: name}
}

func (r *RangeIndex) Len() int { return r.len }

func (r *RangeIndex) Labels() []any {
	out := make([]any, r.len)
	for i := range out {
		out[i] = i
	}
	return out
}

func (r *RangeIndex) Loc(label any) (int, bool) {
	v, ok := label.(int)
	if !ok {
		return -1, false
	}
	if v < 0 || v >= r.len {
		return -1, false
	}
	return v, true
}

func (r *RangeIndex) Slice(start, end int) Index {
	if start < 0 {
		start = 0
	}
	if end > r.len {
		end = r.len
	}
	if start >= end {
		return NewRangeIndex(0, r.name)
	}
	return NewRangeIndex(end-start, r.name)
}

func (r *RangeIndex) Name() string { return r.name }
