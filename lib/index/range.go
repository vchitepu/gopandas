package index

import "fmt"

// RangeIndex is the default index: integer labels 0..n-1.
type RangeIndex struct {
	n    int
	name string
}

// NewRangeIndex creates a RangeIndex with n labels (0 through n-1).
// Panics if n < 0.
func NewRangeIndex(n int, name string) *RangeIndex {
	if n < 0 {
		panic(fmt.Sprintf("index: NewRangeIndex called with negative length %d", n))
	}
	return &RangeIndex{n: n, name: name}
}

// Len returns the number of labels in the index.
func (r *RangeIndex) Len() int { return r.n }

// Labels returns index labels as integers from 0 to Len()-1.
func (r *RangeIndex) Labels() []any {
	out := make([]any, r.n)
	for i := range out {
		out[i] = i
	}
	return out
}

// Loc returns the position of the given label.
func (r *RangeIndex) Loc(label any) (int, bool) {
	v, ok := label.(int)
	if !ok {
		return -1, false
	}
	if v < 0 || v >= r.n {
		return -1, false
	}
	return v, true
}

// Slice returns a new RangeIndex of length (end-start) with labels starting at 0.
// NOTE: unlike pandas, labels are NOT offset-preserved; the slice always starts at 0.
// Bounds are clamped: negative start becomes 0, end beyond Len() becomes Len().
func (r *RangeIndex) Slice(start, end int) Index {
	if start < 0 {
		start = 0
	}
	if end > r.n {
		end = r.n
	}
	if start >= end {
		return NewRangeIndex(0, r.name)
	}
	return NewRangeIndex(end-start, r.name)
}

// Name returns the index name.
func (r *RangeIndex) Name() string { return r.name }
