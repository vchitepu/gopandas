package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/arrowutil"
	"github.com/vinaychitepu/gopandas/index"
)

// Head returns a new Series with the first n elements.
// If n >= Len(), returns a copy of the entire Series.
// If n <= 0, returns an empty Series.
func (s Series[T]) Head(n int) Series[T] {
	if n <= 0 {
		return s.islice(0, 0)
	}
	if n > s.Len() {
		n = s.Len()
	}
	return s.islice(0, n)
}

// islice is an internal helper that returns a new Series from position start to end (exclusive).
func (s Series[T]) islice(start, end int) Series[T] {
	newArr := arrowutil.SliceArray(s.arr, start, end)
	newIdx := s.index.Slice(start, end)
	return Series[T]{
		name:  s.name,
		index: newIdx,
		arr:   newArr,
	}
}

// Tail returns a new Series with the last n elements.
// If n >= Len(), returns a copy of the entire Series.
// If n <= 0, returns an empty Series.
func (s Series[T]) Tail(n int) Series[T] {
	if n <= 0 {
		return s.islice(s.Len(), s.Len())
	}
	if n > s.Len() {
		n = s.Len()
	}
	start := s.Len() - n
	return s.islice(start, s.Len())
}

// ILoc returns a new Series containing elements from start (inclusive) to end (exclusive).
// Out-of-range values are clamped to valid bounds.
func (s Series[T]) ILoc(start, end int) Series[T] {
	if start < 0 {
		start = 0
	}
	if end > s.Len() {
		end = s.Len()
	}
	if start >= end {
		return s.islice(0, 0)
	}
	return s.islice(start, end)
}

// Filter returns a new Series containing only the elements where mask[i] is true.
// Panics if len(mask) != Len().
func (s Series[T]) Filter(mask []bool) Series[T] {
	if len(mask) != s.Len() {
		panic(fmt.Sprintf("series.Filter: mask length %d != series length %d", len(mask), s.Len()))
	}

	// Collect indices where mask is true
	var indices []int
	for i, m := range mask {
		if m {
			indices = append(indices, i)
		}
	}

	// Build new values and index labels
	vals := make([]T, len(indices))
	for j, idx := range indices {
		vals[j] = getTypedValue[T](s.arr, idx)
	}

	// Build new index — use RangeIndex for simplicity
	newIdx := index.NewRangeIndex(len(indices), s.index.Name())
	return New[T](memory.DefaultAllocator, vals, newIdx, s.name)
}
