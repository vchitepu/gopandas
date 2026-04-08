package series

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/arrowutil"
	"github.com/vchitepu/gopandas/index"
)

// DropNA returns a new Series with all null values removed.
// The index is reset to a new RangeIndex.
func (s Series[T]) DropNA() Series[T] {
	n := s.Len()
	var vals []T
	for i := 0; i < n; i++ {
		if !arrowutil.IsNull(s.arr, i) {
			vals = append(vals, getTypedValue[T](s.arr, i))
		}
	}
	if vals == nil {
		vals = []T{}
	}
	newIdx := index.NewRangeIndex(len(vals), s.index.Name())
	return New[T](memory.DefaultAllocator, vals, newIdx, s.name)
}

// FillNA returns a new Series with null values replaced by val.
func (s Series[T]) FillNA(val T) Series[T] {
	n := s.Len()
	vals := make([]T, n)
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			vals[i] = val
		} else {
			vals[i] = getTypedValue[T](s.arr, i)
		}
	}
	return New[T](memory.DefaultAllocator, vals, s.index, s.name)
}

// Count returns the number of non-null elements.
func (s Series[T]) Count() int {
	return s.Len() - arrowutil.NullCount(s.arr)
}

// NullCount returns the number of null elements.
func (s Series[T]) NullCount() int {
	return arrowutil.NullCount(s.arr)
}
