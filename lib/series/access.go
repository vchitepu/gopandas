package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/vchitepu/gopandas/lib/arrowutil"
)

// At returns the value at positional index i and a boolean indicating if the value is null.
// Panics if i is out of bounds.
func (s Series[T]) At(i int) (T, bool) {
	if i < 0 || i >= s.Len() {
		panic("series.At: index out of bounds")
	}
	if arrowutil.IsNull(s.arr, i) {
		var zero T
		return zero, true
	}
	return getTypedValue[T](s.arr, i), false
}

// getTypedValue extracts a value from an Arrow array and returns it as type T.
func getTypedValue[T any](arr arrow.Array, i int) T {
	val, err := arrowutil.GetValue(arr, i)
	if err != nil {
		panic("series: getTypedValue failed: " + err.Error())
	}
	// For Series[any], val can be returned directly.
	// For typed Series, we need a type assertion.
	if v, ok := val.(T); ok {
		return v
	}
	// Fallback: return via any conversion (handles Series[any] case)
	var result T
	switch p := any(&result).(type) {
	case *any:
		*p = val
	}
	return result
}

// Loc returns the value at the given index label and a boolean indicating if the value is null.
// Panics if the label is not found in the index.
func (s Series[T]) Loc(label any) (T, bool) {
	pos, ok := s.index.Loc(label)
	if !ok {
		panic("series.Loc: label not found in index")
	}
	return s.At(pos)
}

// Values returns all values in the Series as a typed slice.
// Null values are represented as the zero value of T.
func (s Series[T]) Values() []T {
	n := s.Len()
	result := make([]T, n)
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			var zero T
			result[i] = zero
		} else {
			result[i] = getTypedValue[T](s.arr, i)
		}
	}
	return result
}

// IsNull returns true if the value at positional index i is null.
// Panics if i is out of bounds.
func (s Series[T]) IsNull(i int) bool {
	if i < 0 || i >= s.Len() {
		panic("series.IsNull: index out of bounds")
	}
	return arrowutil.IsNull(s.arr, i)
}
