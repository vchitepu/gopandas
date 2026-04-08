package series

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/arrowutil"
	"github.com/vchitepu/gopandas/dtype"
	"github.com/vchitepu/gopandas/index"
)

// Map applies fn to every element and returns a new Series of the same type.
// Null values are passed as the zero value of T to fn.
func (s Series[T]) Map(fn func(T) T) Series[T] {
	n := s.Len()
	result := make([]T, n)
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			var zero T
			result[i] = fn(zero)
		} else {
			result[i] = fn(getTypedValue[T](s.arr, i))
		}
	}
	return New[T](memory.DefaultAllocator, result, s.index, s.name)
}

// Apply applies fn to every element and returns a new Series[any].
// The result dtype is inferred from the first non-nil return value.
// If all return values are nil, the result is a String series with all nulls.
func (s Series[T]) Apply(fn func(T) any) Series[any] {
	n := s.Len()
	results := make([]any, n)
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			var zero T
			results[i] = fn(zero)
		} else {
			results[i] = fn(getTypedValue[T](s.arr, i))
		}
	}

	// Infer dtype from first non-nil result
	var dt dtype.DType
	for _, r := range results {
		if r == nil {
			continue
		}
		switch r.(type) {
		case int64:
			dt = dtype.Int64
		case float64:
			dt = dtype.Float64
		case string:
			dt = dtype.String
		case bool:
			dt = dtype.Bool
		default:
			dt = dtype.String
			for j, v := range results {
				if v != nil {
					results[j] = fmt.Sprintf("%v", v)
				}
			}
		}
		break
	}
	if dt == dtype.Invalid {
		dt = dtype.String
	}

	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, results, dt)
	if err != nil {
		panic("series.Apply: " + err.Error())
	}

	return Series[any]{
		name:  s.name,
		index: s.index,
		arr:   arr,
	}
}

// Sort returns a new Series with values sorted in ascending or descending order.
// The index is reset to a new RangeIndex.
func (s Series[T]) Sort(ascending bool) Series[T] {
	n := s.Len()
	if n == 0 {
		return s
	}

	type pair struct {
		idx int
		val any
	}
	pairs := make([]pair, n)
	for i := 0; i < n; i++ {
		val, _ := arrowutil.GetValue(s.arr, i)
		pairs[i] = pair{idx: i, val: val}
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		a, b := pairs[i].val, pairs[j].val
		if a == nil && b == nil {
			return false
		}
		if a == nil {
			return !ascending
		}
		if b == nil {
			return ascending
		}
		var less bool
		switch va := a.(type) {
		case int64:
			less = va < b.(int64)
		case float64:
			less = va < b.(float64)
		case string:
			less = va < b.(string)
		case bool:
			less = !va && b.(bool)
		default:
			less = fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
		}
		if ascending {
			return less
		}
		return !less
	})

	vals := make([]T, n)
	for i, p := range pairs {
		vals[i] = getTypedValue[T](s.arr, p.idx)
	}

	newIdx := index.NewRangeIndex(n, s.index.Name())
	return New[T](memory.DefaultAllocator, vals, newIdx, s.name)
}

// Rename returns a new Series with the given name. Values and index are shared.
func (s Series[T]) Rename(name string) Series[T] {
	s.arr.Retain()
	return Series[T]{
		name:  name,
		index: s.index,
		arr:   s.arr,
	}
}

// AsType converts the Series to a new dtype, returning a Series[any].
// Supports conversions between numeric types and to/from string.
// Returns an error if the conversion is not supported or fails.
func (s Series[T]) AsType(d dtype.DType) (Series[any], error) {
	if s.DType() == d {
		s.arr.Retain()
		return Series[any]{
			name:  s.name,
			index: s.index,
			arr:   s.arr,
		}, nil
	}

	n := s.Len()
	results := make([]any, n)
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			results[i] = nil
			continue
		}
		val, err := arrowutil.GetValue(s.arr, i)
		if err != nil {
			return Series[any]{}, fmt.Errorf("series.AsType: %w", err)
		}
		converted, err := convertValue(val, d)
		if err != nil {
			return Series[any]{}, fmt.Errorf("series.AsType: index %d: %w", i, err)
		}
		results[i] = converted
	}

	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, results, d)
	if err != nil {
		return Series[any]{}, fmt.Errorf("series.AsType: %w", err)
	}
	return Series[any]{
		name:  s.name,
		index: s.index,
		arr:   arr,
	}, nil
}

// convertValue converts a single value to the target dtype.
func convertValue(val any, d dtype.DType) (any, error) {
	switch d {
	case dtype.Int64:
		switch v := val.(type) {
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to int64: %w", v, err)
			}
			return i, nil
		case bool:
			if v {
				return int64(1), nil
			}
			return int64(0), nil
		}
	case dtype.Float64:
		switch v := val.(type) {
		case int64:
			return float64(v), nil
		case float64:
			return v, nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to float64: %w", v, err)
			}
			return f, nil
		case bool:
			if v {
				return float64(1), nil
			}
			return float64(0), nil
		}
	case dtype.String:
		return fmt.Sprintf("%v", val), nil
	case dtype.Bool:
		switch v := val.(type) {
		case bool:
			return v, nil
		case int64:
			return v != 0, nil
		case float64:
			return v != 0, nil
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to bool: %w", v, err)
			}
			return b, nil
		}
	}
	return nil, fmt.Errorf("unsupported conversion from %T to %v", val, d)
}
