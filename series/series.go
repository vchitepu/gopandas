package series

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/arrowutil"
	"github.com/vchitepu/gopandas/dtype"
	"github.com/vchitepu/gopandas/index"
)

// Series is a single named column backed by an Apache Arrow array with label-based indexing.
// All operations are immutable — they return new Series values.
type Series[T any] struct {
	name  string
	index index.Index
	arr   arrow.Array
}

// New creates a Series[T] from a typed Go slice with the given index and name.
// T must be one of: int64, float64, string, bool.
// Panics if len(values) != idx.Len().
func New[T any](alloc memory.Allocator, values []T, idx index.Index, name string) Series[T] {
	if len(values) != idx.Len() {
		panic(fmt.Sprintf("series.New: len(values)=%d != index.Len()=%d", len(values), idx.Len()))
	}
	arr := buildTypedArray(alloc, values)
	return Series[T]{
		name:  name,
		index: idx,
		arr:   arr,
	}
}

// buildTypedArray dispatches to the correct arrowutil builder based on T's runtime type.
func buildTypedArray[T any](alloc memory.Allocator, values []T) arrow.Array {
	switch v := any(values).(type) {
	case []int64:
		return arrowutil.BuildInt64Array(alloc, v)
	case []float64:
		return arrowutil.BuildFloat64Array(alloc, v)
	case []string:
		return arrowutil.BuildStringArray(alloc, v)
	case []bool:
		return arrowutil.BuildBoolArray(alloc, v)
	case []any:
		// For Series[any], infer dtype from first non-nil element
		dt := inferDTypeFromAny(v)
		arr, err := arrowutil.BuildArray(alloc, v, dt)
		if err != nil {
			panic(fmt.Sprintf("series.New: failed to build []any array: %v", err))
		}
		return arr
	default:
		panic(fmt.Sprintf("series.New: unsupported type %T", values))
	}
}

// inferDTypeFromAny infers the dtype from the first non-nil element of a []any slice.
func inferDTypeFromAny(values []any) dtype.DType {
	for _, v := range values {
		if v == nil {
			continue
		}
		switch v.(type) {
		case int64:
			return dtype.Int64
		case float64:
			return dtype.Float64
		case string:
			return dtype.String
		case bool:
			return dtype.Bool
		case time.Time:
			return dtype.Timestamp
		}
	}
	return dtype.String // default
}

// FromArrow creates a Series[any] from a pre-built Arrow array.
// The caller retains ownership of arr; the Series takes a reference via Retain.
// Panics if arr.Len() != idx.Len().
func FromArrow(arr arrow.Array, idx index.Index, name string) Series[any] {
	if arr.Len() != idx.Len() {
		panic(fmt.Sprintf("series.FromArrow: arr.Len()=%d != index.Len()=%d", arr.Len(), idx.Len()))
	}
	arr.Retain()
	return Series[any]{
		name:  name,
		index: idx,
		arr:   arr,
	}
}

// Len returns the number of elements in the Series.
func (s Series[T]) Len() int {
	return arrowutil.ArrayLen(s.arr)
}

// Name returns the name of the Series.
func (s Series[T]) Name() string {
	return s.name
}

// DType returns the gopandas dtype of this Series, inferred from the underlying Arrow array.
func (s Series[T]) DType() dtype.DType {
	return arrowutil.InferDType(s.arr)
}

// Index returns the Index associated with this Series.
func (s Series[T]) Index() index.Index {
	return s.index
}

// String returns a human-readable string representation of the Series,
// showing index labels, values, name, dtype, and length.
func (s Series[T]) String() string {
	var b strings.Builder
	labels := s.index.Labels()
	for i := 0; i < s.Len(); i++ {
		label := fmt.Sprintf("%v", labels[i])
		val, err := arrowutil.GetValue(s.arr, i)
		var valStr string
		if err != nil || val == nil {
			valStr = "NaN"
		} else {
			valStr = fmt.Sprintf("%v", val)
		}
		b.WriteString(fmt.Sprintf("%-10s %s\n", label, valStr))
	}
	b.WriteString(fmt.Sprintf("Name: %s, dtype: %s, length: %d\n", s.name, s.DType(), s.Len()))
	return b.String()
}

// Array returns the underlying Arrow array.
// This is intended for internal use by other series methods and the dataframe package.
func (s Series[T]) Array() arrow.Array {
	return s.arr
}
