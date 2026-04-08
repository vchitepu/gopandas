package arrowutil

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ArrayLen returns the number of elements in an Arrow array.
func ArrayLen(arr arrow.Array) int { return arr.Len() }

// NullCount returns the number of null elements in an Arrow array.
func NullCount(arr arrow.Array) int { return arr.NullN() }

// IsNull returns true if the element at position i is null.
func IsNull(arr arrow.Array, i int) bool { return arr.IsNull(i) }

// GetValue extracts a single value from an Arrow array at position i.
// Returns nil for null values. Returns an error for out-of-bounds or unsupported types.
func GetValue(arr arrow.Array, i int) (any, error) {
	if i < 0 || i >= arr.Len() {
		return nil, fmt.Errorf("arrowutil.GetValue: index %d out of bounds [0, %d)", i, arr.Len())
	}
	if arr.IsNull(i) {
		return nil, nil
	}
	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Timestamp:
		dt := a.DataType().(*arrow.TimestampType)
		raw := int64(a.Value(i))
		var ns int64
		switch dt.Unit {
		case arrow.Second:
			ns = raw * int64(time.Second)
		case arrow.Millisecond:
			ns = raw * int64(time.Millisecond)
		case arrow.Microsecond:
			ns = raw * int64(time.Microsecond)
		case arrow.Nanosecond:
			ns = raw
		default:
			ns = raw * int64(time.Microsecond) // fallback
		}
		return time.Unix(0, ns).UTC(), nil
	default:
		return nil, fmt.Errorf("arrowutil.GetValue: unsupported array type %T", arr)
	}
}

// SliceArray returns a zero-copy slice of arr from start (inclusive) to end (exclusive).
// The caller must call Release() on the returned array.
// Panics if start > end or if bounds are outside [0, arr.Len()].
func SliceArray(arr arrow.Array, start, end int) arrow.Array {
	return array.NewSlice(arr, int64(start), int64(end))
}
