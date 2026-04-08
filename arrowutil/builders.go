package arrowutil

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BuildInt64Array creates an Arrow Int64 array from a Go slice.
func BuildInt64Array(alloc memory.Allocator, values []int64) *array.Int64 {
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	bldr.AppendValues(values, nil)
	return bldr.NewInt64Array()
}

// BuildFloat64Array creates an Arrow Float64 array from a Go slice.
func BuildFloat64Array(alloc memory.Allocator, values []float64) *array.Float64 {
	bldr := array.NewFloat64Builder(alloc)
	defer bldr.Release()
	bldr.AppendValues(values, nil)
	return bldr.NewFloat64Array()
}

// BuildStringArray creates an Arrow String array from a Go slice.
func BuildStringArray(alloc memory.Allocator, values []string) *array.String {
	bldr := array.NewStringBuilder(alloc)
	defer bldr.Release()
	bldr.AppendValues(values, nil)
	return bldr.NewStringArray()
}

// BuildBoolArray creates an Arrow Boolean array from a Go slice.
func BuildBoolArray(alloc memory.Allocator, values []bool) *array.Boolean {
	bldr := array.NewBooleanBuilder(alloc)
	defer bldr.Release()
	bldr.AppendValues(values, nil)
	return bldr.NewBooleanArray()
}

// BuildTimestampArray creates an Arrow Timestamp (microsecond, UTC) array from a Go time.Time slice.
func BuildTimestampArray(alloc memory.Allocator, values []time.Time) *array.Timestamp {
	dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bldr := array.NewTimestampBuilder(alloc, dt)
	defer bldr.Release()
	for _, v := range values {
		bldr.Append(arrow.Timestamp(v.UnixMicro()))
	}
	return bldr.NewTimestampArray()
}
