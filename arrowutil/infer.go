package arrowutil

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/dtype"
)

// InferDType maps an Arrow array's type to a gopandas DType.
func InferDType(arr arrow.Array) dtype.DType {
	switch arr.DataType().ID() {
	case arrow.INT64:
		return dtype.Int64
	case arrow.FLOAT64:
		return dtype.Float64
	case arrow.STRING, arrow.LARGE_STRING:
		return dtype.String
	case arrow.BOOL:
		return dtype.Bool
	case arrow.TIMESTAMP:
		return dtype.Timestamp
	case arrow.DICTIONARY:
		return dtype.Dictionary
	default:
		return dtype.Invalid
	}
}

// BuildArray creates an Arrow array from a []any slice using the specified DType.
// nil values in the input become Arrow nulls.
// The caller is responsible for calling Release() on the returned array.
// Returns an error for unsupported dtypes or element type mismatches.
func BuildArray(alloc memory.Allocator, values []any, dt dtype.DType) (arrow.Array, error) {
	switch dt {
	case dtype.Int64:
		return buildInt64FromAny(alloc, values)
	case dtype.Float64:
		return buildFloat64FromAny(alloc, values)
	case dtype.String:
		return buildStringFromAny(alloc, values)
	case dtype.Bool:
		return buildBoolFromAny(alloc, values)
	case dtype.Timestamp:
		return buildTimestampFromAny(alloc, values)
	// dtype.Dictionary is intentionally not supported: dictionary arrays require
	// both index and value arrays and cannot be built from a flat []any slice.
	default:
		return nil, fmt.Errorf("arrowutil.BuildArray: unsupported dtype %v", dt)
	}
}

func buildInt64FromAny(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	for i, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		val, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("arrowutil.BuildArray: index %d: expected int64, got %T", i, v)
		}
		bldr.Append(val)
	}
	return bldr.NewInt64Array(), nil
}

func buildFloat64FromAny(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewFloat64Builder(alloc)
	defer bldr.Release()
	for i, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		val, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("arrowutil.BuildArray: index %d: expected float64, got %T", i, v)
		}
		bldr.Append(val)
	}
	return bldr.NewFloat64Array(), nil
}

func buildStringFromAny(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewStringBuilder(alloc)
	defer bldr.Release()
	for i, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		val, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("arrowutil.BuildArray: index %d: expected string, got %T", i, v)
		}
		bldr.Append(val)
	}
	return bldr.NewStringArray(), nil
}

func buildBoolFromAny(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewBooleanBuilder(alloc)
	defer bldr.Release()
	for i, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		val, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("arrowutil.BuildArray: index %d: expected bool, got %T", i, v)
		}
		bldr.Append(val)
	}
	return bldr.NewBooleanArray(), nil
}

func buildTimestampFromAny(alloc memory.Allocator, values []any) (arrow.Array, error) {
	dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bldr := array.NewTimestampBuilder(alloc, dt)
	defer bldr.Release()
	for i, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		val, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("arrowutil.BuildArray: index %d: expected time.Time, got %T", i, v)
		}
		bldr.Append(arrow.Timestamp(val.UnixMicro()))
	}
	return bldr.NewTimestampArray(), nil
}
