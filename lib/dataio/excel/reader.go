package excel

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dtype"
)

// defaultDateFormats are the Go time layouts tried when inferring dates.
var defaultDateFormats = []string{
	"01/02/2006",
	"1/2/2006",
	"2006-01-02",
	"2006/01/02",
	"01-02-2006",
	"1-2-2006",
}

func valueToString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

// inferColumnType tries int64 -> float64 -> timestamp -> string.
// If all integers but any NA present, promotes to float64.
func inferColumnType(values []any) dtype.DType {
	hasNull := false
	allInt := true
	allFloat := true
	allDate := true

	for _, v := range values {
		s, ok := valueToString(v)
		if !ok {
			hasNull = true
			continue
		}

		if allInt {
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				allInt = false
			}
		}
		if allFloat {
			if _, err := strconv.ParseFloat(s, 64); err != nil {
				allFloat = false
			}
		}
		if allDate {
			if _, ok := parseDateString(s); !ok {
				allDate = false
			}
		}
		if !allInt && !allFloat && !allDate {
			break
		}
	}

	if allInt && !hasNull {
		return dtype.Int64
	}
	if allInt && hasNull {
		return dtype.Float64
	}
	if allFloat {
		return dtype.Float64
	}
	if allDate {
		return dtype.Timestamp
	}
	return dtype.String
}

func parseDateString(s string) (time.Time, bool) {
	for _, layout := range defaultDateFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// buildArrowArray converts []any (string values or nil) into a typed Arrow array.
func buildArrowArray(alloc memory.Allocator, values []any, dt dtype.DType) (arrow.Array, error) {
	switch dt {
	case dtype.Int64:
		return buildInt64Array(alloc, values)
	case dtype.Float64:
		return buildFloat64Array(alloc, values)
	case dtype.Timestamp:
		return buildTimestampArray(alloc, values)
	case dtype.String:
		return buildStringArray(alloc, values)
	default:
		return nil, fmt.Errorf("unsupported dtype %v", dt)
	}
}

func buildInt64Array(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()

	for _, v := range values {
		s, ok := valueToString(v)
		if !ok {
			if v == nil {
				bldr.AppendNull()
				continue
			}
			return nil, fmt.Errorf("cannot convert %v to string for int64 parsing", v)
		}

		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as int64: %w", v, err)
		}
		bldr.Append(n)
	}

	return bldr.NewInt64Array(), nil
}

func buildFloat64Array(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewFloat64Builder(alloc)
	defer bldr.Release()

	for _, v := range values {
		s, ok := valueToString(v)
		if !ok {
			if v == nil {
				bldr.AppendNull()
				continue
			}
			return nil, fmt.Errorf("cannot convert %v to string for float64 parsing", v)
		}

		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as float64: %w", v, err)
		}
		bldr.Append(f)
	}

	return bldr.NewFloat64Array(), nil
}

func buildStringArray(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewStringBuilder(alloc)
	defer bldr.Release()

	for _, v := range values {
		s, ok := valueToString(v)
		if !ok {
			if v == nil {
				bldr.AppendNull()
				continue
			}
			return nil, fmt.Errorf("cannot convert %v to string", v)
		}
		bldr.Append(s)
	}

	return bldr.NewStringArray(), nil
}

func buildTimestampArray(alloc memory.Allocator, values []any) (arrow.Array, error) {
	dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bldr := array.NewTimestampBuilder(alloc, dt)
	defer bldr.Release()

	for _, v := range values {
		s, ok := valueToString(v)
		if !ok {
			if v == nil {
				bldr.AppendNull()
				continue
			}
			return nil, fmt.Errorf("cannot convert %v to string for timestamp parsing", v)
		}

		t, ok := parseDateString(s)
		if !ok {
			return nil, fmt.Errorf("cannot parse %q as timestamp", v)
		}
		bldr.Append(arrow.Timestamp(t.UnixMicro()))
	}

	return bldr.NewTimestampArray(), nil
}
