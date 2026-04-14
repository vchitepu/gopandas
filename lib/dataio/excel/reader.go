package excel

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/xuri/excelize/v2"
)

// FromXLSX reads an xlsx workbook from r and returns a DataFrame.
// By default, the first sheet is read. Use WithSheetName or WithSheetIndex to select another.
func FromXLSX(r io.Reader, opts ...XLSXOption) (dataframe.DataFrame, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.sheetIndex < unsetSheetIndex {
		return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: invalid sheet index %d", cfg.sheetIndex)
	}

	f, err := excelize.OpenReader(r)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: %w", err)
	}
	defer f.Close()

	sheetName, err := resolveSheet(f, cfg)
	if err != nil {
		return dataframe.DataFrame{}, err
	}

	allRows, err := f.GetRows(sheetName)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: %w", err)
	}

	allRows = trimTrailingEmptyRows(allRows)
	if len(allRows) == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: empty sheet")
	}

	header := allRows[0]
	if len(header) == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: empty or invalid header row")
	}
	for _, col := range header {
		if strings.TrimSpace(col) == "" {
			return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: empty or invalid header row")
		}
	}

	dataRows := allRows[1:]
	nCols := len(header)
	nRows := len(dataRows)

	for i := range dataRows {
		if len(dataRows[i]) < nCols {
			padded := make([]string, nCols)
			copy(padded, dataRows[i])
			dataRows[i] = padded
		}
	}

	type rawColumn struct {
		name   string
		values []any
	}
	rawCols := make([]rawColumn, nCols)
	for c := 0; c < nCols; c++ {
		vals := make([]any, nRows)
		for r := 0; r < nRows; r++ {
			cell := dataRows[r][c]
			if cell == "" {
				vals[r] = nil
			} else {
				vals[r] = cell
			}
		}
		rawCols[c] = rawColumn{name: header[c], values: vals}
	}

	alloc := memory.DefaultAllocator
	fields := make([]arrow.Field, nCols)
	arrays := make([]arrow.Array, nCols)

	for i, rc := range rawCols {
		dt := inferColumnType(rc.values)
		arr, err := buildArrowArray(alloc, rc.values, dt)
		if err != nil {
			for j := 0; j < i; j++ {
				if arrays[j] != nil {
					arrays[j].Release()
				}
			}
			return dataframe.DataFrame{}, fmt.Errorf("excel.FromXLSX: column %q: %w", rc.name, err)
		}
		fields[i] = arrow.Field{Name: rc.name, Type: arr.DataType()}
		arrays[i] = arr
	}

	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, arrays, int64(nRows))
	defer rec.Release()

	for _, arr := range arrays {
		arr.Release()
	}

	return dataframe.FromArrow(rec)
}

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

// resolveSheet determines which sheet to read based on config.
func resolveSheet(f *excelize.File, cfg xlsxConfig) (string, error) {
	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return "", fmt.Errorf("excel.FromXLSX: workbook has no sheets")
	}

	if cfg.sheetName != "" {
		for _, sheet := range sheets {
			if sheet == cfg.sheetName {
				return sheet, nil
			}
		}
		return "", fmt.Errorf("excel.FromXLSX: sheet %q not found", cfg.sheetName)
	}

	if cfg.sheetIndex >= 0 {
		if cfg.sheetIndex >= len(sheets) {
			return "", fmt.Errorf("excel.FromXLSX: sheet index %d out of range (file has %d sheets)", cfg.sheetIndex, len(sheets))
		}
		return sheets[cfg.sheetIndex], nil
	}

	return sheets[0], nil
}

// trimTrailingEmptyRows removes trailing rows where every cell is empty.
func trimTrailingEmptyRows(rows [][]string) [][]string {
	for i := len(rows) - 1; i >= 0; i-- {
		empty := true
		for _, cell := range rows[i] {
			if cell != "" {
				empty = false
				break
			}
		}
		if !empty {
			return rows[:i+1]
		}
	}

	return nil
}
