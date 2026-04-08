package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/dataframe"
	"github.com/vchitepu/gopandas/dtype"
	"github.com/vchitepu/gopandas/index"
)

// FromCSV reads CSV data from the given io.Reader and returns a DataFrame.
// Options can be used to customize parsing behavior.
func FromCSV(r io.Reader, opts ...CSVOption) (dataframe.DataFrame, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	reader := csv.NewReader(r)
	reader.Comma = cfg.sep
	reader.FieldsPerRecord = -1 // allow variable-length records

	allRows, err := reader.ReadAll()
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("csv.FromCSV: %w", err)
	}
	if len(allRows) == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("csv.FromCSV: empty input")
	}

	// --- Determine header ---
	var header []string
	var dataRows [][]string
	if cfg.header {
		header = allRows[0]
		dataRows = allRows[1:]
	} else {
		// Auto-generate column names: "0", "1", "2", ...
		nCols := 0
		if len(allRows) > 0 {
			nCols = len(allRows[0])
		}
		header = make([]string, nCols)
		for i := range header {
			header[i] = strconv.Itoa(i)
		}
		dataRows = allRows
	}

	// --- Apply skipRows ---
	if cfg.skipRows > 0 {
		if cfg.skipRows >= len(dataRows) {
			dataRows = nil
		} else {
			dataRows = dataRows[cfg.skipRows:]
		}
	}

	// --- Apply nRows ---
	if cfg.nRows > 0 && cfg.nRows < len(dataRows) {
		dataRows = dataRows[:cfg.nRows]
	}

	nRows := len(dataRows)

	// --- Determine column indices to use ---
	// Build a mapping: header index -> column name
	type colInfo struct {
		srcIdx int
		name   string
	}
	var selectedCols []colInfo

	if len(cfg.useCols) > 0 {
		// Build lookup from header name to index
		headerIdx := make(map[string]int, len(header))
		for i, h := range header {
			headerIdx[h] = i
		}
		for _, col := range cfg.useCols {
			idx, ok := headerIdx[col]
			if !ok {
				return dataframe.DataFrame{}, fmt.Errorf("csv.FromCSV: column %q not found in header", col)
			}
			selectedCols = append(selectedCols, colInfo{srcIdx: idx, name: col})
		}
	} else {
		for i, h := range header {
			selectedCols = append(selectedCols, colInfo{srcIdx: i, name: h})
		}
	}

	// --- Read raw string columns, replacing NA values with nil ---
	type rawColumn struct {
		name   string
		values []any // string values or nil for NA
	}
	rawCols := make([]rawColumn, len(selectedCols))
	for i, ci := range selectedCols {
		vals := make([]any, nRows)
		for r := 0; r < nRows; r++ {
			var cell string
			if ci.srcIdx < len(dataRows[r]) {
				cell = dataRows[r][ci.srcIdx]
			}
			if cfg.naValues[cell] {
				vals[r] = nil
			} else {
				vals[r] = cell
			}
		}
		rawCols[i] = rawColumn{name: ci.name, values: vals}
	}

	// --- Determine index ---
	var idx index.Index
	var dataCols []rawColumn

	if cfg.indexCol != "" {
		// Find the index column
		found := false
		for i, rc := range rawCols {
			if rc.name == cfg.indexCol {
				// Build string index from non-nil values
				labels := make([]string, nRows)
				for j, v := range rc.values {
					if v == nil {
						labels[j] = ""
					} else {
						labels[j] = v.(string)
					}
				}
				idx = index.NewStringIndex(labels, cfg.indexCol)
				// Remove from data columns (allocate new slice to avoid mutating rawCols)
				dataCols = make([]rawColumn, 0, len(rawCols)-1)
				dataCols = append(dataCols, rawCols[:i]...)
				dataCols = append(dataCols, rawCols[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return dataframe.DataFrame{}, fmt.Errorf("csv.FromCSV: indexCol %q not found", cfg.indexCol)
		}
	} else {
		idx = index.NewRangeIndex(nRows, "")
		dataCols = rawCols
	}

	// --- Type inference and Arrow array building ---
	alloc := memory.DefaultAllocator
	nDataCols := len(dataCols)
	fields := make([]arrow.Field, nDataCols)
	arrays := make([]arrow.Array, nDataCols)

	for i, rc := range dataCols {
		dt, ok := cfg.dtypeOverride[rc.name]
		if !ok {
			dt = inferColumnType(rc.values, cfg.parseDateCols[rc.name], cfg.dateFormats)
		}

		arr, err := buildArrowArray(alloc, rc.values, dt, cfg.dateFormats)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("csv.FromCSV: column %q: %w", rc.name, err)
		}
		fields[i] = arrow.Field{Name: rc.name, Type: arr.DataType()}
		arrays[i] = arr
	}

	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, arrays, int64(nRows))
	defer rec.Release()

	// Release the arrays we built (the Record retains them)
	for _, arr := range arrays {
		arr.Release()
	}

	return dataframe.FromArrowWithIndex(rec, idx)
}

// inferColumnType tries int64 → float64 → timestamp (optional) → string.
// If the column is all integers but has any NA, promote to float64.
func inferColumnType(values []any, tryDate bool, dateFormats []string) dtype.DType {
	hasNull := false
	allInt := true
	allFloat := true
	allDate := tryDate

	for _, v := range values {
		if v == nil {
			hasNull = true
			continue
		}
		s := v.(string)
		if allInt {
			_, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				allInt = false
			}
		}
		if allFloat {
			_, err := strconv.ParseFloat(s, 64)
			if err != nil {
				allFloat = false
			}
		}
		if allDate {
			if _, ok := parseDateString(s, dateFormats); !ok {
				allDate = false
			}
		}
		// If neither int nor float, it's string — can break early
		if !allInt && !allFloat && !allDate {
			break
		}
	}

	if allInt && !hasNull {
		return dtype.Int64
	}
	// Integers with NA → promote to float64 (pandas behavior)
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

// buildArrowArray converts []any (string values or nil) into a typed Arrow array.
func buildArrowArray(alloc memory.Allocator, values []any, dt dtype.DType, dateFormats []string) (arrow.Array, error) {
	switch dt {
	case dtype.Int64:
		return buildInt64Array(alloc, values)
	case dtype.Float64:
		return buildFloat64Array(alloc, values)
	case dtype.Timestamp:
		return buildTimestampArray(alloc, values, dateFormats)
	case dtype.String:
		return buildStringArray(alloc, values)
	default:
		return nil, fmt.Errorf("unsupported dtype %v for CSV column", dt)
	}
}

func parseDateString(s string, dateFormats []string) (time.Time, bool) {
	for _, layout := range dateFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func buildInt64Array(alloc memory.Allocator, values []any) (arrow.Array, error) {
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	for _, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}
		n, err := strconv.ParseInt(v.(string), 10, 64)
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
		if v == nil {
			bldr.AppendNull()
			continue
		}
		f, err := strconv.ParseFloat(v.(string), 64)
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
		if v == nil {
			bldr.AppendNull()
			continue
		}
		bldr.Append(v.(string))
	}
	return bldr.NewStringArray(), nil
}

func buildTimestampArray(alloc memory.Allocator, values []any, dateFormats []string) (arrow.Array, error) {
	// Use microsecond timestamp in UTC to align with existing dtype.Timestamp handling.
	dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bldr := array.NewTimestampBuilder(alloc, dt)
	defer bldr.Release()

	for _, v := range values {
		if v == nil {
			bldr.AppendNull()
			continue
		}

		t, ok := parseDateString(v.(string), dateFormats)
		if !ok {
			return nil, fmt.Errorf("cannot parse %q as timestamp", v)
		}
		bldr.Append(arrow.Timestamp(t.UnixMicro()))
	}

	return bldr.NewTimestampArray(), nil
}
