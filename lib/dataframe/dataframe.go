package dataframe

import (
	"fmt"
	"sort"
	"strings"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// DataFrame is a two-dimensional, column-oriented data structure with labeled rows and columns.
type DataFrame struct {
	index   index.Index
	columns []string
	data    map[string]*series.Series[any]
}

// New creates a DataFrame from a map[string]any where values are typed slices
// ([]int64, []float64, []string, []bool). Column names are sorted deterministically.
// All slices must have the same length.
func New(data map[string]any) (DataFrame, error) {
	if len(data) == 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0, ""),
			columns: []string{},
			data:    map[string]*series.Series[any]{},
		}, nil
	}

	// Sort column names for deterministic order
	cols := make([]string, 0, len(data))
	for k := range data {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	// Check all slices have the same length
	var nRows int = -1
	for _, col := range cols {
		n, err := sliceLen(data[col])
		if err != nil {
			return DataFrame{}, fmt.Errorf("dataframe.New: column %q: %w", col, err)
		}
		if nRows == -1 {
			nRows = n
		} else if n != nRows {
			return DataFrame{}, fmt.Errorf("dataframe.New: column lengths mismatch: got %d and %d", nRows, n)
		}
	}

	idx := index.NewRangeIndex(nRows, "")
	colData := make(map[string]*series.Series[any], len(cols))
	for _, col := range cols {
		s, err := buildSeries(col, data[col], idx)
		if err != nil {
			return DataFrame{}, fmt.Errorf("dataframe.New: column %q: %w", col, err)
		}
		colData[col] = s
	}

	return DataFrame{
		index:   idx,
		columns: cols,
		data:    colData,
	}, nil
}

// FromRecords creates a DataFrame from a slice of row-maps.
// Missing keys produce nil values. Column names are sorted deterministically.
func FromRecords(records []map[string]any) (DataFrame, error) {
	if len(records) == 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0, ""),
			columns: []string{},
			data:    map[string]*series.Series[any]{},
		}, nil
	}

	// Collect all column names
	colSet := make(map[string]bool)
	for _, rec := range records {
		for k := range rec {
			colSet[k] = true
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	nRows := len(records)
	idx := index.NewRangeIndex(nRows, "")
	colData := make(map[string]*series.Series[any], len(cols))

	for _, col := range cols {
		vals := make([]any, nRows)
		for i, rec := range records {
			vals[i] = rec[col] // nil if key missing
		}
		s := series.New[any](memory.DefaultAllocator, vals, idx, col)
		colData[col] = &s
	}

	return DataFrame{
		index:   idx,
		columns: cols,
		data:    colData,
	}, nil
}

// FromArrow creates a DataFrame from an Arrow Record.
// Column order matches the schema field order.
func FromArrow(rec arrowlib.Record) (DataFrame, error) {
	nRows := int(rec.NumRows())
	nCols := int(rec.NumCols())

	idx := index.NewRangeIndex(nRows, "")
	cols := make([]string, nCols)
	colData := make(map[string]*series.Series[any], nCols)

	schema := rec.Schema()
	for i := 0; i < nCols; i++ {
		field := schema.Field(i)
		cols[i] = field.Name
		s := series.FromArrow(rec.Column(i), idx, field.Name)
		colData[field.Name] = &s
	}

	return DataFrame{
		index:   idx,
		columns: cols,
		data:    colData,
	}, nil
}

// FromArrowWithIndex creates a DataFrame from an Arrow Record with a custom index.
func FromArrowWithIndex(rec arrowlib.Record, idx index.Index) (DataFrame, error) {
	if idx.Len() != int(rec.NumRows()) {
		return DataFrame{}, fmt.Errorf("index length %d does not match record row count %d", idx.Len(), int(rec.NumRows()))
	}

	nCols := int(rec.NumCols())
	cols := make([]string, nCols)
	colData := make(map[string]*series.Series[any], nCols)

	schema := rec.Schema()
	for i := 0; i < nCols; i++ {
		field := schema.Field(i)
		cols[i] = field.Name
		s := series.FromArrow(rec.Column(i), idx, field.Name)
		colData[field.Name] = &s
	}

	return DataFrame{
		index:   idx,
		columns: cols,
		data:    colData,
	}, nil
}

// sliceLen returns the length of a typed slice wrapped in any.
func sliceLen(v any) (int, error) {
	switch s := v.(type) {
	case []int64:
		return len(s), nil
	case []float64:
		return len(s), nil
	case []string:
		return len(s), nil
	case []bool:
		return len(s), nil
	case []any:
		return len(s), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// buildSeries creates a *Series[any] from a typed slice with the given index.
func buildSeries(name string, v any, idx index.Index) (*series.Series[any], error) {
	var s series.Series[any]
	switch vals := v.(type) {
	case []int64:
		s = series.New[any](memory.DefaultAllocator, toAnySlice(vals), idx, name)
	case []float64:
		s = series.New[any](memory.DefaultAllocator, toAnySlice(vals), idx, name)
	case []string:
		s = series.New[any](memory.DefaultAllocator, toAnySlice(vals), idx, name)
	case []bool:
		s = series.New[any](memory.DefaultAllocator, toAnySlice(vals), idx, name)
	case []any:
		s = series.New[any](memory.DefaultAllocator, vals, idx, name)
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
	return &s, nil
}

// toAnySlice converts a typed slice to []any.
func toAnySlice[T any](s []T) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

// Shape returns (rows, cols).
func (df DataFrame) Shape() (int, int) {
	return df.Len(), len(df.columns)
}

// Columns returns a copy of the column names.
func (df DataFrame) Columns() []string {
	out := make([]string, len(df.columns))
	copy(out, df.columns)
	return out
}

// Len returns the number of rows.
func (df DataFrame) Len() int {
	return df.index.Len()
}

// DTypes returns a map of column name to dtype.DType.
func (df DataFrame) DTypes() map[string]dtype.DType {
	out := make(map[string]dtype.DType, len(df.columns))
	for _, col := range df.columns {
		out[col] = df.data[col].DType()
	}
	return out
}

// Index returns the Index of the DataFrame.
func (df DataFrame) Index() index.Index {
	return df.index
}

// String returns a tabular string representation of the DataFrame.
func (df DataFrame) String() string {
	if len(df.columns) == 0 || df.Len() == 0 {
		return "Empty DataFrame"
	}

	nRows := df.Len()
	labels := df.index.Labels()

	// Compute column widths: index column + data columns
	idxWidth := 5 // minimum width
	for i := 0; i < nRows; i++ {
		w := len(fmt.Sprintf("%v", labels[i]))
		if w > idxWidth {
			idxWidth = w
		}
	}

	colWidths := make([]int, len(df.columns))
	for j, col := range df.columns {
		colWidths[j] = len(col)
		s := df.data[col]
		for i := 0; i < nRows; i++ {
			val, isNull := s.At(i)
			var str string
			if isNull {
				str = "NaN"
			} else {
				str = fmt.Sprintf("%v", val)
			}
			if len(str) > colWidths[j] {
				colWidths[j] = len(str)
			}
		}
	}

	var b strings.Builder
	// Header
	b.WriteString(padRight("", idxWidth))
	for j, col := range df.columns {
		b.WriteString("  ")
		b.WriteString(padRight(col, colWidths[j]))
	}
	b.WriteString("\n")

	// Rows
	for i := 0; i < nRows; i++ {
		b.WriteString(padRight(fmt.Sprintf("%v", labels[i]), idxWidth))
		for j, col := range df.columns {
			b.WriteString("  ")
			s := df.data[col]
			val, isNull := s.At(i)
			var str string
			if isNull {
				str = "NaN"
			} else {
				str = fmt.Sprintf("%v", val)
			}
			b.WriteString(padRight(str, colWidths[j]))
		}
		b.WriteString("\n")
	}

	return strings.TrimRight(b.String(), "\n")
}

// padRight pads a string with spaces to the given width.
func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}
