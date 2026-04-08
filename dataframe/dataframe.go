package dataframe

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/dtype"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
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
