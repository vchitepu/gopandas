package dataframe

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
)

// SortBy sorts the DataFrame by one or more columns.
// cols and ascending must have the same length.
func (df DataFrame) SortBy(cols []string, ascending []bool) (DataFrame, error) {
	if len(cols) != len(ascending) {
		return DataFrame{}, fmt.Errorf("dataframe.SortBy: cols and ascending must have same length")
	}
	for _, col := range cols {
		if _, ok := df.data[col]; !ok {
			return DataFrame{}, fmt.Errorf("dataframe.SortBy: column %q not found", col)
		}
	}

	n := df.Len()
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(i, j int) bool {
		ri, rj := indices[i], indices[j]
		for k, col := range cols {
			s := df.data[col]
			vi, _ := s.At(ri)
			vj, _ := s.At(rj)
			cmp := compareAny(vi, vj)
			if cmp == 0 {
				continue
			}
			if ascending[k] {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})

	// Rebuild DataFrame with sorted rows
	newIdx := index.NewRangeIndex(n, "")
	newData := make(map[string]*series.Series[any], len(df.columns))
	for _, col := range df.columns {
		src := df.data[col]
		vals := make([]any, n)
		for i, pos := range indices {
			val, _ := src.At(pos)
			vals[i] = val
		}
		s := series.New[any](memory.DefaultAllocator, vals, newIdx, col)
		newData[col] = &s
	}

	return DataFrame{index: newIdx, columns: df.Columns(), data: newData}, nil
}

// compareAny compares two values. Nils sort last.
func compareAny(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // nils last
	}
	if b == nil {
		return -1
	}

	af, aOK := toFloat64Sort(a)
	bf, bOK := toFloat64Sort(b)
	if aOK && bOK {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	// Fallback to string comparison
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

// toFloat64Sort converts a value to float64 for sorting.
func toFloat64Sort(v any) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}
