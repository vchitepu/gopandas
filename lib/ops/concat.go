package ops

import (
	"fmt"
	"sort"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

// Concat vertically concatenates multiple DataFrames.
// The union of all columns is used. Missing columns are filled with nil.
func Concat(dfs ...dataframe.DataFrame) (dataframe.DataFrame, error) {
	if len(dfs) == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("ops.Concat: no DataFrames provided")
	}
	if len(dfs) == 1 {
		return dfs[0], nil
	}

	// Collect union of all column names.
	colSet := make(map[string]bool)
	for _, df := range dfs {
		for _, c := range df.Columns() {
			colSet[c] = true
		}
	}
	allCols := make([]string, 0, len(colSet))
	for c := range colSet {
		allCols = append(allCols, c)
	}
	sort.Strings(allCols)

	// Build records by iterating all rows of all DataFrames.
	var records []map[string]any
	for _, df := range dfs {
		dfCols := make(map[string]bool, len(df.Columns()))
		for _, c := range df.Columns() {
			dfCols[c] = true
		}
		for i := 0; i < df.Len(); i++ {
			rec := make(map[string]any, len(allCols))
			for _, col := range allCols {
				if dfCols[col] {
					v, _ := df.At(i, col)
					rec[col] = v
				} else {
					rec[col] = nil
				}
			}
			records = append(records, rec)
		}
	}

	return dataframe.FromRecords(records)
}
