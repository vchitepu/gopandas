package ops

import (
	"fmt"
	"sort"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// Transpose transposes a DataFrame: column names become "index" column values,
// row indices become column headers ("0", "1", ...).
func Transpose(df dataframe.DataFrame) (dataframe.DataFrame, error) {
	cols := df.Columns()
	nRows := df.Len()

	// Each original column becomes a row in the transposed DataFrame.
	records := make([]map[string]any, len(cols))
	for i, col := range cols {
		rec := map[string]any{"index": col}
		for r := 0; r < nRows; r++ {
			colName := fmt.Sprintf("%d", r)
			v, _ := df.At(r, col)
			rec[colName] = v
		}
		records[i] = rec
	}

	return dataframe.FromRecords(records)
}

// Stack converts every cell into a (row, column, value) triple.
// Output has 3 columns: "row", "column", "value".
func Stack(df dataframe.DataFrame) (dataframe.DataFrame, error) {
	cols := df.Columns()
	nRows := df.Len()

	records := make([]map[string]any, 0, nRows*len(cols))
	for r := 0; r < nRows; r++ {
		for _, col := range cols {
			v, _ := df.At(r, col)
			rec := map[string]any{
				"row":    int64(r),
				"column": col,
				"value":  v,
			}
			records = append(records, rec)
		}
	}

	return dataframe.FromRecords(records)
}

// Unstack is the inverse of Stack. The named column's unique values become new column headers.
// Expects at least 3 columns. The last non-column column is treated as the value column.
// All other columns (except the unstacked one and the value column) become identity columns.
func Unstack(df dataframe.DataFrame, column string) (dataframe.DataFrame, error) {
	cols := df.Columns()
	if len(cols) < 3 {
		return dataframe.DataFrame{}, fmt.Errorf("ops.Unstack: need at least 3 columns, got %d", len(cols))
	}

	if _, err := df.Col(column); err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("ops.Unstack: column %q not found", column)
	}

	// Determine id columns and value column.
	// The value column is the last column that isn't the unstacked column.
	var valueCol string
	for i := len(cols) - 1; i >= 0; i-- {
		if cols[i] != column {
			valueCol = cols[i]
			break
		}
	}

	var idCols []string
	for _, c := range cols {
		if c != column && c != valueCol {
			idCols = append(idCols, c)
		}
	}

	// Get unique values of the column to unstack (these become new column headers).
	newColVals := uniqueValues(df, column)
	sort.Strings(newColVals)

	// Build a map: (idKey, colVal) -> value.
	type cellKey struct {
		id  string
		col string
	}
	cells := make(map[cellKey]any)

	// Collect unique id keys in order.
	idKeyOrder := make([]string, 0)
	idKeySet := make(map[string]bool)
	idKeyVals := make(map[string]map[string]any) // idKey -> idCol -> value

	for i := 0; i < df.Len(); i++ {
		// Build id key.
		idParts := make([]string, len(idCols))
		idMap := make(map[string]any, len(idCols))
		for j, id := range idCols {
			v, _ := df.At(i, id)
			idParts[j] = fmt.Sprintf("%v", v)
			idMap[id] = v
		}
		idKey := fmt.Sprintf("%v", idParts)

		colVal, _ := df.At(i, column)
		val, _ := df.At(i, valueCol)

		ck := cellKey{idKey, fmt.Sprintf("%v", colVal)}
		cells[ck] = val

		if !idKeySet[idKey] {
			idKeySet[idKey] = true
			idKeyOrder = append(idKeyOrder, idKey)
			idKeyVals[idKey] = idMap
		}
	}

	// Build records.
	records := make([]map[string]any, len(idKeyOrder))
	for i, idKey := range idKeyOrder {
		rec := make(map[string]any)
		// Add id columns.
		for col, val := range idKeyVals[idKey] {
			rec[col] = val
		}
		// Add new columns from unstacked values.
		for _, colVal := range newColVals {
			ck := cellKey{idKey, colVal}
			if v, ok := cells[ck]; ok {
				rec[colVal] = v
			} else {
				rec[colVal] = nil
			}
		}
		records[i] = rec
	}

	return dataframe.FromRecords(records)
}
