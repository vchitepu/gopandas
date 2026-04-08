package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/dtype"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
)

// WithColumn returns a new DataFrame with the given column added or replaced.
// Returns an error if the series length does not match the DataFrame length.
func (df DataFrame) WithColumn(name string, s *series.Series[any]) (DataFrame, error) {
	if s.Len() != df.Len() {
		return DataFrame{}, fmt.Errorf("dataframe.WithColumn: series length %d does not match DataFrame length %d", s.Len(), df.Len())
	}

	newData := make(map[string]*series.Series[any], len(df.data)+1)
	for k, v := range df.data {
		newData[k] = v
	}
	newData[name] = s

	// Preserve column order, add new column at end if not replacing
	var newCols []string
	found := false
	for _, c := range df.columns {
		newCols = append(newCols, c)
		if c == name {
			found = true
		}
	}
	if !found {
		newCols = append(newCols, name)
	}

	return DataFrame{index: df.index, columns: newCols, data: newData}, nil
}

// Rename returns a new DataFrame with columns renamed according to the mapping.
func (df DataFrame) Rename(mapping map[string]string) DataFrame {
	newCols := make([]string, len(df.columns))
	newData := make(map[string]*series.Series[any], len(df.data))

	for i, oldName := range df.columns {
		newName := oldName
		if n, ok := mapping[oldName]; ok {
			newName = n
		}
		newCols[i] = newName
		s := df.data[oldName].Rename(newName)
		newData[newName] = &s
	}

	return DataFrame{index: df.index, columns: newCols, data: newData}
}

// SetIndex sets the named column as the row index, removing it from the columns.
func (df DataFrame) SetIndex(col string) (DataFrame, error) {
	s, ok := df.data[col]
	if !ok {
		return DataFrame{}, fmt.Errorf("dataframe.SetIndex: column %q not found", col)
	}

	// Extract string labels from the column
	labels := make([]string, df.Len())
	for i := 0; i < df.Len(); i++ {
		val, isNull := s.At(i)
		if isNull {
			labels[i] = ""
		} else {
			labels[i] = fmt.Sprintf("%v", val)
		}
	}
	newIdx := index.NewStringIndex(labels, col)

	// Remove the column from data
	var newCols []string
	newData := make(map[string]*series.Series[any], len(df.data)-1)
	for _, c := range df.columns {
		if c != col {
			newCols = append(newCols, c)
			newData[c] = df.data[c]
		}
	}
	if newCols == nil {
		newCols = []string{}
	}

	return DataFrame{index: newIdx, columns: newCols, data: newData}, nil
}

// ResetIndex resets the index to a RangeIndex.
// If drop is false, the old index is inserted as a column.
func (df DataFrame) ResetIndex(drop bool) DataFrame {
	newIdx := index.NewRangeIndex(df.Len(), "")
	newData := make(map[string]*series.Series[any], len(df.data)+1)
	newCols := make([]string, 0, len(df.columns)+1)

	if !drop {
		idxName := df.index.Name()
		if idxName == "" {
			idxName = "index"
		}
		labels := df.index.Labels()
		vals := make([]any, len(labels))
		for i, l := range labels {
			// Convert label to string for any type
			vals[i] = fmt.Sprintf("%v", l)
		}
		s := series.New[any](memory.DefaultAllocator, vals, newIdx, idxName)
		newData[idxName] = &s
		newCols = append(newCols, idxName)
	}

	for _, col := range df.columns {
		newCols = append(newCols, col)
		newData[col] = df.data[col]
	}

	return DataFrame{index: newIdx, columns: newCols, data: newData}
}

// AsType casts specified columns to the given dtypes.
func (df DataFrame) AsType(dtypes map[string]dtype.DType) (DataFrame, error) {
	newData := make(map[string]*series.Series[any], len(df.data))
	for k, v := range df.data {
		newData[k] = v
	}

	for col, dt := range dtypes {
		s, ok := df.data[col]
		if !ok {
			return DataFrame{}, fmt.Errorf("dataframe.AsType: column %q not found", col)
		}
		converted, err := s.AsType(dt)
		if err != nil {
			return DataFrame{}, fmt.Errorf("dataframe.AsType: column %q: %w", col, err)
		}
		newData[col] = &converted
	}

	return DataFrame{index: df.index, columns: df.Columns(), data: newData}, nil
}

// FillNA returns a new DataFrame with null values filled with the given value.
// The fill value is converted to match each column's dtype when possible.
func (df DataFrame) FillNA(val any) DataFrame {
	newData := make(map[string]*series.Series[any], len(df.data))
	for _, col := range df.columns {
		s := df.data[col]
		n := s.Len()
		vals := make([]any, n)
		for i := 0; i < n; i++ {
			if s.IsNull(i) {
				vals[i] = convertFillValue(val, s.DType())
			} else {
				v, _ := s.At(i)
				vals[i] = v
			}
		}
		ns := series.New[any](memory.DefaultAllocator, vals, df.index, col)
		newData[col] = &ns
	}
	return DataFrame{index: df.index, columns: df.Columns(), data: newData}
}

// convertFillValue converts a fill value to match the target dtype.
func convertFillValue(val any, dt dtype.DType) any {
	switch dt {
	case dtype.Int64:
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case int:
			return int64(v)
		}
	case dtype.Float64:
		switch v := val.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case int:
			return float64(v)
		}
	case dtype.String:
		return fmt.Sprintf("%v", val)
	case dtype.Bool:
		if v, ok := val.(bool); ok {
			return v
		}
	}
	return val
}

// DropNA drops rows or columns with null values.
// axis=0: drop rows, axis=1: drop columns.
// how="any": drop if any null, how="all": drop only if all null.
func (df DataFrame) DropNA(axis int, how string) (DataFrame, error) {
	if axis != 0 && axis != 1 {
		return DataFrame{}, fmt.Errorf("dataframe.DropNA: axis must be 0 or 1, got %d", axis)
	}
	if how != "any" && how != "all" {
		return DataFrame{}, fmt.Errorf("dataframe.DropNA: how must be 'any' or 'all', got %q", how)
	}

	if axis == 1 {
		// Drop columns
		var newCols []string
		newData := make(map[string]*series.Series[any])
		for _, col := range df.columns {
			s := df.data[col]
			nullCount := s.NullCount()
			drop := false
			if how == "any" && nullCount > 0 {
				drop = true
			} else if how == "all" && nullCount == s.Len() {
				drop = true
			}
			if !drop {
				newCols = append(newCols, col)
				newData[col] = s
			}
		}
		if newCols == nil {
			newCols = []string{}
		}
		return DataFrame{index: df.index, columns: newCols, data: newData}, nil
	}

	// axis == 0: Drop rows
	nRows := df.Len()
	nCols := len(df.columns)
	var keepRows []int
	for i := 0; i < nRows; i++ {
		nullsInRow := 0
		for _, col := range df.columns {
			if df.data[col].IsNull(i) {
				nullsInRow++
			}
		}
		drop := false
		if how == "any" && nullsInRow > 0 {
			drop = true
		} else if how == "all" && nullsInRow == nCols {
			drop = true
		}
		if !drop {
			keepRows = append(keepRows, i)
		}
	}

	result, err := df.selectRowsByPositions(keepRows)
	if err != nil {
		return DataFrame{}, err
	}
	return result, nil
}
