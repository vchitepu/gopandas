package dataframe

import (
	"fmt"
	"math/rand"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/index"
	"github.com/vchitepu/gopandas/series"
)

// sliceRows returns a new DataFrame with rows [start, end).
func (df DataFrame) sliceRows(start, end int) DataFrame {
	if start < 0 {
		start = 0
	}
	if end > df.Len() {
		end = df.Len()
	}
	if start >= end {
		newIdx := index.NewRangeIndex(0, "")
		newData := make(map[string]*series.Series[any], len(df.columns))
		for _, col := range df.columns {
			s := df.data[col].ILoc(0, 0)
			newData[col] = &s
		}
		return DataFrame{index: newIdx, columns: df.Columns(), data: newData}
	}

	newIdx := df.index.Slice(start, end)
	newData := make(map[string]*series.Series[any], len(df.columns))
	for _, col := range df.columns {
		s := df.data[col].ILoc(start, end)
		newData[col] = &s
	}
	return DataFrame{index: newIdx, columns: df.Columns(), data: newData}
}

// Head returns a new DataFrame with the first n rows.
func (df DataFrame) Head(n int) DataFrame {
	if n > df.Len() {
		n = df.Len()
	}
	if n <= 0 {
		return df.sliceRows(0, 0)
	}
	return df.sliceRows(0, n)
}

// Tail returns a new DataFrame with the last n rows.
func (df DataFrame) Tail(n int) DataFrame {
	if n > df.Len() {
		n = df.Len()
	}
	if n <= 0 {
		return df.sliceRows(0, 0)
	}
	return df.sliceRows(df.Len()-n, df.Len())
}

// ILoc returns a positional slice of the DataFrame.
// rowStart and rowEnd select rows [rowStart, rowEnd).
// colStart and colEnd select columns [colStart, colEnd) by position.
func (df DataFrame) ILoc(rowStart, rowEnd, colStart, colEnd int) (DataFrame, error) {
	if rowStart < 0 || rowEnd < 0 || colStart < 0 || colEnd < 0 {
		return DataFrame{}, fmt.Errorf("dataframe.ILoc: negative index")
	}
	if rowStart > df.Len() || rowEnd > df.Len() {
		return DataFrame{}, fmt.Errorf("dataframe.ILoc: row index out of bounds")
	}
	if colStart > len(df.columns) || colEnd > len(df.columns) {
		return DataFrame{}, fmt.Errorf("dataframe.ILoc: column index out of bounds")
	}
	if rowStart > rowEnd || colStart > colEnd {
		return DataFrame{}, fmt.Errorf("dataframe.ILoc: start > end")
	}

	selectedCols := df.columns[colStart:colEnd]
	newIdx := df.index.Slice(rowStart, rowEnd)
	newData := make(map[string]*series.Series[any], len(selectedCols))
	for _, col := range selectedCols {
		s := df.data[col].ILoc(rowStart, rowEnd)
		newData[col] = &s
	}
	cols := make([]string, len(selectedCols))
	copy(cols, selectedCols)
	return DataFrame{index: newIdx, columns: cols, data: newData}, nil
}

// LocRows returns a new DataFrame with rows matching the given labels.
func (df DataFrame) LocRows(labels []any) (DataFrame, error) {
	positions := make([]int, len(labels))
	for i, label := range labels {
		pos, ok := df.index.Loc(label)
		if !ok {
			return DataFrame{}, fmt.Errorf("dataframe.LocRows: label %v not found", label)
		}
		positions[i] = pos
	}
	return df.selectRowsByPositions(positions)
}

// selectRowsByPositions creates a new DataFrame by picking rows at given positions.
// This is an internal method that assumes all positions are valid indices in [0, df.Len()).
func (df DataFrame) selectRowsByPositions(positions []int) (DataFrame, error) {
	nRows := len(positions)
	newIdx := index.NewRangeIndex(nRows, "")
	newData := make(map[string]*series.Series[any], len(df.columns))

	for _, col := range df.columns {
		src := df.data[col]
		vals := make([]any, nRows)
		for i, pos := range positions {
			val, _ := src.At(pos)
			vals[i] = val
		}
		s := series.New[any](memory.DefaultAllocator, vals, newIdx, col)
		newData[col] = &s
	}
	return DataFrame{index: newIdx, columns: df.Columns(), data: newData}, nil
}

// Select returns a new DataFrame with only the specified columns, in the given order.
func (df DataFrame) Select(cols ...string) (DataFrame, error) {
	for _, col := range cols {
		if _, ok := df.data[col]; !ok {
			return DataFrame{}, fmt.Errorf("dataframe.Select: column %q not found", col)
		}
	}
	newData := make(map[string]*series.Series[any], len(cols))
	for _, col := range cols {
		newData[col] = df.data[col]
	}
	colsCopy := make([]string, len(cols))
	copy(colsCopy, cols)
	return DataFrame{index: df.index, columns: colsCopy, data: newData}, nil
}

// Drop returns a new DataFrame without the specified columns.
// Non-existent columns are silently ignored.
func (df DataFrame) Drop(cols ...string) DataFrame {
	dropSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		dropSet[c] = true
	}
	var newCols []string
	newData := make(map[string]*series.Series[any])
	for _, col := range df.columns {
		if !dropSet[col] {
			newCols = append(newCols, col)
			newData[col] = df.data[col]
		}
	}
	if newCols == nil {
		newCols = []string{}
	}
	return DataFrame{index: df.index, columns: newCols, data: newData}
}

// Sample returns a random sample of n rows using Fisher-Yates shuffle.
func (df DataFrame) Sample(n int, seed int64) (DataFrame, error) {
	if n > df.Len() {
		return DataFrame{}, fmt.Errorf("dataframe.Sample: n=%d > Len()=%d", n, df.Len())
	}
	if n <= 0 {
		return df.sliceRows(0, 0), nil
	}

	rng := rand.New(rand.NewSource(seed))
	indices := make([]int, df.Len())
	for i := range indices {
		indices[i] = i
	}
	// Fisher-Yates shuffle
	for i := len(indices) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}
	return df.selectRowsByPositions(indices[:n])
}
