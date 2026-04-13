package dataframe

import (
	"fmt"

	"github.com/vchitepu/gopandas/lib/series"
)

// Col returns the named column as a *Series[any].
func (df DataFrame) Col(name string) (*series.Series[any], error) {
	s, ok := df.data[name]
	if !ok {
		return nil, fmt.Errorf("dataframe.Col: column %q not found", name)
	}
	return s, nil
}

// At returns the value at the given positional row and column name.
func (df DataFrame) At(row int, col string) (any, error) {
	s, ok := df.data[col]
	if !ok {
		return nil, fmt.Errorf("dataframe.At: column %q not found", col)
	}
	if row < 0 || row >= df.Len() {
		return nil, fmt.Errorf("dataframe.At: row %d out of bounds [0, %d)", row, df.Len())
	}
	val, _ := s.At(row)
	return val, nil
}

// Loc returns the value at the given index label and column name.
func (df DataFrame) Loc(label any, col string) (any, error) {
	s, ok := df.data[col]
	if !ok {
		return nil, fmt.Errorf("dataframe.Loc: column %q not found", col)
	}
	pos, found := df.index.Loc(label)
	if !found {
		return nil, fmt.Errorf("dataframe.Loc: label %v not found in index", label)
	}
	val, _ := s.At(pos)
	return val, nil
}
