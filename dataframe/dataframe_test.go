package dataframe

import (
	"testing"

	"github.com/vinaychitepu/gopandas/dtype"
)

func TestNew(t *testing.T) {
	data := map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{4.0, 5.0, 6.0},
	}
	df, err := New(data)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("Shape() = (%d, %d), want (3, 2)", rows, cols)
	}
	// Columns should be sorted
	colNames := df.Columns()
	if colNames[0] != "a" || colNames[1] != "b" {
		t.Fatalf("Columns() = %v, want [a b]", colNames)
	}
}

func TestNew_EmptyMap(t *testing.T) {
	df, err := New(map[string]any{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Fatalf("Shape() = (%d, %d), want (0, 0)", rows, cols)
	}
}

func TestNew_MismatchedLengths(t *testing.T) {
	data := map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{4.0, 5.0},
	}
	_, err := New(data)
	if err == nil {
		t.Fatal("New() expected error for mismatched lengths")
	}
}

func TestMetadata(t *testing.T) {
	data := map[string]any{
		"x": []int64{10, 20, 30},
		"y": []float64{1.1, 2.2, 3.3},
	}
	df, err := New(data)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Shape
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Errorf("Shape() = (%d, %d), want (3, 2)", rows, cols)
	}

	// Len
	if df.Len() != 3 {
		t.Errorf("Len() = %d, want 3", df.Len())
	}

	// Columns
	colNames := df.Columns()
	if len(colNames) != 2 || colNames[0] != "x" || colNames[1] != "y" {
		t.Errorf("Columns() = %v, want [x y]", colNames)
	}

	// DTypes
	dtypes := df.DTypes()
	if dtypes["x"] != dtype.Int64 {
		t.Errorf("DTypes()[x] = %v, want int64", dtypes["x"])
	}
	if dtypes["y"] != dtype.Float64 {
		t.Errorf("DTypes()[y] = %v, want float64", dtypes["y"])
	}

	// Index
	idx := df.Index()
	if idx.Len() != 3 {
		t.Errorf("Index().Len() = %d, want 3", idx.Len())
	}
}
