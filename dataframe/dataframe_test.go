package dataframe

import (
	"testing"
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
