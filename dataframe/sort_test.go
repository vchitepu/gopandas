package dataframe

import (
	"testing"
)

func TestSortBy_SingleCol_Asc(t *testing.T) {
	df, _ := New(map[string]any{
		"name": []string{"Carol", "Alice", "Bob"},
		"age":  []int64{35, 30, 25},
	})
	sorted, err := df.SortBy([]string{"age"}, []bool{true})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}
	val, _ := sorted.At(0, "name")
	if val != "Bob" {
		t.Errorf("SortBy asc: first name = %v, want Bob", val)
	}
	val2, _ := sorted.At(2, "name")
	if val2 != "Carol" {
		t.Errorf("SortBy asc: last name = %v, want Carol", val2)
	}
}

func TestSortBy_SingleCol_Desc(t *testing.T) {
	df, _ := New(map[string]any{
		"x": []float64{1.0, 3.0, 2.0},
	})
	sorted, err := df.SortBy([]string{"x"}, []bool{false})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}
	val, _ := sorted.At(0, "x")
	if val != 3.0 {
		t.Errorf("SortBy desc: first x = %v, want 3.0", val)
	}
}

func TestSortBy_MultiCol(t *testing.T) {
	df, _ := New(map[string]any{
		"group": []string{"a", "b", "a", "b"},
		"val":   []int64{2, 1, 1, 2},
	})
	sorted, err := df.SortBy([]string{"group", "val"}, []bool{true, true})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}
	// Should be: a/1, a/2, b/1, b/2
	v0, _ := sorted.At(0, "val")
	v1, _ := sorted.At(1, "val")
	if v0 != int64(1) || v1 != int64(2) {
		t.Errorf("SortBy multi: got vals %v, %v, want 1, 2", v0, v1)
	}
}

func TestSortBy_ColNotFound(t *testing.T) {
	df, _ := New(map[string]any{"a": []int64{1}})
	_, err := df.SortBy([]string{"z"}, []bool{true})
	if err == nil {
		t.Fatal("SortBy() expected error for missing column")
	}
}
