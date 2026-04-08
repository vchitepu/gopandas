package dataframe

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
)

func selectTestDF(t *testing.T) DataFrame {
	t.Helper()
	data := map[string]any{
		"a": []int64{1, 2, 3, 4, 5},
		"b": []float64{10.0, 20.0, 30.0, 40.0, 50.0},
		"c": []string{"x", "y", "z", "w", "v"},
	}
	df, err := New(data)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return df
}

// --- Task 9: Head and Tail ---

func TestHead(t *testing.T) {
	df := selectTestDF(t)
	h := df.Head(2)
	if h.Len() != 2 {
		t.Fatalf("Head(2).Len() = %d, want 2", h.Len())
	}
	val, err := h.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Head(2).At(0, a) = %v, want 1", val)
	}
}

func TestHead_MoreThanLen(t *testing.T) {
	df := selectTestDF(t)
	h := df.Head(100)
	if h.Len() != 5 {
		t.Fatalf("Head(100).Len() = %d, want 5", h.Len())
	}
}

func TestTail(t *testing.T) {
	df := selectTestDF(t)
	tl := df.Tail(2)
	if tl.Len() != 2 {
		t.Fatalf("Tail(2).Len() = %d, want 2", tl.Len())
	}
	val, err := tl.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(4) {
		t.Errorf("Tail(2).At(0, a) = %v, want 4", val)
	}
}

// --- Task 10: ILoc ---

func TestILoc(t *testing.T) {
	df := selectTestDF(t)
	sub, err := df.ILoc(1, 3, 0, 2) // rows [1,3), cols [0,2) -> cols a, b
	if err != nil {
		t.Fatalf("ILoc() error: %v", err)
	}
	rows, cols := sub.Shape()
	if rows != 2 || cols != 2 {
		t.Fatalf("ILoc Shape() = (%d, %d), want (2, 2)", rows, cols)
	}
	val, err := sub.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(2) {
		t.Errorf("ILoc.At(0, a) = %v, want 2", val)
	}
}

func TestILoc_OutOfBounds(t *testing.T) {
	df := selectTestDF(t)
	_, err := df.ILoc(-1, 3, 0, 2)
	if err == nil {
		t.Fatal("ILoc() expected error for negative start")
	}
}

// --- Task 11: LocRows ---

func TestLocRows(t *testing.T) {
	// Build DF with string index
	idx := index.NewStringIndex([]string{"x", "y", "z"}, "key")
	aS := series.New[any](memory.DefaultAllocator, []any{int64(10), int64(20), int64(30)}, idx, "a")
	bS := series.New[any](memory.DefaultAllocator, []any{1.1, 2.2, 3.3}, idx, "b")
	df := DataFrame{
		index:   idx,
		columns: []string{"a", "b"},
		data:    map[string]*series.Series[any]{"a": &aS, "b": &bS},
	}

	sub, err := df.LocRows([]any{"z", "x"})
	if err != nil {
		t.Fatalf("LocRows() error: %v", err)
	}
	if sub.Len() != 2 {
		t.Fatalf("LocRows().Len() = %d, want 2", sub.Len())
	}
	val, err := sub.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(30) {
		t.Errorf("LocRows first row a = %v, want 30", val)
	}
}

func TestLocRows_LabelNotFound(t *testing.T) {
	df := selectTestDF(t)
	_, err := df.LocRows([]any{"missing"})
	if err == nil {
		t.Fatal("LocRows() expected error for missing label")
	}
}

// --- Task 12: Select ---

func TestSelect(t *testing.T) {
	df := selectTestDF(t)
	sub, err := df.Select("c", "a")
	if err != nil {
		t.Fatalf("Select() error: %v", err)
	}
	cols := sub.Columns()
	if len(cols) != 2 || cols[0] != "c" || cols[1] != "a" {
		t.Fatalf("Select().Columns() = %v, want [c a]", cols)
	}
}

func TestSelect_NotFound(t *testing.T) {
	df := selectTestDF(t)
	_, err := df.Select("a", "z")
	if err == nil {
		t.Fatal("Select() expected error for missing column")
	}
}

// --- Task 13: Drop ---

func TestDrop(t *testing.T) {
	df := selectTestDF(t)
	sub := df.Drop("b", "nonexistent")
	cols := sub.Columns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "c" {
		t.Fatalf("Drop().Columns() = %v, want [a c]", cols)
	}
}

// --- Task 14: Sample ---

func TestSample(t *testing.T) {
	df := selectTestDF(t)
	s, err := df.Sample(3, 42)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}
	if s.Len() != 3 {
		t.Fatalf("Sample(3).Len() = %d, want 3", s.Len())
	}
}

func TestSample_TooMany(t *testing.T) {
	df := selectTestDF(t)
	_, err := df.Sample(100, 42)
	if err == nil {
		t.Fatal("Sample(100) expected error for n > Len()")
	}
}

func TestSample_Deterministic(t *testing.T) {
	df := selectTestDF(t)
	seed := int64(123)
	s1, err := df.Sample(3, seed)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}
	s2, err := df.Sample(3, seed)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}
	// Same seed should produce the same results
	for i := 0; i < s1.Len(); i++ {
		for _, col := range s1.Columns() {
			v1, _ := s1.At(i, col)
			v2, _ := s2.At(i, col)
			if fmt.Sprintf("%v", v1) != fmt.Sprintf("%v", v2) {
				t.Errorf("Sample deterministic: row %d col %s: %v != %v", i, col, v1, v2)
			}
		}
	}
}

// Suppress unused import warnings
var _ = memory.DefaultAllocator
