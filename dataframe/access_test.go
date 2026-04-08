package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/index"
	"github.com/vchitepu/gopandas/series"
)

func testDF(t *testing.T) DataFrame {
	t.Helper()
	data := map[string]any{
		"a": []int64{10, 20, 30},
		"b": []float64{1.1, 2.2, 3.3},
	}
	df, err := New(data)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return df
}

// --- Task 6: Col ---

func TestCol(t *testing.T) {
	df := testDF(t)
	s, err := df.Col("a")
	if err != nil {
		t.Fatalf("Col() error: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Col(a).Len() = %d, want 3", s.Len())
	}
	if s.Name() != "a" {
		t.Errorf("Col(a).Name() = %q, want %q", s.Name(), "a")
	}
}

func TestCol_NotFound(t *testing.T) {
	df := testDF(t)
	_, err := df.Col("z")
	if err == nil {
		t.Fatal("Col(z) expected error for missing column")
	}
}

// --- Task 7: At ---

func TestAt(t *testing.T) {
	df := testDF(t)
	val, err := df.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(10) {
		t.Errorf("At(0, a) = %v, want 10", val)
	}
}

func TestAt_OutOfBounds(t *testing.T) {
	df := testDF(t)
	_, err := df.At(5, "a")
	if err == nil {
		t.Fatal("At(5, a) expected error for out of bounds")
	}
}

func TestAt_BadColumn(t *testing.T) {
	df := testDF(t)
	_, err := df.At(0, "z")
	if err == nil {
		t.Fatal("At(0, z) expected error for missing column")
	}
}

// --- Task 8: Loc ---

func TestLoc(t *testing.T) {
	// Create a DF with StringIndex
	idx := index.NewStringIndex([]string{"x", "y", "z"}, "key")
	aS := series.New[any](memory.DefaultAllocator, []any{int64(10), int64(20), int64(30)}, idx, "a")
	bS := series.New[any](memory.DefaultAllocator, []any{1.1, 2.2, 3.3}, idx, "b")
	df := DataFrame{
		index:   idx,
		columns: []string{"a", "b"},
		data:    map[string]*series.Series[any]{"a": &aS, "b": &bS},
	}

	val, err := df.Loc("y", "a")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	if val != int64(20) {
		t.Errorf("Loc(y, a) = %v, want 20", val)
	}
}

func TestLoc_LabelNotFound(t *testing.T) {
	df := testDF(t)
	_, err := df.Loc("missing", "a")
	if err == nil {
		t.Fatal("Loc(missing, a) expected error for missing label")
	}
}
