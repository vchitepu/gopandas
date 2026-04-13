package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// --- Task 17: WithColumn ---

func TestWithColumn_Add(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
	})
	newCol := series.New[any](memory.DefaultAllocator, []any{10.0, 20.0, 30.0}, index.NewRangeIndex(3, ""), "b")
	df2, err := df.WithColumn("b", &newCol)
	if err != nil {
		t.Fatalf("WithColumn() error: %v", err)
	}
	if len(df2.Columns()) != 2 {
		t.Fatalf("WithColumn().Columns() len = %d, want 2", len(df2.Columns()))
	}
	val, err := df2.At(0, "b")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != 10.0 {
		t.Errorf("At(0, b) = %v, want 10.0", val)
	}
}

func TestWithColumn_Replace(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{4.0, 5.0, 6.0},
	})
	newB := series.New[any](memory.DefaultAllocator, []any{int64(10), int64(20), int64(30)}, index.NewRangeIndex(3, ""), "b")
	df2, err := df.WithColumn("b", &newB)
	if err != nil {
		t.Fatalf("WithColumn() error: %v", err)
	}
	val, err := df2.At(0, "b")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(10) {
		t.Errorf("WithColumn replace At(0, b) = %v, want 10", val)
	}
}

// --- Task 18: Rename ---

func TestRename(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2},
		"b": []float64{3.0, 4.0},
	})
	df2 := df.Rename(map[string]string{"a": "x", "b": "y"})
	cols := df2.Columns()
	if cols[0] != "x" || cols[1] != "y" {
		t.Fatalf("Rename().Columns() = %v, want [x y]", cols)
	}
	val, err := df2.At(0, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Rename().At(0, x) = %v, want 1", val)
	}
}

// --- Task 19: SetIndex ---

func TestSetIndex(t *testing.T) {
	df, _ := New(map[string]any{
		"name": []string{"Alice", "Bob", "Carol"},
		"age":  []int64{30, 25, 35},
	})
	df2, err := df.SetIndex("name")
	if err != nil {
		t.Fatalf("SetIndex() error: %v", err)
	}
	// "name" should no longer be a column
	cols := df2.Columns()
	if len(cols) != 1 || cols[0] != "age" {
		t.Fatalf("SetIndex().Columns() = %v, want [age]", cols)
	}
	// Index should be a StringIndex
	idx := df2.Index()
	if idx.Name() != "name" {
		t.Errorf("SetIndex().Index().Name() = %q, want %q", idx.Name(), "name")
	}
	// Loc should work with string labels
	val, err := df2.Loc("Bob", "age")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	if val != int64(25) {
		t.Errorf("Loc(Bob, age) = %v, want 25", val)
	}
}

// --- Task 20: ResetIndex ---

func TestResetIndex_NoDrop(t *testing.T) {
	df, _ := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	df2, _ := df.SetIndex("name")
	df3 := df2.ResetIndex(false)
	cols := df3.Columns()
	// Should have "name" back as a column
	found := false
	for _, c := range cols {
		if c == "name" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ResetIndex(false).Columns() = %v, missing 'name'", cols)
	}
}

func TestResetIndex_Drop(t *testing.T) {
	df, _ := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	df2, _ := df.SetIndex("name")
	df3 := df2.ResetIndex(true)
	cols := df3.Columns()
	for _, c := range cols {
		if c == "name" {
			t.Fatalf("ResetIndex(true).Columns() should not contain 'name', got %v", cols)
		}
	}
}

// --- Task 21: AsType ---

func TestAsType(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{4.0, 5.0, 6.0},
	})
	df2, err := df.AsType(map[string]dtype.DType{"a": dtype.Float64})
	if err != nil {
		t.Fatalf("AsType() error: %v", err)
	}
	dtypes := df2.DTypes()
	if dtypes["a"] != dtype.Float64 {
		t.Errorf("AsType DTypes[a] = %v, want float64", dtypes["a"])
	}
}

// --- Task 22: FillNA ---

func TestFillNA(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": 10.0},
		{"a": nil, "b": 20.0},
		{"a": int64(3), "b": nil},
	}
	df, _ := FromRecords(records)
	df2 := df.FillNA(int64(0))
	val, err := df2.At(1, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	// FillNA should replace nil with 0
	if val != int64(0) {
		t.Errorf("FillNA().At(1, a) = %v, want 0", val)
	}
}

// --- Task 23: DropNA ---

func TestDropNA_Rows_Any(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": 10.0},
		{"a": nil, "b": 20.0},
		{"a": int64(3), "b": 30.0},
	}
	df, _ := FromRecords(records)
	df2, err := df.DropNA(0, "any")
	if err != nil {
		t.Fatalf("DropNA() error: %v", err)
	}
	if df2.Len() != 2 {
		t.Fatalf("DropNA(0, any).Len() = %d, want 2", df2.Len())
	}
}

func TestDropNA_Cols_Any(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": 10.0},
		{"a": nil, "b": 20.0},
		{"a": int64(3), "b": 30.0},
	}
	df, _ := FromRecords(records)
	df2, err := df.DropNA(1, "any")
	if err != nil {
		t.Fatalf("DropNA() error: %v", err)
	}
	// Column "a" has a null, so it should be dropped
	cols := df2.Columns()
	for _, c := range cols {
		if c == "a" {
			t.Fatalf("DropNA(1, any) should have dropped column 'a', got %v", cols)
		}
	}
}

func TestDropNA_Rows_All(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": 10.0},
		{"a": nil, "b": nil},
		{"a": int64(3), "b": 30.0},
	}
	df, _ := FromRecords(records)
	df2, err := df.DropNA(0, "all")
	if err != nil {
		t.Fatalf("DropNA() error: %v", err)
	}
	if df2.Len() != 2 {
		t.Fatalf("DropNA(0, all).Len() = %d, want 2", df2.Len())
	}
}

func TestDropNA_Cols_All(t *testing.T) {
	// Column "a" has one null (not all), column "b" has all nulls.
	// DropNA(1, "all") should only drop "b".
	records := []map[string]any{
		{"a": int64(1), "b": nil},
		{"a": nil, "b": nil},
		{"a": int64(3), "b": nil},
	}
	df, _ := FromRecords(records)
	df2, err := df.DropNA(1, "all")
	if err != nil {
		t.Fatalf("DropNA() error: %v", err)
	}
	cols := df2.Columns()
	// "a" should be kept (not all null), "b" should be dropped (all null)
	if len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("DropNA(1, all).Columns() = %v, want [a]", cols)
	}
}

func TestRename_Partial(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2},
		"b": []float64{3.0, 4.0},
		"c": []string{"x", "y"},
	})
	// Rename only "a" -> "alpha", leave "b" and "c" unchanged
	df2 := df.Rename(map[string]string{"a": "alpha"})
	cols := df2.Columns()
	if cols[0] != "alpha" {
		t.Errorf("Rename partial: cols[0] = %q, want 'alpha'", cols[0])
	}
	if cols[1] != "b" {
		t.Errorf("Rename partial: cols[1] = %q, want 'b'", cols[1])
	}
	if cols[2] != "c" {
		t.Errorf("Rename partial: cols[2] = %q, want 'c'", cols[2])
	}
	// Verify data accessible via new name
	val, err := df2.At(0, "alpha")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Rename partial: At(0, alpha) = %v, want 1", val)
	}
}

func TestWithColumn_LengthMismatch(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
	})
	// Create a series with different length
	shortCol := series.New[any](memory.DefaultAllocator, []any{10.0, 20.0}, index.NewRangeIndex(2, ""), "b")
	_, err := df.WithColumn("b", &shortCol)
	if err == nil {
		t.Fatal("WithColumn() expected error for length mismatch")
	}
}
