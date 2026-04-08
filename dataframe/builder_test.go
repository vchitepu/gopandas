package dataframe

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/index"
	"github.com/vchitepu/gopandas/series"
)

func builderTestDF(t *testing.T) DataFrame {
	t.Helper()

	df, err := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	return df
}

func TestBuilder_NoOp(t *testing.T) {
	df := builderTestDF(t)

	got, err := df.Build().Result()
	if err != nil {
		t.Fatalf("Build().Result() error: %v", err)
	}

	if got.String() != df.String() {
		t.Fatalf("Build().Result() changed dataframe:\ngot:\n%s\nwant:\n%s", got.String(), df.String())
	}
}

func TestBuilder_HappyPath_SelectQuerySortByHead(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{3, 1, 2, 4},
		"b": []string{"x", "y", "z", "w"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	got, err := df.Build().
		Select("a", "b").
		Query("a >= 2").
		SortBy([]string{"a"}, []bool{true}).
		Head(2).
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	if got.Len() != 2 {
		t.Fatalf("got.Len() = %d, want 2", got.Len())
	}

	cols := got.Columns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Fatalf("got.Columns() = %v, want [a b]", cols)
	}

	v0, err := got.At(0, "a")
	if err != nil {
		t.Fatalf("got.At(0, a) error: %v", err)
	}
	if v0 != int64(2) {
		t.Fatalf("got.At(0, a) = %v, want 2", v0)
	}

	v1, err := got.At(1, "a")
	if err != nil {
		t.Fatalf("got.At(1, a) error: %v", err)
	}
	if v1 != int64(3) {
		t.Fatalf("got.At(1, a) = %v, want 3", v1)
	}
}

func TestBuilder_HappyPath_FilterTailChain(t *testing.T) {
	df := builderTestDF(t)

	mask := series.New[bool](
		memory.DefaultAllocator,
		[]bool{true, false, true},
		index.NewRangeIndex(df.Len(), ""),
		"mask",
	)

	got, err := df.Build().
		Filter(mask).
		Tail(1).
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	if got.Len() != 1 {
		t.Fatalf("got.Len() = %d, want 1", got.Len())
	}

	v, err := got.At(0, "a")
	if err != nil {
		t.Fatalf("got.At(0, a) error: %v", err)
	}
	if v != int64(3) {
		t.Fatalf("got.At(0, a) = %v, want 3", v)
	}
}

func TestBuilder_ErrorPath_FilterShortCircuit(t *testing.T) {
	df := builderTestDF(t)

	badMask := series.New[bool](
		memory.DefaultAllocator,
		[]bool{true, false},
		index.NewRangeIndex(2, ""),
		"mask",
	)

	got, err := df.Build().
		Filter(badMask).
		Tail(1).
		Result()
	if err == nil {
		t.Fatal("builder Result() error = nil, want non-nil")
	}

	if err.Error() != "dataframe.Filter: mask length 2 != DataFrame length 3" {
		t.Fatalf("builder Result() error = %q, want %q", err.Error(), "dataframe.Filter: mask length 2 != DataFrame length 3")
	}

	if got.String() != df.String() {
		t.Fatalf("error short-circuit mutated dataframe:\ngot:\n%s\nwant:\n%s", got.String(), df.String())
	}
}

func TestBuilder_ErrorShortCircuit(t *testing.T) {
	df := builderTestDF(t)

	_, wantErr := df.Select("does_not_exist")
	if wantErr == nil {
		t.Fatal("df.Select(does_not_exist) error = nil, want non-nil")
	}

	got, err := df.Build().
		Select("does_not_exist").
		Query("a >>> 1").
		SortBy([]string{"a"}, []bool{}).
		Head(1).
		Result()
	if err == nil {
		t.Fatal("builder Result() error = nil, want non-nil")
	}

	if err.Error() != wantErr.Error() {
		t.Fatalf("builder Result() error = %q, want first error %q", err.Error(), wantErr.Error())
	}

	if got.String() != df.String() {
		t.Fatalf("error short-circuit mutated dataframe:\ngot:\n%s\nwant:\n%s", got.String(), df.String())
	}
}

func TestBuilder_ILoc(t *testing.T) {
	df := builderTestDF(t)

	got, err := df.Build().
		ILoc(1, 3, 0, 1).
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	if got.Len() != 2 {
		t.Fatalf("got.Len() = %d, want 2", got.Len())
	}

	cols := got.Columns()
	if len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("got.Columns() = %v, want [a]", cols)
	}

	v0, err := got.At(0, "a")
	if err != nil {
		t.Fatalf("got.At(0, a) error: %v", err)
	}
	if v0 != int64(2) {
		t.Fatalf("got.At(0, a) = %v, want 2", v0)
	}
}

func TestBuilder_SetIndex(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Carol"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	got, err := df.Build().
		SetIndex("name").
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	cols := got.Columns()
	if len(cols) != 1 || cols[0] != "age" {
		t.Fatalf("got.Columns() = %v, want [age]", cols)
	}

	v, err := got.Loc("Bob", "age")
	if err != nil {
		t.Fatalf("got.Loc(Bob, age) error: %v", err)
	}
	if v != int64(25) {
		t.Fatalf("got.Loc(Bob, age) = %v, want 25", v)
	}
}

func TestBuilder_PlainMethods(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": nil, "b": "y"},
		{"a": int64(3), "b": "z"},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	got, err := df.Build().
		Drop("b").
		Rename(map[string]string{"a": "x"}).
		FillNA(int64(0)).
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	cols := got.Columns()
	if len(cols) != 1 || cols[0] != "x" {
		t.Fatalf("got.Columns() = %v, want [x]", cols)
	}

	v, err := got.At(1, "x")
	if err != nil {
		t.Fatalf("got.At(1, x) error: %v", err)
	}
	if v != int64(0) {
		t.Fatalf("got.At(1, x) = %v, want 0", v)
	}
}

func TestBuilder_ResetIndex(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	withIndex, err := df.Build().
		SetIndex("name").
		ResetIndex(false).
		Result()
	if err != nil {
		t.Fatalf("builder ResetIndex(false) error: %v", err)
	}

	cols := withIndex.Columns()
	if len(cols) != 2 || cols[0] != "name" || cols[1] != "age" {
		t.Fatalf("ResetIndex(false) got.Columns() = %v, want [name age]", cols)
	}

	v, err := withIndex.At(0, "name")
	if err != nil {
		t.Fatalf("withIndex.At(0, name) error: %v", err)
	}
	if v != "Alice" {
		t.Fatalf("withIndex.At(0, name) = %v, want Alice", v)
	}

	droppedIndex, err := df.Build().
		SetIndex("name").
		ResetIndex(true).
		Result()
	if err != nil {
		t.Fatalf("builder ResetIndex(true) error: %v", err)
	}

	droppedCols := droppedIndex.Columns()
	if len(droppedCols) != 1 || droppedCols[0] != "age" {
		t.Fatalf("ResetIndex(true) got.Columns() = %v, want [age]", droppedCols)
	}
}

func TestBuilder_Describe(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []int64{10, 20, 30, 40, 50},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	got, err := df.Build().
		Describe().
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	rows, cols := got.Shape()
	if rows != 5 {
		t.Fatalf("Describe().Shape() rows = %d, want 5", rows)
	}
	if cols != 2 {
		t.Fatalf("Describe().Shape() cols = %d, want 2", cols)
	}

	v, err := got.Loc("count", "a")
	if err != nil {
		t.Fatalf("Describe().Loc(count, a) error: %v", err)
	}
	if v != 5.0 {
		t.Fatalf("Describe().Loc(count, a) = %v, want 5.0", v)
	}
}

func TestBuilder_Corr(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{2.0, 4.0, 6.0, 8.0, 10.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	got, err := df.Build().
		Corr().
		Result()
	if err != nil {
		t.Fatalf("builder Result() error: %v", err)
	}

	aa, err := got.Loc("a", "a")
	if err != nil {
		t.Fatalf("Corr().Loc(a, a) error: %v", err)
	}
	aaFloat, ok := aa.(float64)
	if !ok {
		t.Fatalf("Corr().Loc(a, a) type = %T, want float64", aa)
	}
	if math.Abs(aaFloat-1.0) > 0.001 {
		t.Fatalf("Corr().Loc(a, a) = %v, want 1.0", aa)
	}

	ab, err := got.Loc("a", "b")
	if err != nil {
		t.Fatalf("Corr().Loc(a, b) error: %v", err)
	}
	abFloat, ok := ab.(float64)
	if !ok {
		t.Fatalf("Corr().Loc(a, b) type = %T, want float64", ab)
	}
	if math.Abs(abFloat-1.0) > 0.001 {
		t.Fatalf("Corr().Loc(a, b) = %v, want 1.0", ab)
	}
}
