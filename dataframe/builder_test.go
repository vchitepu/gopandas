package dataframe

import (
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
