package dataframe

import "testing"

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

func TestBuilder_ErrorShortCircuit(t *testing.T) {
	df := builderTestDF(t)

	got, err := df.Build().
		Select("does_not_exist").
		Query("a > 1").
		SortBy([]string{"a"}, []bool{true}).
		Head(1).
		Result()
	if err == nil {
		t.Fatal("builder Result() error = nil, want non-nil")
	}

	if got.String() != df.String() {
		t.Fatalf("error short-circuit mutated dataframe:\ngot:\n%s\nwant:\n%s", got.String(), df.String())
	}
}
