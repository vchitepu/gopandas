package dataframe

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/series"
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

// containsAll checks that s contains all substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

func TestString(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	out := df.String()
	if !containsAll(out, "Alice", "Bob", "30", "25", "age", "name") {
		t.Errorf("String() missing expected content:\n%s", out)
	}
}

func TestString_Empty(t *testing.T) {
	df, err := New(map[string]any{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	out := df.String()
	if out != "Empty DataFrame" {
		t.Errorf("String() = %q, want %q", out, "Empty DataFrame")
	}
}

func TestFromRecords(t *testing.T) {
	records := []map[string]any{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob", "age": int64(25)},
		{"name": "Carol"}, // missing "age"
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("Shape() = (%d, %d), want (3, 2)", rows, cols)
	}
	// Columns should be sorted
	colNames := df.Columns()
	if colNames[0] != "age" || colNames[1] != "name" {
		t.Fatalf("Columns() = %v, want [age name]", colNames)
	}
}

func TestFromRecords_Empty(t *testing.T) {
	df, err := FromRecords(nil)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Fatalf("Shape() = (%d, %d), want (0, 0)", rows, cols)
	}
}

func TestFromArrow(t *testing.T) {
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "val", Type: arrow.PrimitiveTypes.Float64},
		}, nil,
	)
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{10.0, 20.0, 30.0}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()

	df, err := FromArrow(rec)
	if err != nil {
		t.Fatalf("FromArrow() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("Shape() = (%d, %d), want (3, 2)", rows, cols)
	}
	// Column order should match schema
	colNames := df.Columns()
	if colNames[0] != "id" || colNames[1] != "val" {
		t.Fatalf("Columns() = %v, want [id val]", colNames)
	}
}

func TestIntegration_Pipeline(t *testing.T) {
	// Step 1: New
	df, err := New(map[string]any{
		"name":   []string{"Alice", "Bob", "Carol", "Dave", "Eve"},
		"age":    []int64{30, 25, 35, 28, 22},
		"salary": []float64{80000, 60000, 90000, 70000, 55000},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if df.Len() != 5 {
		t.Fatalf("New().Len() = %d, want 5", df.Len())
	}

	// Step 2: Select
	df2, err := df.Select("name", "salary")
	if err != nil {
		t.Fatalf("Select() error: %v", err)
	}
	if len(df2.Columns()) != 2 {
		t.Fatalf("Select().Columns() len = %d, want 2", len(df2.Columns()))
	}

	// Step 3: Query
	df3, err := df.Query("age > 25")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if df3.Len() != 3 {
		t.Fatalf("Query().Len() = %d, want 3", df3.Len())
	}

	// Step 4: SortBy
	df4, err := df.SortBy([]string{"salary"}, []bool{false})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}
	topSalary, _ := df4.At(0, "salary")
	if topSalary != 90000.0 {
		t.Errorf("SortBy().At(0, salary) = %v, want 90000.0", topSalary)
	}

	// Step 5: Head
	df5 := df4.Head(3)
	if df5.Len() != 3 {
		t.Fatalf("Head(3).Len() = %d, want 3", df5.Len())
	}

	// Step 6: Mean
	mean := df.Mean()
	ageMean, _ := mean.Loc("age")
	if ageMean != 28.0 {
		t.Errorf("Mean().Loc(age) = %v, want 28.0", ageMean)
	}

	// Step 7: Rename
	df6 := df.Rename(map[string]string{"salary": "income"})
	cols6 := df6.Columns()
	hasIncome := false
	for _, c := range cols6 {
		if c == "income" {
			hasIncome = true
		}
	}
	if !hasIncome {
		t.Fatalf("Rename: Columns() = %v, missing 'income'", cols6)
	}

	// Step 8: WithColumn
	bonusVals := make([]any, df.Len())
	for i := 0; i < df.Len(); i++ {
		sal, _ := df.At(i, "salary")
		bonusVals[i] = sal.(float64) * 0.1
	}
	bonusS := series.New[any](memory.DefaultAllocator, bonusVals, df.Index(), "bonus")
	df7, err := df.WithColumn("bonus", &bonusS)
	if err != nil {
		t.Fatalf("WithColumn() error: %v", err)
	}
	if len(df7.Columns()) != 4 {
		t.Fatalf("WithColumn: Columns() len = %d, want 4", len(df7.Columns()))
	}

	// Step 9: Drop
	df8 := df7.Drop("bonus")
	if len(df8.Columns()) != 3 {
		t.Fatalf("Drop: Columns() len = %d, want 3", len(df8.Columns()))
	}

	// Step 10: String
	out := df.String()
	if !containsAll(out, "Alice", "Bob", "salary") {
		t.Errorf("String() missing expected content:\n%s", out)
	}
}
