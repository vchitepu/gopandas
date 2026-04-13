package dataframe

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/arrowutil"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// --- Task 15: Filter ---

func TestFilter(t *testing.T) {
	data := map[string]any{
		"a": []int64{1, 2, 3, 4},
		"b": []string{"x", "y", "z", "w"},
	}
	df, err := New(data)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	mask := series.New[bool](memory.DefaultAllocator, []bool{true, false, true, false}, index.NewRangeIndex(4, ""), "mask")
	result, err := df.Filter(mask)
	if err != nil {
		t.Fatalf("Filter() error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Filter().Len() = %d, want 2", result.Len())
	}
	val, err := result.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Filter().At(0, a) = %v, want 1", val)
	}
	val2, err := result.At(1, "b")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val2 != "z" {
		t.Errorf("Filter().At(1, b) = %v, want z", val2)
	}
}

func TestFilter_LengthMismatch(t *testing.T) {
	df, _ := New(map[string]any{"a": []int64{1, 2}})
	mask := series.New[bool](memory.DefaultAllocator, []bool{true}, index.NewRangeIndex(1, ""), "mask")
	_, err := df.Filter(mask)
	if err == nil {
		t.Fatal("Filter() expected error for length mismatch")
	}
}

// --- Task 16: Query ---

func TestQuery_GreaterThan(t *testing.T) {
	df, _ := New(map[string]any{
		"age":  []int64{10, 20, 30, 40},
		"name": []string{"a", "b", "c", "d"},
	})
	result, err := df.Query("age > 20")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Query().Len() = %d, want 2", result.Len())
	}
}

func TestQuery_EqualString(t *testing.T) {
	df, _ := New(map[string]any{
		"name": []string{"Alice", "Bob", "Carol"},
		"val":  []int64{1, 2, 3},
	})
	result, err := df.Query("name == 'Bob'")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 1 {
		t.Fatalf("Query().Len() = %d, want 1", result.Len())
	}
	v, _ := result.At(0, "val")
	if v != int64(2) {
		t.Errorf("Query().At(0, val) = %v, want 2", v)
	}
}

func TestQuery_LessThanEqual(t *testing.T) {
	df, _ := New(map[string]any{
		"x": []float64{1.0, 2.0, 3.0, 4.0},
	})
	result, err := df.Query("x <= 2.5")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Query().Len() = %d, want 2", result.Len())
	}
}

func TestQuery_NotEqual(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
	})
	result, err := df.Query("a != 2")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Query().Len() = %d, want 2", result.Len())
	}
}

func TestQuery_GreaterThanOrEqual(t *testing.T) {
	df, _ := New(map[string]any{
		"x": []int64{10, 20, 30, 40},
	})
	result, err := df.Query("x >= 20")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 3 {
		t.Fatalf("Query(x >= 20).Len() = %d, want 3", result.Len())
	}
	v0, _ := result.At(0, "x")
	if v0 != int64(20) {
		t.Errorf("Query(x >= 20).At(0, x) = %v, want 20", v0)
	}
}

func TestQuery_WithTimestampColumnDoesNotPanic(t *testing.T) {
	alloc := memory.DefaultAllocator

	dateVals := []time.Time{
		time.Date(2025, 12, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	dateArr := arrowutil.BuildTimestampArray(alloc, dateVals)
	defer dateArr.Release()

	amountArr := arrowutil.BuildInt64Array(alloc, []int64{10, 20})
	defer amountArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Amount", Type: amountArr.DataType()},
		{Name: "Date", Type: dateArr.DataType()},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{amountArr, dateArr}, 2)
	defer rec.Release()

	df, err := FromArrow(rec)
	if err != nil {
		t.Fatalf("FromArrow() error: %v", err)
	}

	result, err := df.Query("Amount > 0")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}

	if result.Len() != 2 {
		t.Fatalf("Query().Len() = %d, want 2", result.Len())
	}

	if got := result.DTypes()["Date"]; got.String() != "timestamp" {
		t.Fatalf("Date dtype = %s, want timestamp", got)
	}
}

func TestQuery_TimestampLiteralComparison(t *testing.T) {
	alloc := memory.DefaultAllocator

	dateVals := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	dateArr := arrowutil.BuildTimestampArray(alloc, dateVals)
	defer dateArr.Release()

	amountArr := arrowutil.BuildInt64Array(alloc, []int64{10, 20})
	defer amountArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Amount", Type: amountArr.DataType()},
		{Name: "Date", Type: dateArr.DataType()},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{amountArr, dateArr}, 2)
	defer rec.Release()

	df, err := FromArrow(rec)
	if err != nil {
		t.Fatalf("FromArrow() error: %v", err)
	}

	result, err := df.Query("Date > '06/12/2025'")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}

	if result.Len() != 1 {
		t.Fatalf("Query().Len() = %d, want 1", result.Len())
	}

	gotDate, err := result.At(0, "Date")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if gotDate != time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("Date = %v, want 2025-12-31", gotDate)
	}
}
