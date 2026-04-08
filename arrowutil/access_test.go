package arrowutil_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/arrowutil"
)

func TestArrayLen(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{10, 20, 30})
	defer arr.Release()
	if got := arrowutil.ArrayLen(arr); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

func TestArrayLen_Empty(t *testing.T) {
	arr := arrowutil.BuildStringArray(memory.DefaultAllocator, []string{})
	defer arr.Release()
	if got := arrowutil.ArrayLen(arr); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestNullCount_NoNulls(t *testing.T) {
	arr := arrowutil.BuildFloat64Array(memory.DefaultAllocator, []float64{1.0, 2.0})
	defer arr.Release()
	if got := arrowutil.NullCount(arr); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestNullCount_WithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	bldr.Append(1)
	bldr.AppendNull()
	bldr.Append(3)
	bldr.AppendNull()
	arr := bldr.NewInt64Array()
	defer arr.Release()
	if got := arrowutil.NullCount(arr); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
}

func TestIsNull(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	arr := bldr.NewInt64Array()
	defer arr.Release()
	tests := []struct {
		idx  int
		want bool
	}{{0, false}, {1, true}, {2, false}}
	for _, tt := range tests {
		if got := arrowutil.IsNull(arr, tt.idx); got != tt.want {
			t.Errorf("IsNull(%d): expected %v, got %v", tt.idx, tt.want, got)
		}
	}
}

func TestGetValue_Int64(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{100, 200, 300})
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(200) {
		t.Errorf("expected 200, got %v", got)
	}
}

func TestGetValue_Float64(t *testing.T) {
	arr := arrowutil.BuildFloat64Array(memory.DefaultAllocator, []float64{1.5, 2.5})
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != float64(1.5) {
		t.Errorf("expected 1.5, got %v", got)
	}
}

func TestGetValue_String(t *testing.T) {
	arr := arrowutil.BuildStringArray(memory.DefaultAllocator, []string{"a", "b", "c"})
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got != "c" {
		t.Errorf("expected \"c\", got %v", got)
	}
}

func TestGetValue_Bool(t *testing.T) {
	arr := arrowutil.BuildBoolArray(memory.DefaultAllocator, []bool{true, false})
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != false {
		t.Errorf("expected false, got %v", got)
	}
}

func TestGetValue_Timestamp(t *testing.T) {
	ts := time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC)
	arr := arrowutil.BuildTimestampArray(memory.DefaultAllocator, []time.Time{ts})
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotTime, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", got)
	}
	if !gotTime.Equal(ts) {
		t.Errorf("expected %v, got %v", ts, gotTime)
	}
}

func TestGetValue_Null(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	bldr.AppendNull()
	arr := bldr.NewInt64Array()
	defer arr.Release()
	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestGetValue_OutOfBounds(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{1})
	defer arr.Release()
	_, err := arrowutil.GetValue(arr, 5)
	if err == nil {
		t.Error("expected error for out-of-bounds")
	}
}

func TestSliceArray_Int64(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{10, 20, 30, 40, 50})
	defer arr.Release()
	sliced := arrowutil.SliceArray(arr, 1, 4)
	defer sliced.Release()
	if sliced.Len() != 3 {
		t.Fatalf("expected len 3, got %d", sliced.Len())
	}
	for i, want := range []int64{20, 30, 40} {
		got, err := arrowutil.GetValue(sliced, i)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("index %d: expected %d, got %v", i, want, got)
		}
	}
}

func TestSliceArray_Empty(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{1, 2, 3})
	defer arr.Release()
	sliced := arrowutil.SliceArray(arr, 1, 1)
	defer sliced.Release()
	if sliced.Len() != 0 {
		t.Fatalf("expected len 0, got %d", sliced.Len())
	}
}

func TestGetValue_Timestamp_Nanosecond(t *testing.T) {
	alloc := memory.DefaultAllocator
	dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	bldr := array.NewTimestampBuilder(alloc, dt)
	defer bldr.Release()
	ts := time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC)
	bldr.Append(arrow.Timestamp(ts.UnixNano()))
	arr := bldr.NewTimestampArray()
	defer arr.Release()

	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotTime, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", got)
	}
	if !gotTime.Equal(ts) {
		t.Errorf("expected %v, got %v", ts, gotTime)
	}
}
