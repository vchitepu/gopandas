package arrowutil_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/arrowutil"
)

func TestBuildInt64Array(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5}
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, values)
	defer arr.Release()
	if arr.Len() != 5 {
		t.Fatalf("expected len 5, got %d", arr.Len())
	}
	for i, want := range values {
		if arr.Value(i) != want {
			t.Errorf("index %d: expected %d, got %d", i, want, arr.Value(i))
		}
	}
	if arr.NullN() != 0 {
		t.Errorf("expected 0 nulls, got %d", arr.NullN())
	}
}

func TestBuildInt64Array_Empty(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{})
	defer arr.Release()
	if arr.Len() != 0 {
		t.Fatalf("expected len 0, got %d", arr.Len())
	}
}

func TestBuildFloat64Array(t *testing.T) {
	values := []float64{1.1, 2.2, 3.3}
	arr := arrowutil.BuildFloat64Array(memory.DefaultAllocator, values)
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("expected len 3, got %d", arr.Len())
	}
	for i, want := range values {
		if arr.Value(i) != want {
			t.Errorf("index %d: expected %f, got %f", i, want, arr.Value(i))
		}
	}
}

func TestBuildFloat64Array_Empty(t *testing.T) {
	arr := arrowutil.BuildFloat64Array(memory.DefaultAllocator, []float64{})
	defer arr.Release()
	if arr.Len() != 0 {
		t.Fatalf("expected len 0, got %d", arr.Len())
	}
}

func TestBuildStringArray(t *testing.T) {
	values := []string{"hello", "world", "gopandas"}
	arr := arrowutil.BuildStringArray(memory.DefaultAllocator, values)
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("expected len 3, got %d", arr.Len())
	}
	for i, want := range values {
		if arr.Value(i) != want {
			t.Errorf("index %d: expected %q, got %q", i, want, arr.Value(i))
		}
	}
}

func TestBuildStringArray_Empty(t *testing.T) {
	arr := arrowutil.BuildStringArray(memory.DefaultAllocator, []string{})
	defer arr.Release()
	if arr.Len() != 0 {
		t.Fatalf("expected len 0, got %d", arr.Len())
	}
}

func TestBuildBoolArray(t *testing.T) {
	values := []bool{true, false, true, true, false}
	arr := arrowutil.BuildBoolArray(memory.DefaultAllocator, values)
	defer arr.Release()
	if arr.Len() != 5 {
		t.Fatalf("expected len 5, got %d", arr.Len())
	}
	for i, want := range values {
		if arr.Value(i) != want {
			t.Errorf("index %d: expected %v, got %v", i, want, arr.Value(i))
		}
	}
}

func TestBuildTimestampArray(t *testing.T) {
	t1 := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 20, 14, 0, 0, 0, time.UTC)
	values := []time.Time{t1, t2}
	arr := arrowutil.BuildTimestampArray(memory.DefaultAllocator, values)
	defer arr.Release()
	if arr.Len() != 2 {
		t.Fatalf("expected len 2, got %d", arr.Len())
	}
	if arr.NullN() != 0 {
		t.Errorf("expected 0 nulls, got %d", arr.NullN())
	}
}

func TestBuildTimestampArray_Empty(t *testing.T) {
	arr := arrowutil.BuildTimestampArray(memory.DefaultAllocator, []time.Time{})
	defer arr.Release()
	if arr.Len() != 0 {
		t.Fatalf("expected len 0, got %d", arr.Len())
	}
}
