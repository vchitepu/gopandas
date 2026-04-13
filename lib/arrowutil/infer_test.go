package arrowutil_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/arrowutil"
	"github.com/vchitepu/gopandas/lib/dtype"
)

func TestInferDType_Int64(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{1, 2})
	defer arr.Release()
	if got := arrowutil.InferDType(arr); got != dtype.Int64 {
		t.Errorf("expected Int64, got %v", got)
	}
}

func TestInferDType_Float64(t *testing.T) {
	arr := arrowutil.BuildFloat64Array(memory.DefaultAllocator, []float64{1.0})
	defer arr.Release()
	if got := arrowutil.InferDType(arr); got != dtype.Float64 {
		t.Errorf("expected Float64, got %v", got)
	}
}

func TestInferDType_String(t *testing.T) {
	arr := arrowutil.BuildStringArray(memory.DefaultAllocator, []string{"x"})
	defer arr.Release()
	if got := arrowutil.InferDType(arr); got != dtype.String {
		t.Errorf("expected String, got %v", got)
	}
}

func TestInferDType_Bool(t *testing.T) {
	arr := arrowutil.BuildBoolArray(memory.DefaultAllocator, []bool{true})
	defer arr.Release()
	if got := arrowutil.InferDType(arr); got != dtype.Bool {
		t.Errorf("expected Bool, got %v", got)
	}
}

func TestInferDType_Timestamp(t *testing.T) {
	arr := arrowutil.BuildTimestampArray(memory.DefaultAllocator, []time.Time{time.Now()})
	defer arr.Release()
	if got := arrowutil.InferDType(arr); got != dtype.Timestamp {
		t.Errorf("expected Timestamp, got %v", got)
	}
}

func TestBuildArray_Int64(t *testing.T) {
	values := []any{int64(1), int64(2), int64(3)}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.Int64)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("expected len 3, got %d", arr.Len())
	}
	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(1) {
		t.Errorf("expected 1, got %v", got)
	}
}

func TestBuildArray_Float64(t *testing.T) {
	values := []any{float64(1.5), float64(2.5)}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.Float64)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 2 {
		t.Fatalf("expected len 2, got %d", arr.Len())
	}
}

func TestBuildArray_String(t *testing.T) {
	values := []any{"hello", "world"}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.String)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 2 {
		t.Fatalf("expected len 2, got %d", arr.Len())
	}
}

func TestBuildArray_Bool(t *testing.T) {
	values := []any{true, false, true}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.Bool)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("expected len 3, got %d", arr.Len())
	}
}

func TestBuildArray_Timestamp(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	values := []any{t1}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 1 {
		t.Fatalf("expected len 1, got %d", arr.Len())
	}
	got, err := arrowutil.GetValue(arr, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotTime, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", got)
	}
	if !gotTime.Equal(t1) {
		t.Errorf("expected %v, got %v", t1, gotTime)
	}
}

func TestBuildArray_WithNils(t *testing.T) {
	values := []any{int64(1), nil, int64(3)}
	arr, err := arrowutil.BuildArray(memory.DefaultAllocator, values, dtype.Int64)
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("expected len 3, got %d", arr.Len())
	}
	if arr.NullN() != 1 {
		t.Errorf("expected 1 null, got %d", arr.NullN())
	}
	if !arrowutil.IsNull(arr, 1) {
		t.Error("expected index 1 to be null")
	}
}

func TestBuildArray_TypeMismatch(t *testing.T) {
	_, err := arrowutil.BuildArray(memory.DefaultAllocator, []any{"not_an_int"}, dtype.Int64)
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestBuildArray_UnsupportedDType(t *testing.T) {
	_, err := arrowutil.BuildArray(memory.DefaultAllocator, []any{1}, dtype.Invalid)
	if err == nil {
		t.Error("expected error for unsupported dtype")
	}
}
