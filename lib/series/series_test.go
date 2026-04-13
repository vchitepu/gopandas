package series

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/arrowutil"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
)

func TestNewInt64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "col1")
	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}
	if s.Name() != "col1" {
		t.Errorf("Name() = %q, want %q", s.Name(), "col1")
	}
}

func TestNewFloat64(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.1, 2.2}, idx, "floats")
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2", s.Len())
	}
	if s.Name() != "floats" {
		t.Errorf("Name() = %q, want %q", s.Name(), "floats")
	}
}

func TestNewString(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b"}, "")
	s := New[string](memory.DefaultAllocator, []string{"hello", "world"}, idx, "words")
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2", s.Len())
	}
	if s.Name() != "words" {
		t.Errorf("Name() = %q, want %q", s.Name(), "words")
	}
}

func TestNewBool(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[bool](memory.DefaultAllocator, []bool{true, false, true}, idx, "flags")
	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}
}

func TestNewEmpty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "empty")
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
}

func TestNewPanicsOnLengthMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on length mismatch")
		}
	}()
	idx := index.NewRangeIndex(2, "")
	New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "bad")
}

func TestFromArrow(t *testing.T) {
	arr := arrowutil.BuildInt64Array(memory.DefaultAllocator, []int64{100, 200, 300})
	defer arr.Release()

	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "from_arrow")

	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}
	if s.Name() != "from_arrow" {
		t.Errorf("Name() = %q, want %q", s.Name(), "from_arrow")
	}
}

func TestDType_Int64(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2}, idx, "x")
	if s.DType() != dtype.Int64 {
		t.Errorf("DType() = %v, want Int64", s.DType())
	}
}

func TestDType_Float64(t *testing.T) {
	idx := index.NewRangeIndex(1, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0}, idx, "x")
	if s.DType() != dtype.Float64 {
		t.Errorf("DType() = %v, want Float64", s.DType())
	}
}

func TestDType_String(t *testing.T) {
	idx := index.NewRangeIndex(1, "")
	s := New[string](memory.DefaultAllocator, []string{"a"}, idx, "x")
	if s.DType() != dtype.String {
		t.Errorf("DType() = %v, want String", s.DType())
	}
}

func TestDType_Bool(t *testing.T) {
	idx := index.NewRangeIndex(1, "")
	s := New[bool](memory.DefaultAllocator, []bool{true}, idx, "x")
	if s.DType() != dtype.Bool {
		t.Errorf("DType() = %v, want Bool", s.DType())
	}
}

func TestIndex(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b"}, "labels")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2}, idx, "x")
	got := s.Index()
	if got.Len() != 2 {
		t.Errorf("Index().Len() = %d, want 2", got.Len())
	}
	if got.Name() != "labels" {
		t.Errorf("Index().Name() = %q, want %q", got.Name(), "labels")
	}
}

func TestString(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "values")
	got := s.String()
	if !strings.Contains(got, "values") {
		t.Errorf("String() missing series name, got:\n%s", got)
	}
	if !strings.Contains(got, "10") || !strings.Contains(got, "20") || !strings.Contains(got, "30") {
		t.Errorf("String() missing values, got:\n%s", got)
	}
	if !strings.Contains(got, "int64") {
		t.Errorf("String() missing dtype, got:\n%s", got)
	}
}

func TestStringWithStringIndex(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b"}, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.5, 2.5}, idx, "prices")
	got := s.String()
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") {
		t.Errorf("String() missing index labels, got:\n%s", got)
	}
	if !strings.Contains(got, "1.5") || !strings.Contains(got, "2.5") {
		t.Errorf("String() missing values, got:\n%s", got)
	}
}
