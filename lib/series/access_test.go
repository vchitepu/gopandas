package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/index"
)

func TestAt_Int64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "x")

	val, isNull := s.At(0)
	if isNull {
		t.Error("At(0) returned isNull=true, want false")
	}
	if val != 10 {
		t.Errorf("At(0) = %v, want 10", val)
	}

	val, isNull = s.At(2)
	if isNull {
		t.Error("At(2) returned isNull=true, want false")
	}
	if val != 30 {
		t.Errorf("At(2) = %v, want 30", val)
	}
}

func TestAt_String(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"hello", "world"}, idx, "x")

	val, isNull := s.At(1)
	if isNull {
		t.Error("At(1) returned isNull=true, want false")
	}
	if val != "world" {
		t.Errorf("At(1) = %q, want %q", val, "world")
	}
}

func TestAt_Float64(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.5, 2.5}, idx, "x")

	val, isNull := s.At(0)
	if isNull {
		t.Error("At(0) returned isNull=true, want false")
	}
	if val != 1.5 {
		t.Errorf("At(0) = %v, want 1.5", val)
	}
}

func TestAt_Bool(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[bool](memory.DefaultAllocator, []bool{true, false}, idx, "x")

	val, isNull := s.At(1)
	if isNull {
		t.Error("At(1) returned isNull=true, want false")
	}
	if val != false {
		t.Errorf("At(1) = %v, want false", val)
	}
}

func TestLoc_StringIndex(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b", "c"}, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "x")

	val, isNull := s.Loc("b")
	if isNull {
		t.Error("Loc(\"b\") returned isNull=true, want false")
	}
	if val != 20 {
		t.Errorf("Loc(\"b\") = %v, want 20", val)
	}
}

func TestLoc_RangeIndex(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.1, 2.2, 3.3}, idx, "x")

	val, isNull := s.Loc(2)
	if isNull {
		t.Error("Loc(2) returned isNull=true, want false")
	}
	if val != 3.3 {
		t.Errorf("Loc(2) = %v, want 3.3", val)
	}
}

func TestLoc_NotFound(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Loc with missing label did not panic")
		}
	}()
	idx := index.NewStringIndex([]string{"a", "b"}, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2}, idx, "x")
	s.Loc("z") // should panic
}

func TestValues_Int64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "x")

	vals := s.Values()
	if len(vals) != 3 {
		t.Fatalf("Values() len = %d, want 3", len(vals))
	}
	want := []int64{10, 20, 30}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestValues_String(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"hello", "world"}, idx, "x")

	vals := s.Values()
	if len(vals) != 2 {
		t.Fatalf("Values() len = %d, want 2", len(vals))
	}
	if vals[0] != "hello" || vals[1] != "world" {
		t.Errorf("Values() = %v, want [hello world]", vals)
	}
}

func TestValues_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")

	vals := s.Values()
	if len(vals) != 0 {
		t.Fatalf("Values() len = %d, want 0", len(vals))
	}
}

func TestIsNull_NoNulls(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	for i := 0; i < 3; i++ {
		if s.IsNull(i) {
			t.Errorf("IsNull(%d) = true, want false", i)
		}
	}
}

func TestIsNull_WithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	arr := bldr.NewInt64Array()
	bldr.Release()

	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "x")

	if s.IsNull(0) {
		t.Error("IsNull(0) = true, want false")
	}
	if !s.IsNull(1) {
		t.Error("IsNull(1) = false, want true")
	}
	if s.IsNull(2) {
		t.Error("IsNull(2) = true, want false")
	}
}
