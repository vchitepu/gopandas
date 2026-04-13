package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/index"
)

func TestHead(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, idx, "x")

	h := s.Head(3)
	if h.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", h.Len())
	}
	vals := h.Values()
	want := []int64{1, 2, 3}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Head(3).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
	if h.Name() != "x" {
		t.Errorf("Head(3).Name() = %q, want %q", h.Name(), "x")
	}
}

func TestHeadExceedsLen(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	h := s.Head(10)
	if h.Len() != 3 {
		t.Errorf("Head(10).Len() = %d, want 3", h.Len())
	}
}

func TestHeadZero(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	h := s.Head(0)
	if h.Len() != 0 {
		t.Errorf("Head(0).Len() = %d, want 0", h.Len())
	}
}

func TestHeadWithStringIndex(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b", "c", "d"}, "lbl")
	s := New[string](memory.DefaultAllocator, []string{"w", "x", "y", "z"}, idx, "s")

	h := s.Head(2)
	if h.Len() != 2 {
		t.Errorf("Head(2).Len() = %d, want 2", h.Len())
	}
	if h.Index().Len() != 2 {
		t.Errorf("Head(2).Index().Len() = %d, want 2", h.Index().Len())
	}
	vals := h.Values()
	if vals[0] != "w" || vals[1] != "x" {
		t.Errorf("Head(2).Values() = %v, want [w x]", vals)
	}
}

func TestTail(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, idx, "x")

	tl := s.Tail(3)
	if tl.Len() != 3 {
		t.Errorf("Tail(3).Len() = %d, want 3", tl.Len())
	}
	vals := tl.Values()
	want := []int64{3, 4, 5}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Tail(3).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
	if tl.Name() != "x" {
		t.Errorf("Tail(3).Name() = %q, want %q", tl.Name(), "x")
	}
}

func TestTailExceedsLen(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	tl := s.Tail(10)
	if tl.Len() != 3 {
		t.Errorf("Tail(10).Len() = %d, want 3", tl.Len())
	}
}

func TestTailZero(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	tl := s.Tail(0)
	if tl.Len() != 0 {
		t.Errorf("Tail(0).Len() = %d, want 0", tl.Len())
	}
}

func TestILoc(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30, 40, 50}, idx, "x")

	sl := s.ILoc(1, 4)
	if sl.Len() != 3 {
		t.Errorf("ILoc(1,4).Len() = %d, want 3", sl.Len())
	}
	vals := sl.Values()
	want := []int64{20, 30, 40}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("ILoc(1,4).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestILocFullRange(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	sl := s.ILoc(0, 3)
	if sl.Len() != 3 {
		t.Errorf("ILoc(0,3).Len() = %d, want 3", sl.Len())
	}
}

func TestILocEmpty(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	sl := s.ILoc(1, 1)
	if sl.Len() != 0 {
		t.Errorf("ILoc(1,1).Len() = %d, want 0", sl.Len())
	}
}

func TestILocClamping(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, idx, "x")

	sl := s.ILoc(-1, 100)
	if sl.Len() != 5 {
		t.Errorf("ILoc(-1,100).Len() = %d, want 5", sl.Len())
	}
}

func TestFilter(t *testing.T) {
	idx := index.NewStringIndex([]string{"a", "b", "c", "d"}, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30, 40}, idx, "x")

	mask := []bool{true, false, true, false}
	f := s.Filter(mask)

	if f.Len() != 2 {
		t.Errorf("Filter().Len() = %d, want 2", f.Len())
	}
	vals := f.Values()
	want := []int64{10, 30}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Filter().Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
	if f.Name() != "x" {
		t.Errorf("Filter().Name() = %q, want %q", f.Name(), "x")
	}
}

func TestFilterAllTrue(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	f := s.Filter([]bool{true, true, true})
	if f.Len() != 3 {
		t.Errorf("Filter(all true).Len() = %d, want 3", f.Len())
	}
}

func TestFilterAllFalse(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	f := s.Filter([]bool{false, false, false})
	if f.Len() != 0 {
		t.Errorf("Filter(all false).Len() = %d, want 0", f.Len())
	}
}

func TestFilterPanicsOnLengthMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on mask length mismatch")
		}
	}()
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")
	s.Filter([]bool{true, false})
}

func TestFilterPreservesNulls(t *testing.T) {
	// Build an int64 array with a null at position 1
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	arr := bldr.NewInt64Array()
	bldr.Release()

	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "x")

	// Mask keeps positions 0 and 1 (null)
	f := s.Filter([]bool{true, true, false})

	if f.Len() != 2 {
		t.Fatalf("Filter().Len() = %d, want 2", f.Len())
	}
	// Position 0 is non-null, value 10
	if f.IsNull(0) {
		t.Error("Filter()[0] IsNull = true, want false")
	}
	v, isNull := f.At(0)
	if isNull || v != int64(10) {
		t.Errorf("Filter()[0] = (%v, %v), want (10, false)", v, isNull)
	}
	// Position 1 was null — must remain null
	if !f.IsNull(1) {
		t.Error("Filter()[1] IsNull = false, want true (null must be preserved)")
	}
}
