package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/index"
)

func TestDropNA(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	bldr.AppendNull()
	bldr.Append(50)
	arr := bldr.NewInt64Array()
	bldr.Release()

	idx := index.NewRangeIndex(5, "")
	s := FromArrow(arr, idx, "x")

	dropped := s.DropNA()
	if dropped.Len() != 3 {
		t.Errorf("DropNA().Len() = %d, want 3", dropped.Len())
	}
}

func TestDropNA_NoNulls(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	dropped := s.DropNA()
	if dropped.Len() != 3 {
		t.Errorf("DropNA().Len() = %d, want 3", dropped.Len())
	}
}

func TestDropNA_AllNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.AppendNull()
	bldr.AppendNull()
	arr := bldr.NewInt64Array()
	bldr.Release()

	idx := index.NewRangeIndex(2, "")
	s := FromArrow(arr, idx, "x")

	dropped := s.DropNA()
	if dropped.Len() != 0 {
		t.Errorf("DropNA().Len() = %d, want 0", dropped.Len())
	}
}

func TestFillNA(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	bldr.AppendNull()
	arr := bldr.NewInt64Array()
	bldr.Release()

	idx := index.NewRangeIndex(4, "")
	s := FromArrow(arr, idx, "x")

	filled := s.FillNA(int64(0))
	if filled.Len() != 4 {
		t.Fatalf("FillNA().Len() = %d, want 4", filled.Len())
	}
	if filled.IsNull(1) {
		t.Error("FillNA(): index 1 still null")
	}
	if filled.IsNull(3) {
		t.Error("FillNA(): index 3 still null")
	}
}

func TestFillNA_NoNulls(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	filled := s.FillNA(int64(0))
	vals := filled.Values()
	want := []int64{1, 2, 3}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("FillNA().Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestFillNA_String(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewStringBuilder(alloc)
	bldr.Append("a")
	bldr.AppendNull()
	bldr.Append("c")
	arr := bldr.NewStringArray()
	bldr.Release()

	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "x")

	filled := s.FillNA("missing")
	if filled.Len() != 3 {
		t.Fatalf("FillNA().Len() = %d, want 3", filled.Len())
	}
	if filled.IsNull(1) {
		t.Error("FillNA(): index 1 still null")
	}
}

func TestCount_NoNulls(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")
	if s.Count() != 3 {
		t.Errorf("Count() = %d, want 3", s.Count())
	}
}

func TestCount_WithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.Append(30)
	arr := bldr.NewInt64Array()
	bldr.Release()
	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "x")
	if s.Count() != 2 {
		t.Errorf("Count() = %d, want 2", s.Count())
	}
}

func TestNullCount_Zero(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")
	if s.NullCount() != 0 {
		t.Errorf("NullCount() = %d, want 0", s.NullCount())
	}
}

func TestNullCount_WithNulls(t *testing.T) {
	alloc := memory.DefaultAllocator
	bldr := array.NewInt64Builder(alloc)
	bldr.Append(10)
	bldr.AppendNull()
	bldr.AppendNull()
	arr := bldr.NewInt64Array()
	bldr.Release()
	idx := index.NewRangeIndex(3, "")
	s := FromArrow(arr, idx, "x")
	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %d, want 2", s.NullCount())
	}
}
