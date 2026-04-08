package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/index"
)

func TestUnique_Int64(t *testing.T) {
	idx := index.NewRangeIndex(6, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 2, 3, 1, 3}, idx, "x")

	u := s.Unique()
	if u.Len() != 3 {
		t.Errorf("Unique().Len() = %d, want 3", u.Len())
	}
	vals := u.Values()
	want := []int64{1, 2, 3}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Unique().Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestUnique_String(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b", "a", "c", "b"}, idx, "x")

	u := s.Unique()
	if u.Len() != 3 {
		t.Errorf("Unique().Len() = %d, want 3", u.Len())
	}
	vals := u.Values()
	want := []string{"a", "b", "c"}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Unique().Values()[%d] = %q, want %q", i, v, want[i])
		}
	}
}

func TestUnique_AlreadyUnique(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30}, idx, "x")

	u := s.Unique()
	if u.Len() != 3 {
		t.Errorf("Unique().Len() = %d, want 3", u.Len())
	}
}

func TestUnique_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")

	u := s.Unique()
	if u.Len() != 0 {
		t.Errorf("Unique().Len() = %d, want 0", u.Len())
	}
}

func TestValueCounts_Int64(t *testing.T) {
	idx := index.NewRangeIndex(7, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 2, 3, 1, 3, 1}, idx, "x")

	vc := s.ValueCounts()
	if vc.Len() != 3 {
		t.Fatalf("ValueCounts().Len() = %d, want 3", vc.Len())
	}
	vals := vc.Values()
	if vals[0] != 3 {
		t.Errorf("ValueCounts first count = %d, want 3", vals[0])
	}
}

func TestValueCounts_String(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b", "a", "c", "a"}, idx, "x")

	vc := s.ValueCounts()
	if vc.Len() != 3 {
		t.Fatalf("ValueCounts().Len() = %d, want 3", vc.Len())
	}
	vals := vc.Values()
	if vals[0] != 3 {
		t.Errorf("ValueCounts first count = %d, want 3", vals[0])
	}
}

func TestValueCounts_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")

	vc := s.ValueCounts()
	if vc.Len() != 0 {
		t.Errorf("ValueCounts().Len() = %d, want 0", vc.Len())
	}
}
