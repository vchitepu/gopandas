package series

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/dtype"
	"github.com/vinaychitepu/gopandas/index"
)

func TestMap_Int64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	doubled := s.Map(func(v int64) int64 { return v * 2 })
	if doubled.Len() != 3 {
		t.Fatalf("Map().Len() = %d, want 3", doubled.Len())
	}
	vals := doubled.Values()
	want := []int64{2, 4, 6}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Map().Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
	if doubled.Name() != "x" {
		t.Errorf("Map().Name() = %q, want %q", doubled.Name(), "x")
	}
}

func TestMap_String(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"hello", "world"}, idx, "x")

	upper := s.Map(func(v string) string { return v + "!" })
	vals := upper.Values()
	if vals[0] != "hello!" || vals[1] != "world!" {
		t.Errorf("Map().Values() = %v, want [hello! world!]", vals)
	}
}

func TestMap_Float64(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0, 4.0}, idx, "x")

	result := s.Map(func(v float64) float64 { return v + 0.5 })
	vals := result.Values()
	if vals[0] != 1.5 || vals[1] != 4.5 {
		t.Errorf("Map().Values() = %v, want [1.5 4.5]", vals)
	}
}

func TestApply_Int64ToString(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	result := s.Apply(func(v int64) any {
		return fmt.Sprintf("val_%d", v)
	})
	if result.Len() != 3 {
		t.Fatalf("Apply().Len() = %d, want 3", result.Len())
	}
	if result.Name() != "x" {
		t.Errorf("Apply().Name() = %q, want %q", result.Name(), "x")
	}
}

func TestSort_Int64Ascending(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{30, 10, 50, 20, 40}, idx, "x")

	sorted := s.Sort(true)
	vals := sorted.Values()
	want := []int64{10, 20, 30, 40, 50}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Sort(true).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestSort_Int64Descending(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{30, 10, 50, 20, 40}, idx, "x")

	sorted := s.Sort(false)
	vals := sorted.Values()
	want := []int64{50, 40, 30, 20, 10}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Sort(false).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestSort_StringAscending(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[string](memory.DefaultAllocator, []string{"cherry", "apple", "banana"}, idx, "x")

	sorted := s.Sort(true)
	vals := sorted.Values()
	want := []string{"apple", "banana", "cherry"}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Sort(true).Values()[%d] = %q, want %q", i, v, want[i])
		}
	}
}

func TestSort_Float64Ascending(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{3.3, 1.1, 2.2}, idx, "x")

	sorted := s.Sort(true)
	vals := sorted.Values()
	want := []float64{1.1, 2.2, 3.3}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("Sort(true).Values()[%d] = %v, want %v", i, v, want[i])
		}
	}
}

func TestSort_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")

	sorted := s.Sort(true)
	if sorted.Len() != 0 {
		t.Errorf("Sort(true).Len() = %d, want 0", sorted.Len())
	}
}

func TestRename(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2}, idx, "old_name")

	renamed := s.Rename("new_name")
	if renamed.Name() != "new_name" {
		t.Errorf("Rename().Name() = %q, want %q", renamed.Name(), "new_name")
	}
	if s.Name() != "old_name" {
		t.Errorf("original Name() = %q, want %q", s.Name(), "old_name")
	}
	if renamed.Len() != 2 {
		t.Errorf("Rename().Len() = %d, want 2", renamed.Len())
	}
	vals := renamed.Values()
	if vals[0] != 1 || vals[1] != 2 {
		t.Errorf("Rename().Values() = %v, want [1 2]", vals)
	}
}

func TestAsType_Int64ToFloat64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3}, idx, "x")

	converted, err := s.AsType(dtype.Float64)
	if err != nil {
		t.Fatal(err)
	}
	if converted.Len() != 3 {
		t.Fatalf("AsType().Len() = %d, want 3", converted.Len())
	}
	if converted.DType() != dtype.Float64 {
		t.Errorf("AsType().DType() = %v, want Float64", converted.DType())
	}
}

func TestAsType_Int64ToString(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[int64](memory.DefaultAllocator, []int64{42, 99}, idx, "x")

	converted, err := s.AsType(dtype.String)
	if err != nil {
		t.Fatal(err)
	}
	if converted.DType() != dtype.String {
		t.Errorf("AsType().DType() = %v, want String", converted.DType())
	}
}

func TestAsType_Float64ToInt64(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.9, 2.1}, idx, "x")

	converted, err := s.AsType(dtype.Int64)
	if err != nil {
		t.Fatal(err)
	}
	if converted.DType() != dtype.Int64 {
		t.Errorf("AsType().DType() = %v, want Int64", converted.DType())
	}
}

func TestAsType_SameType(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2}, idx, "x")

	converted, err := s.AsType(dtype.Int64)
	if err != nil {
		t.Fatal(err)
	}
	if converted.Len() != 2 {
		t.Errorf("AsType(same).Len() = %d, want 2", converted.Len())
	}
}
