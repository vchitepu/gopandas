package series

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/index"
)

func TestSum_Int64(t *testing.T) {
	idx := index.NewRangeIndex(4, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30, 40}, idx, "x")

	got, err := s.Sum()
	if err != nil {
		t.Fatal(err)
	}
	if got != 100 {
		t.Errorf("Sum() = %v, want 100", got)
	}
}

func TestSum_Float64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.5, 2.5, 3.0}, idx, "x")

	got, err := s.Sum()
	if err != nil {
		t.Fatal(err)
	}
	if got != 7.0 {
		t.Errorf("Sum() = %v, want 7.0", got)
	}
}

func TestSum_String_Error(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b"}, idx, "x")

	_, err := s.Sum()
	if err == nil {
		t.Error("expected error for Sum() on string series")
	}
}

func TestSum_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")

	got, err := s.Sum()
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Errorf("Sum() = %v, want 0", got)
	}
}

func TestMean_Int64(t *testing.T) {
	idx := index.NewRangeIndex(4, "")
	s := New[int64](memory.DefaultAllocator, []int64{10, 20, 30, 40}, idx, "x")
	got, err := s.Mean()
	if err != nil {
		t.Fatal(err)
	}
	if got != 25.0 {
		t.Errorf("Mean() = %v, want 25.0", got)
	}
}

func TestMean_Float64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, idx, "x")
	got, err := s.Mean()
	if err != nil {
		t.Fatal(err)
	}
	if got != 2.0 {
		t.Errorf("Mean() = %v, want 2.0", got)
	}
}

func TestMean_String_Error(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b"}, idx, "x")
	_, err := s.Mean()
	if err == nil {
		t.Error("expected error for Mean() on string series")
	}
}

func TestMean_Empty(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[float64](memory.DefaultAllocator, []float64{}, idx, "x")
	got, err := s.Mean()
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(got) {
		t.Errorf("Mean() = %v, want NaN", got)
	}
}

func TestStd_Int64(t *testing.T) {
	idx := index.NewRangeIndex(4, "")
	s := New[int64](memory.DefaultAllocator, []int64{2, 4, 4, 4}, idx, "x")
	got, err := s.Std()
	if err != nil {
		t.Fatal(err)
	}
	// sample std dev of [2, 4, 4, 4] = 1.0
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("Std() = %v, want 1.0", got)
	}
}

func TestStd_String_Error(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b"}, idx, "x")
	_, err := s.Std()
	if err == nil {
		t.Error("expected error for Std() on string series")
	}
}

func TestStd_SingleValue(t *testing.T) {
	idx := index.NewRangeIndex(1, "")
	s := New[float64](memory.DefaultAllocator, []float64{5.0}, idx, "x")
	got, err := s.Std()
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(got) {
		t.Errorf("Std() with 1 value = %v, want NaN", got)
	}
}

func TestVar_Int64(t *testing.T) {
	idx := index.NewRangeIndex(4, "")
	s := New[int64](memory.DefaultAllocator, []int64{2, 4, 4, 4}, idx, "x")
	got, err := s.Var()
	if err != nil {
		t.Fatal(err)
	}
	// sample variance of [2, 4, 4, 4]:
	// mean = 14/4 = 3.5
	// deviations: -1.5, 0.5, 0.5, 0.5
	// sum of squares = 2.25 + 0.25 + 0.25 + 0.25 = 3.0
	// / (n-1) = 3 => variance = 1.0
	want := 1.0
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("Var() = %v, want %v", got, want)
	}
}

func TestVar_String_Error(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b"}, idx, "x")
	_, err := s.Var()
	if err == nil {
		t.Error("expected error for Var() on string series")
	}
}

func TestMin_Int64(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{3, 1, 4, 1, 5}, idx, "x")
	got, err := s.Min()
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Errorf("Min() = %v, want 1", got)
	}
}

func TestMin_Float64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{3.3, 1.1, 2.2}, idx, "x")
	got, err := s.Min()
	if err != nil {
		t.Fatal(err)
	}
	if got != 1.1 {
		t.Errorf("Min() = %v, want 1.1", got)
	}
}

func TestMin_Empty_Error(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")
	_, err := s.Min()
	if err == nil {
		t.Error("expected error for Min() on empty series")
	}
}

func TestMax_Int64(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{3, 1, 4, 1, 5}, idx, "x")
	got, err := s.Max()
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("Max() = %v, want 5", got)
	}
}

func TestMax_Float64(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{3.3, 1.1, 2.2}, idx, "x")
	got, err := s.Max()
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.3 {
		t.Errorf("Max() = %v, want 3.3", got)
	}
}

func TestMax_Empty_Error(t *testing.T) {
	idx := index.NewRangeIndex(0, "")
	s := New[int64](memory.DefaultAllocator, []int64{}, idx, "x")
	_, err := s.Max()
	if err == nil {
		t.Error("expected error for Max() on empty series")
	}
}

func TestMedian_OddCount(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{3, 1, 4, 1, 5}, idx, "x")
	got, err := s.Median()
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.0 {
		t.Errorf("Median() = %v, want 3.0", got)
	}
}

func TestMedian_EvenCount(t *testing.T) {
	idx := index.NewRangeIndex(4, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4}, idx, "x")
	got, err := s.Median()
	if err != nil {
		t.Fatal(err)
	}
	if got != 2.5 {
		t.Errorf("Median() = %v, want 2.5", got)
	}
}

func TestMedian_String_Error(t *testing.T) {
	idx := index.NewRangeIndex(2, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b"}, idx, "x")
	_, err := s.Median()
	if err == nil {
		t.Error("expected error for Median() on string series")
	}
}

func TestQuantile_Q50(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, idx, "x")
	got, err := s.Quantile(0.5)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.0 {
		t.Errorf("Quantile(0.5) = %v, want 3.0", got)
	}
}

func TestQuantile_Q0(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, idx, "x")
	got, err := s.Quantile(0.0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1.0 {
		t.Errorf("Quantile(0.0) = %v, want 1.0", got)
	}
}

func TestQuantile_Q1(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, idx, "x")
	got, err := s.Quantile(1.0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.0 {
		t.Errorf("Quantile(1.0) = %v, want 3.0", got)
	}
}

func TestQuantile_InvalidQ(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[float64](memory.DefaultAllocator, []float64{1.0, 2.0, 3.0}, idx, "x")
	_, err := s.Quantile(1.5)
	if err == nil {
		t.Error("expected error for Quantile(1.5)")
	}
}

func TestDescribe_Int64(t *testing.T) {
	idx := index.NewRangeIndex(5, "")
	s := New[int64](memory.DefaultAllocator, []int64{1, 2, 3, 4, 5}, idx, "x")
	desc := s.Describe()

	if desc["count"] != 5.0 {
		t.Errorf("Describe()[count] = %v, want 5.0", desc["count"])
	}
	if desc["mean"] != 3.0 {
		t.Errorf("Describe()[mean] = %v, want 3.0", desc["mean"])
	}
	if desc["min"] != 1.0 {
		t.Errorf("Describe()[min] = %v, want 1.0", desc["min"])
	}
	if desc["max"] != 5.0 {
		t.Errorf("Describe()[max] = %v, want 5.0", desc["max"])
	}
	if _, ok := desc["std"]; !ok {
		t.Error("Describe() missing 'std' key")
	}
	if _, ok := desc["25%"]; !ok {
		t.Error("Describe() missing '25%' key")
	}
	if _, ok := desc["50%"]; !ok {
		t.Error("Describe() missing '50%' key")
	}
	if _, ok := desc["75%"]; !ok {
		t.Error("Describe() missing '75%' key")
	}
}

func TestDescribe_String(t *testing.T) {
	idx := index.NewRangeIndex(3, "")
	s := New[string](memory.DefaultAllocator, []string{"a", "b", "c"}, idx, "x")
	desc := s.Describe()

	if desc["count"] != 3.0 {
		t.Errorf("Describe()[count] = %v, want 3.0", desc["count"])
	}
	// String series should not have mean/std etc
	if _, ok := desc["mean"]; ok {
		t.Error("Describe() for string series should not have 'mean' key")
	}
}
