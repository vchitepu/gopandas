package dataframe

import (
	"math"
	"testing"
)

// --- Task 25: Sum and Count ---

func TestSum(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{10.0, 20.0, 30.0},
		"c": []string{"x", "y", "z"}, // non-numeric, should be skipped
	})
	result := df.Sum()
	// Check "a"
	aVal, _ := result.Loc("a")
	if aVal != float64(6) {
		t.Errorf("Sum().Loc(a) = %v, want 6.0", aVal)
	}
	// Check "b"
	bVal, _ := result.Loc("b")
	if bVal != float64(60) {
		t.Errorf("Sum().Loc(b) = %v, want 60.0", bVal)
	}
}

func TestCount(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": 10.0},
		{"a": int64(2), "b": nil},
		{"a": int64(3), "b": 30.0},
	}
	df, _ := FromRecords(records)
	result := df.Count()
	aVal, _ := result.Loc("a")
	if aVal != int64(3) {
		t.Errorf("Count().Loc(a) = %v, want 3", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != int64(2) {
		t.Errorf("Count().Loc(b) = %v, want 2", bVal)
	}
}

// --- Task 26: Mean and Std ---

func TestMean(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{10.0, 20.0, 30.0},
		"b": []int64{1, 2, 3},
	})
	result := df.Mean()
	aVal, _ := result.Loc("a")
	if aVal != 20.0 {
		t.Errorf("Mean().Loc(a) = %v, want 20.0", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != 2.0 {
		t.Errorf("Mean().Loc(b) = %v, want 2.0", bVal)
	}
}

func TestStd(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
	})
	result := df.Std()
	aVal, _ := result.Loc("a")
	aFloat, ok := aVal.(float64)
	if !ok {
		t.Fatalf("Std().Loc(a) type = %T, want float64", aVal)
	}
	// Sample std of [2,4,4,4,5,5,7,9] ~= 2.138
	if math.Abs(aFloat-2.138) > 0.1 {
		t.Errorf("Std().Loc(a) = %v, want ~2.138", aFloat)
	}
}

// --- Task 27: Min and Max ---

func TestMin(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{3.0, 1.0, 2.0},
		"b": []float64{30.0, 10.0, 20.0},
	})
	result := df.Min()
	aVal, _ := result.Loc("a")
	if aVal != 1.0 {
		t.Errorf("Min().Loc(a) = %v, want 1.0", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != 10.0 {
		t.Errorf("Min().Loc(b) = %v, want 10.0", bVal)
	}
}

func TestMax(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{3.0, 1.0, 2.0},
		"b": []float64{30.0, 10.0, 20.0},
	})
	result := df.Max()
	aVal, _ := result.Loc("a")
	if aVal != 3.0 {
		t.Errorf("Max().Loc(a) = %v, want 3.0", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != 30.0 {
		t.Errorf("Max().Loc(b) = %v, want 30.0", bVal)
	}
}

// --- Task 28: Describe ---

func TestDescribe(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []int64{10, 20, 30, 40, 50},
	})
	desc := df.Describe()
	rows, cols := desc.Shape()
	if rows != 5 {
		t.Errorf("Describe().Shape() rows = %d, want 5", rows)
	}
	if cols < 2 {
		t.Errorf("Describe().Shape() cols = %d, want >= 2", cols)
	}
	// Check count row
	countVal, err := desc.Loc("count", "a")
	if err != nil {
		t.Fatalf("Describe().Loc(count, a) error: %v", err)
	}
	if countVal != 5.0 {
		t.Errorf("Describe().Loc(count, a) = %v, want 5.0", countVal)
	}
}

// --- Task 29: Corr ---

func TestCorr(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{2.0, 4.0, 6.0, 8.0, 10.0},
	})
	corr := df.Corr()
	// a with a should be 1.0
	aa, err := corr.Loc("a", "a")
	if err != nil {
		t.Fatalf("Corr().Loc(a, a) error: %v", err)
	}
	aaFloat, _ := aa.(float64)
	if math.Abs(aaFloat-1.0) > 0.001 {
		t.Errorf("Corr().Loc(a, a) = %v, want 1.0", aa)
	}
	// a with b should be 1.0 (perfectly correlated)
	ab, _ := corr.Loc("a", "b")
	abFloat, _ := ab.(float64)
	if math.Abs(abFloat-1.0) > 0.001 {
		t.Errorf("Corr().Loc(a, b) = %v, want 1.0", ab)
	}
}

// --- Task 30: CorrWith ---

func TestCorrWith(t *testing.T) {
	df, _ := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{5.0, 4.0, 3.0, 2.0, 1.0},
	})
	// Create a reference series
	refS := df.data["a"]
	result := df.CorrWith(refS)

	// a corr with a should be 1.0
	aVal, _ := result.Loc("a")
	aFloat, _ := aVal.(float64)
	if math.Abs(aFloat-1.0) > 0.001 {
		t.Errorf("CorrWith().Loc(a) = %v, want 1.0", aFloat)
	}
	// b corr with a should be -1.0
	bVal, _ := result.Loc("b")
	bFloat, _ := bVal.(float64)
	if math.Abs(bFloat-(-1.0)) > 0.001 {
		t.Errorf("CorrWith().Loc(b) = %v, want -1.0", bFloat)
	}
}
