package ops

import (
	"math"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func TestPivot_Basic(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"date":    []string{"2024-01", "2024-01", "2024-02", "2024-02"},
		"product": []string{"A", "B", "A", "B"},
		"sales":   []float64{100, 200, 150, 250},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Pivot(df, "date", "product", "sales")
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	// Columns: date (index col), A, B
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (date, A, B)", cols)
	}

	// Row 0: date=2024-01, A=100, B=200
	v, _ := result.At(0, "date")
	if v != "2024-01" {
		t.Errorf("row0 date = %v, want 2024-01", v)
	}
	v, _ = result.At(0, "A")
	if v != float64(100) {
		t.Errorf("row0 A = %v, want 100", v)
	}
	v, _ = result.At(0, "B")
	if v != float64(200) {
		t.Errorf("row0 B = %v, want 200", v)
	}

	// Row 1: date=2024-02, A=150, B=250
	v, _ = result.At(1, "A")
	if v != float64(150) {
		t.Errorf("row1 A = %v, want 150", v)
	}
	v, _ = result.At(1, "B")
	if v != float64(250) {
		t.Errorf("row1 B = %v, want 250", v)
	}
}

func TestPivot_DuplicateIndex(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"date":    []string{"2024-01", "2024-01"},
		"product": []string{"A", "A"}, // duplicate (date, product) pair
		"sales":   []float64{100, 200},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Pivot(df, "date", "product", "sales")
	if err == nil {
		t.Error("expected error for duplicate index/column pair")
	}
}

func TestPivot_MissingColumn(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Pivot(df, "missing", "a", "a")
	if err == nil {
		t.Error("expected error for missing column")
	}
}

func TestPivotTable_Sum(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"date":    []string{"2024-01", "2024-01", "2024-01", "2024-02"},
		"product": []string{"A", "A", "B", "A"},
		"sales":   []float64{100, 50, 200, 150},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := PivotTable(df, "date", "product", "sales", AggSum)
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (date, A, B)", cols)
	}

	// Row 0 (2024-01): A=100+50=150, B=200
	v, _ := result.At(0, "A")
	if v != float64(150) {
		t.Errorf("row0 A = %v, want 150", v)
	}
	v, _ = result.At(0, "B")
	if v != float64(200) {
		t.Errorf("row0 B = %v, want 200", v)
	}

	// Row 1 (2024-02): A=150, B=NaN (missing)
	v, _ = result.At(1, "A")
	if v != float64(150) {
		t.Errorf("row1 A = %v, want 150", v)
	}
}

func TestPivotTable_Mean(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"date":    []string{"2024-01", "2024-01", "2024-01"},
		"product": []string{"A", "A", "B"},
		"sales":   []float64{100, 200, 300},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := PivotTable(df, "date", "product", "sales", AggMean)
	if err != nil {
		t.Fatal(err)
	}

	// A = mean(100, 200) = 150
	v, _ := result.At(0, "A")
	if v != float64(150) {
		t.Errorf("A = %v, want 150", v)
	}
	// B = mean(300) = 300
	v, _ = result.At(0, "B")
	if v != float64(300) {
		t.Errorf("B = %v, want 300", v)
	}
}

func TestPivotTable_Count_WithNaN(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"date":    []string{"2024-01", "2024-01", "2024-02"},
		"product": []string{"A", "A", "B"},
		"sales":   []float64{100, 200, 300},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := PivotTable(df, "date", "product", "sales", AggCount)
	if err != nil {
		t.Fatal(err)
	}

	// 2024-01, A: count=2
	v, _ := result.At(0, "A")
	if v != float64(2) {
		t.Errorf("2024-01 A count = %v, want 2", v)
	}

	// 2024-01, B: NaN (missing)
	v, _ = result.At(0, "B")
	if f, ok := v.(float64); !ok || !math.IsNaN(f) {
		t.Errorf("2024-01 B = %v, want NaN", v)
	}

	// 2024-02, A: NaN (missing)
	v, _ = result.At(1, "A")
	if f, ok := v.(float64); !ok || !math.IsNaN(f) {
		t.Errorf("2024-02 A = %v, want NaN", v)
	}

	// 2024-02, B: count=1
	v, _ = result.At(1, "B")
	if v != float64(1) {
		t.Errorf("2024-02 B count = %v, want 1", v)
	}
}

func TestPivotTable_MissingColumn(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = PivotTable(df, "a", "missing", "a", AggSum)
	if err == nil {
		t.Error("expected error for missing column")
	}
}
