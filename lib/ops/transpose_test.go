package ops

import (
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func TestTranspose(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Transpose(df)
	if err != nil {
		t.Fatal(err)
	}

	// Original: 2 rows, 2 cols (a, b)
	// Transposed: 2 rows (one per column), 3 cols (index, 0, 1)
	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (index, 0, 1)", cols)
	}

	// Row 0: index=a, 0=1, 1=2
	v, _ := result.At(0, "index")
	if v != "a" {
		t.Errorf("row0 index = %v, want a", v)
	}
	v, _ = result.At(0, "0")
	if v != int64(1) {
		t.Errorf("row0 col0 = %v, want 1", v)
	}
	v, _ = result.At(0, "1")
	if v != int64(2) {
		t.Errorf("row0 col1 = %v, want 2", v)
	}

	// Row 1: index=b, 0=3, 1=4
	v, _ = result.At(1, "index")
	if v != "b" {
		t.Errorf("row1 index = %v, want b", v)
	}
	v, _ = result.At(1, "0")
	if v != int64(3) {
		t.Errorf("row1 col0 = %v, want 3", v)
	}
	v, _ = result.At(1, "1")
	if v != int64(4) {
		t.Errorf("row1 col1 = %v, want 4", v)
	}
}

func TestStack(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Stack(df)
	if err != nil {
		t.Fatal(err)
	}

	// 2 rows * 2 cols = 4 rows, 3 cols (row, column, value)
	rows, cols := result.Shape()
	if rows != 4 {
		t.Errorf("rows = %d, want 4", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (row, column, value)", cols)
	}

	// Row 0: row=0, column=a, value=1
	v, _ := result.At(0, "row")
	if v != int64(0) {
		t.Errorf("row0 row = %v, want 0", v)
	}
	v, _ = result.At(0, "column")
	if v != "a" {
		t.Errorf("row0 column = %v, want a", v)
	}
	v, _ = result.At(0, "value")
	if v != int64(1) {
		t.Errorf("row0 value = %v, want 1", v)
	}

	// Row 3: row=1, column=b, value=4
	v, _ = result.At(3, "row")
	if v != int64(1) {
		t.Errorf("row3 row = %v, want 1", v)
	}
	v, _ = result.At(3, "column")
	if v != "b" {
		t.Errorf("row3 column = %v, want b", v)
	}
	v, _ = result.At(3, "value")
	if v != int64(4) {
		t.Errorf("row3 value = %v, want 4", v)
	}
}

func TestUnstack(t *testing.T) {
	// Create a stacked DataFrame with 3 columns: row, product, sales
	df, err := dataframe.New(map[string]any{
		"row":     []string{"0", "0", "1", "1"},
		"product": []string{"A", "B", "A", "B"},
		"sales":   []float64{100, 200, 150, 250},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Unstack(df, "product")
	if err != nil {
		t.Fatal(err)
	}

	// Should have: 2 rows, 3 cols (row, A, B)
	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (row, A, B)", cols)
	}

	// Row 0: row=0, A=100, B=200
	v, _ := result.At(0, "A")
	if v != float64(100) {
		t.Errorf("row0 A = %v, want 100", v)
	}
	v, _ = result.At(0, "B")
	if v != float64(200) {
		t.Errorf("row0 B = %v, want 200", v)
	}

	// Row 1: row=1, A=150, B=250
	v, _ = result.At(1, "A")
	if v != float64(150) {
		t.Errorf("row1 A = %v, want 150", v)
	}
	v, _ = result.At(1, "B")
	if v != float64(250) {
		t.Errorf("row1 B = %v, want 250", v)
	}
}

func TestUnstack_Error(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1},
		"b": []int64{2},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Need at least 3 columns
	_, err = Unstack(df, "a")
	if err == nil {
		t.Error("expected error for too few columns")
	}

	// Missing column
	df3, _ := dataframe.New(map[string]any{
		"a": []int64{1},
		"b": []int64{2},
		"c": []int64{3},
	})
	_, err = Unstack(df3, "missing")
	if err == nil {
		t.Error("expected error for missing column")
	}
}

func TestTranspose_SingleRow(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1},
		"b": []int64{2},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Transpose(df)
	if err != nil {
		t.Fatal(err)
	}

	// Original: 1 row, 2 cols (a, b)
	// Transposed: 2 rows (one per column), 2 cols (index, 0)
	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 2 {
		t.Errorf("cols = %d, want 2 (index, 0)", cols)
	}

	// Row 0: index=a, 0=1
	v, _ := result.At(0, "index")
	if v != "a" {
		t.Errorf("row0 index = %v, want a", v)
	}
	v, _ = result.At(0, "0")
	if v != int64(1) {
		t.Errorf("row0 col0 = %v, want 1", v)
	}

	// Row 1: index=b, 0=2
	v, _ = result.At(1, "index")
	if v != "b" {
		t.Errorf("row1 index = %v, want b", v)
	}
	v, _ = result.At(1, "0")
	if v != int64(2) {
		t.Errorf("row1 col0 = %v, want 2", v)
	}
}

func TestStack_SingleColumn(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1, 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Stack(df)
	if err != nil {
		t.Fatal(err)
	}

	// 2 rows * 1 col = 2 rows, 3 cols (row, column, value)
	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (row, column, value)", cols)
	}

	// Row 0: row=0, column=a, value=1
	v, _ := result.At(0, "row")
	if v != int64(0) {
		t.Errorf("row0 row = %v, want int64(0)", v)
	}
	v, _ = result.At(0, "column")
	if v != "a" {
		t.Errorf("row0 column = %v, want a", v)
	}
	v, _ = result.At(0, "value")
	if v != int64(1) {
		t.Errorf("row0 value = %v, want 1", v)
	}

	// Row 1: row=1, column=a, value=2
	v, _ = result.At(1, "row")
	if v != int64(1) {
		t.Errorf("row1 row = %v, want int64(1)", v)
	}
	v, _ = result.At(1, "column")
	if v != "a" {
		t.Errorf("row1 column = %v, want a", v)
	}
	v, _ = result.At(1, "value")
	if v != int64(2) {
		t.Errorf("row1 value = %v, want 2", v)
	}
}

func TestUnstack_ThreeColumns(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"row": []string{"r0", "r0", "r1", "r1"},
		"col": []string{"A", "B", "A", "B"},
		"val": []float64{10.0, 20.0, 30.0, 40.0},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Unstack(df, "col")
	if err != nil {
		t.Fatal(err)
	}

	// Should have: 2 rows, 3 cols (row, A, B)
	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (row, A, B)", cols)
	}

	// Row 0: A=10.0, B=20.0
	v, _ := result.At(0, "A")
	if v != float64(10) {
		t.Errorf("row0 A = %v, want 10", v)
	}
	v, _ = result.At(0, "B")
	if v != float64(20) {
		t.Errorf("row0 B = %v, want 20", v)
	}

	// Row 1: A=30.0, B=40.0
	v, _ = result.At(1, "A")
	if v != float64(30) {
		t.Errorf("row1 A = %v, want 30", v)
	}
	v, _ = result.At(1, "B")
	if v != float64(40) {
		t.Errorf("row1 B = %v, want 40", v)
	}
}
