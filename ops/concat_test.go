package ops

import (
	"testing"

	"github.com/vinaychitepu/gopandas/dataframe"
)

func TestConcat_TwoFrames(t *testing.T) {
	df1, err := dataframe.New(map[string]any{
		"a": []int64{1, 2},
		"b": []string{"x", "y"},
	})
	if err != nil {
		t.Fatal(err)
	}

	df2, err := dataframe.New(map[string]any{
		"a": []int64{3, 4},
		"b": []string{"z", "w"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Concat(df1, df2)
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 4 {
		t.Errorf("rows = %d, want 4", rows)
	}
	if cols != 2 {
		t.Errorf("cols = %d, want 2", cols)
	}

	v, _ := result.At(0, "a")
	if v != int64(1) {
		t.Errorf("row0 a = %v, want 1", v)
	}
	v, _ = result.At(2, "a")
	if v != int64(3) {
		t.Errorf("row2 a = %v, want 3", v)
	}
	v, _ = result.At(3, "b")
	if v != "w" {
		t.Errorf("row3 b = %v, want w", v)
	}
}

func TestConcat_ThreeFrames(t *testing.T) {
	df1, _ := dataframe.New(map[string]any{"a": []int64{1}})
	df2, _ := dataframe.New(map[string]any{"a": []int64{2}})
	df3, _ := dataframe.New(map[string]any{"a": []int64{3}})

	result, err := Concat(df1, df2, df3)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}

	v, _ := result.At(2, "a")
	if v != int64(3) {
		t.Errorf("row2 a = %v, want 3", v)
	}
}

func TestConcat_DifferentColumns(t *testing.T) {
	df1, _ := dataframe.New(map[string]any{
		"a": []int64{1},
		"b": []string{"x"},
	})
	df2, _ := dataframe.New(map[string]any{
		"b": []string{"y"},
		"c": []float64{3.0},
	})

	result, err := Concat(df1, df2)
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (a, b, c)", cols)
	}

	// df1 row: a=1, b=x, c=nil
	v, _ := result.At(0, "a")
	if v != int64(1) {
		t.Errorf("row0 a = %v, want 1", v)
	}
	v, _ = result.At(0, "c")
	if v != nil {
		t.Errorf("row0 c = %v, want nil", v)
	}

	// df2 row: a=nil, b=y, c=3.0
	v, _ = result.At(1, "a")
	if v != nil {
		t.Errorf("row1 a = %v, want nil", v)
	}
	v, _ = result.At(1, "c")
	if v != float64(3.0) {
		t.Errorf("row1 c = %v, want 3.0", v)
	}
}

func TestConcat_SingleFrame(t *testing.T) {
	df1, _ := dataframe.New(map[string]any{"a": []int64{1, 2}})

	result, err := Concat(df1)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
}

func TestConcat_NoFrames(t *testing.T) {
	_, err := Concat()
	if err == nil {
		t.Error("expected error for no frames")
	}
}
