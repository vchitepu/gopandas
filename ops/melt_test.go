package ops

import (
	"testing"

	"github.com/vchitepu/gopandas/dataframe"
)

func TestMelt_Basic(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"name":  []string{"Alice", "Bob"},
		"math":  []float64{90, 80},
		"music": []float64{85, 75},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Melt(df, []string{"name"}, []string{"math", "music"}, "subject", "score")
	if err != nil {
		t.Fatal(err)
	}

	// 2 rows * 2 value vars = 4 rows
	rows, cols := result.Shape()
	if rows != 4 {
		t.Errorf("rows = %d, want 4", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (name, subject, score)", cols)
	}

	// Row 0: name=Alice, subject=math, score=90
	v, _ := result.At(0, "name")
	if v != "Alice" {
		t.Errorf("row0 name = %v, want Alice", v)
	}
	v, _ = result.At(0, "subject")
	if v != "math" {
		t.Errorf("row0 subject = %v, want math", v)
	}
	v, _ = result.At(0, "score")
	if v != float64(90) {
		t.Errorf("row0 score = %v, want 90", v)
	}

	// Row 1: name=Bob, subject=math, score=80
	v, _ = result.At(1, "name")
	if v != "Bob" {
		t.Errorf("row1 name = %v, want Bob", v)
	}
	v, _ = result.At(1, "score")
	if v != float64(80) {
		t.Errorf("row1 score = %v, want 80", v)
	}

	// Row 2: name=Alice, subject=music, score=85
	v, _ = result.At(2, "subject")
	if v != "music" {
		t.Errorf("row2 subject = %v, want music", v)
	}
	v, _ = result.At(2, "score")
	if v != float64(85) {
		t.Errorf("row2 score = %v, want 85", v)
	}

	// Row 3: name=Bob, subject=music, score=75
	v, _ = result.At(3, "score")
	if v != float64(75) {
		t.Errorf("row3 score = %v, want 75", v)
	}
}

func TestMelt_MultipleIdVars(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"first": []string{"Alice", "Bob"},
		"last":  []string{"A", "B"},
		"val":   []float64{1, 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Melt(df, []string{"first", "last"}, []string{"val"}, "variable", "value")
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 4 {
		t.Errorf("cols = %d, want 4 (first, last, variable, value)", cols)
	}

	v, _ := result.At(0, "first")
	if v != "Alice" {
		t.Errorf("row0 first = %v, want Alice", v)
	}
	v, _ = result.At(0, "last")
	if v != "A" {
		t.Errorf("row0 last = %v, want A", v)
	}
}

func TestMelt_DefaultNames(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"id":  []int64{1},
		"val": []float64{10},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Melt(df, []string{"id"}, []string{"val"}, "", "")
	if err != nil {
		t.Fatal(err)
	}

	cols := result.Columns()
	colSet := make(map[string]bool)
	for _, c := range cols {
		colSet[c] = true
	}

	if !colSet["variable"] {
		t.Errorf("expected 'variable' column, got %v", cols)
	}
	if !colSet["value"] {
		t.Errorf("expected 'value' column, got %v", cols)
	}
}

func TestMelt_MissingColumn(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"a": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Melt(df, []string{"missing"}, []string{"a"}, "var", "val")
	if err == nil {
		t.Error("expected error for missing id column")
	}

	_, err = Melt(df, []string{"a"}, []string{"missing"}, "var", "val")
	if err == nil {
		t.Error("expected error for missing value column")
	}
}
