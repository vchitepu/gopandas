package ops

import (
	"testing"

	"github.com/vchitepu/gopandas/dataframe"
)

func TestJoin_Inner(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []string{"a", "b", "c"},
		"valL": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	left, err = left.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []string{"b", "c", "d"},
		"valR": []int64{10, 20, 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err = right.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	result, err := Join(left, right, Inner, "", "")
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 2 {
		t.Errorf("cols = %d, want 2 (valL, valR)", cols)
	}

	// Row 0: valL=2, valR=10
	v, _ := result.At(0, "valL")
	if v != int64(2) {
		t.Errorf("row0 valL = %v, want 2", v)
	}
	v, _ = result.At(0, "valR")
	if v != int64(10) {
		t.Errorf("row0 valR = %v, want 10", v)
	}

	// Row 1: valL=3, valR=20
	v, _ = result.At(1, "valL")
	if v != int64(3) {
		t.Errorf("row1 valL = %v, want 3", v)
	}
	v, _ = result.At(1, "valR")
	if v != int64(20) {
		t.Errorf("row1 valR = %v, want 20", v)
	}
}

func TestJoin_Left(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []string{"a", "b", "c"},
		"valL": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	left, err = left.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []string{"b", "d"},
		"valR": []int64{10, 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err = right.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	result, err := Join(left, right, Left, "", "")
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}

	// Row 0: valL=1, valR=nil (a not in right)
	v, _ := result.At(0, "valR")
	if v != nil {
		t.Errorf("row0 valR = %v, want nil", v)
	}

	// Row 1: valL=2, valR=10 (b matches)
	v, _ = result.At(1, "valR")
	if v != int64(10) {
		t.Errorf("row1 valR = %v, want 10", v)
	}

	// Row 2: valL=3, valR=nil (c not in right)
	v, _ = result.At(2, "valR")
	if v != nil {
		t.Errorf("row2 valR = %v, want nil", v)
	}
}

func TestJoin_ColumnNameConflict(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":  []string{"a", "b"},
		"val": []int64{1, 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	left, err = left.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":  []string{"a", "b"},
		"val": []int64{10, 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err = right.SetIndex("id")
	if err != nil {
		t.Fatal(err)
	}

	result, err := Join(left, right, Inner, "_left", "_right")
	if err != nil {
		t.Fatal(err)
	}

	cols := result.Columns()
	colSet := make(map[string]bool)
	for _, c := range cols {
		colSet[c] = true
	}

	if !colSet["val_left"] {
		t.Errorf("expected column val_left, got %v", cols)
	}
	if !colSet["val_right"] {
		t.Errorf("expected column val_right, got %v", cols)
	}

	// Check values
	v, _ := result.At(0, "val_left")
	if v != int64(1) {
		t.Errorf("row0 val_left = %v, want 1", v)
	}
	v, _ = result.At(0, "val_right")
	if v != int64(10) {
		t.Errorf("row0 val_right = %v, want 10", v)
	}
}
