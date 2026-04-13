package ops

import (
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func TestMerge_InnerJoin(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 2, 3},
		"valL": []string{"a", "b", "c"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{2, 3, 4},
		"valR": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Inner)
	if err != nil {
		t.Fatal(err)
	}

	rows, cols := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3 (id, valL, valR)", cols)
	}

	// Row 0: id=2, valL=b, valR=x
	v, _ := result.At(0, "id")
	if v != int64(2) {
		t.Errorf("row0 id = %v, want 2", v)
	}
	v, _ = result.At(0, "valL")
	if v != "b" {
		t.Errorf("row0 valL = %v, want b", v)
	}
	v, _ = result.At(0, "valR")
	if v != "x" {
		t.Errorf("row0 valR = %v, want x", v)
	}

	// Row 1: id=3, valL=c, valR=y
	v, _ = result.At(1, "id")
	if v != int64(3) {
		t.Errorf("row1 id = %v, want 3", v)
	}
	v, _ = result.At(1, "valL")
	if v != "c" {
		t.Errorf("row1 valL = %v, want c", v)
	}
	v, _ = result.At(1, "valR")
	if v != "y" {
		t.Errorf("row1 valR = %v, want y", v)
	}
}

func TestMerge_InnerJoin_NoMatch(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 2},
		"valL": []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{3, 4},
		"valR": []string{"x", "y"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Inner)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 0 {
		t.Errorf("rows = %d, want 0 (no match)", rows)
	}
}

func TestMerge_LeftJoin(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 2, 3},
		"valL": []string{"a", "b", "c"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{2, 4},
		"valR": []string{"x", "z"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Left)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}

	// Row 0: id=1, valL=a, valR=nil
	v, _ := result.At(0, "id")
	if v != int64(1) {
		t.Errorf("row0 id = %v, want 1", v)
	}
	v, _ = result.At(0, "valL")
	if v != "a" {
		t.Errorf("row0 valL = %v, want a", v)
	}
	v, _ = result.At(0, "valR")
	if v != nil {
		t.Errorf("row0 valR = %v, want nil", v)
	}

	// Row 1: id=2, valL=b, valR=x
	v, _ = result.At(1, "valR")
	if v != "x" {
		t.Errorf("row1 valR = %v, want x", v)
	}

	// Row 2: id=3, valL=c, valR=nil
	v, _ = result.At(2, "valR")
	if v != nil {
		t.Errorf("row2 valR = %v, want nil", v)
	}
}

func TestMerge_RightJoin(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 2},
		"valL": []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{2, 3, 4},
		"valR": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Right)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}

	// Row 0: id=2, valL=b, valR=x (match)
	v, _ := result.At(0, "id")
	if v != int64(2) {
		t.Errorf("row0 id = %v, want 2", v)
	}
	v, _ = result.At(0, "valL")
	if v != "b" {
		t.Errorf("row0 valL = %v, want b", v)
	}

	// Row 1: id=3, valL=nil, valR=y (right only)
	v, _ = result.At(1, "id")
	if v != int64(3) {
		t.Errorf("row1 id = %v, want 3", v)
	}
	v, _ = result.At(1, "valL")
	if v != nil {
		t.Errorf("row1 valL = %v, want nil", v)
	}

	// Row 2: id=4, valL=nil, valR=z (right only)
	v, _ = result.At(2, "valL")
	if v != nil {
		t.Errorf("row2 valL = %v, want nil", v)
	}
}

func TestMerge_OuterJoin(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 2},
		"valL": []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{2, 3},
		"valR": []string{"x", "y"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Outer)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}

	// Row 0: id=1, valL=a, valR=nil (left only)
	v, _ := result.At(0, "valR")
	if v != nil {
		t.Errorf("row0 valR = %v, want nil", v)
	}

	// Row 1: id=2, valL=b, valR=x (match)
	v, _ = result.At(1, "valL")
	if v != "b" {
		t.Errorf("row1 valL = %v, want b", v)
	}
	v, _ = result.At(1, "valR")
	if v != "x" {
		t.Errorf("row1 valR = %v, want x", v)
	}

	// Row 2: id=3, valL=nil, valR=y (right only)
	v, _ = result.At(2, "valL")
	if v != nil {
		t.Errorf("row2 valL = %v, want nil", v)
	}
	v, _ = result.At(2, "valR")
	if v != "y" {
		t.Errorf("row2 valR = %v, want y", v)
	}
}

func TestMerge_MultiKey(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"k1":   []string{"a", "a", "b"},
		"k2":   []int64{1, 2, 1},
		"valL": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"k1":   []string{"a", "b"},
		"k2":   []int64{2, 1},
		"valR": []string{"m", "n"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"k1", "k2"}, Inner)
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}

	// Row 0: k1=a, k2=2, valL=y, valR=m
	v, _ := result.At(0, "valL")
	if v != "y" {
		t.Errorf("row0 valL = %v, want y", v)
	}
	v, _ = result.At(0, "valR")
	if v != "m" {
		t.Errorf("row0 valR = %v, want m", v)
	}

	// Row 1: k1=b, k2=1, valL=z, valR=n
	v, _ = result.At(1, "valL")
	if v != "z" {
		t.Errorf("row1 valL = %v, want z", v)
	}
	v, _ = result.At(1, "valR")
	if v != "n" {
		t.Errorf("row1 valR = %v, want n", v)
	}
}

func TestMerge_DuplicateKeys(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id":   []int64{1, 1},
		"valL": []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id":   []int64{1, 1},
		"valR": []string{"x", "y"},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := Merge(left, right, []string{"id"}, Inner)
	if err != nil {
		t.Fatal(err)
	}

	// 2 left * 2 right = 4 rows (cartesian product for duplicate keys)
	rows, _ := result.Shape()
	if rows != 4 {
		t.Errorf("rows = %d, want 4", rows)
	}
}

func TestMerge_InvalidColumn(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Merge(left, right, []string{"missing"}, Inner)
	if err == nil {
		t.Error("expected error for missing column")
	}
}

func TestMerge_EmptyOn(t *testing.T) {
	left, err := dataframe.New(map[string]any{
		"id": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	right, err := dataframe.New(map[string]any{
		"id": []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Merge(left, right, []string{}, Inner)
	if err == nil {
		t.Error("expected error for empty on")
	}
}
