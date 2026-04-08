package csv_test

import (
	"os"
	"testing"

	"github.com/vchitepu/gopandas/dataio/csv"
	"github.com/vchitepu/gopandas/dtype"
)

func TestFromCSV_File(t *testing.T) {
	f, err := os.Open("../../testdata/simple.csv")
	if err != nil {
		t.Fatalf("open testdata/simple.csv: %v", err)
	}
	defer f.Close()

	df, err := csv.FromCSV(f)
	if err != nil {
		t.Fatalf("FromCSV() error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3", cols)
	}

	// Verify column names
	names := df.Columns()
	wantNames := []string{"name", "age", "score"}
	for i, want := range wantNames {
		if names[i] != want {
			t.Errorf("Columns()[%d] = %q, want %q", i, names[i], want)
		}
	}

	// Verify types — DTypes() returns map[string]dtype.DType
	dtypes := df.DTypes()
	if dtypes["name"] != dtype.String {
		t.Errorf("name dtype = %v, want String", dtypes["name"])
	}
	if dtypes["age"] != dtype.Int64 {
		t.Errorf("age dtype = %v, want Int64", dtypes["age"])
	}
	if dtypes["score"] != dtype.Float64 {
		t.Errorf("score dtype = %v, want Float64", dtypes["score"])
	}

	// Verify values
	val, _ := df.At(0, "name")
	if val != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", val)
	}
	ageVal, _ := df.At(0, "age")
	if ageVal != int64(30) {
		t.Errorf("At(0, age) = %v, want 30", ageVal)
	}
	scoreVal, _ := df.At(0, "score")
	if scoreVal != 95.5 {
		t.Errorf("At(0, score) = %v, want 95.5", scoreVal)
	}
}
