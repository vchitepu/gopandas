package json_test

import (
	"os"
	"testing"

	gojson "github.com/vchitepu/gopandas/dataio/json"
)

func TestFromJSON_File(t *testing.T) {
	f, err := os.Open("../../testdata/simple.json")
	if err != nil {
		t.Fatalf("open testdata/simple.json: %v", err)
	}
	defer f.Close()

	df, err := gojson.FromJSON(f, gojson.OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON() error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}
	if cols != 3 {
		t.Errorf("cols = %d, want 3", cols)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name) error: %v", err)
	}
	if val != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", val)
	}
}
