package json

import (
	"strings"
	"testing"

	"github.com/vchitepu/gopandas/lib/dtype"
)

func TestFromJSON_Records(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30, "city": "NYC"},
		{"name": "Bob", "age": 25, "city": "LA"},
		{"name": "Charlie", "age": 35, "city": "CHI"}
	]`

	df, err := FromJSON(strings.NewReader(input), OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON(records) error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 3 {
		t.Fatalf("expected shape (3,3), got (%d,%d)", rows, cols)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name) error: %v", err)
	}
	if val != "Alice" {
		t.Errorf("expected At(0, name) = Alice, got %v", val)
	}
}

func TestFromJSON_Records_TypeInference(t *testing.T) {
	input := `[
		{"x": 1, "y": 2.5, "z": "hello"},
		{"x": 2, "y": 3.5, "z": "world"}
	]`

	df, err := FromJSON(strings.NewReader(input), OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON(records) error: %v", err)
	}

	dtypes := df.DTypes()

	// JSON numbers all unmarshal to float64 in Go
	if dtypes["x"] != dtype.Float64 {
		t.Errorf("expected x dtype Float64, got %v", dtypes["x"])
	}
	if dtypes["y"] != dtype.Float64 {
		t.Errorf("expected y dtype Float64, got %v", dtypes["y"])
	}
}

func TestFromJSON_Columns(t *testing.T) {
	input := `{
		"name": ["Alice", "Bob", "Charlie"],
		"age": [30, 25, 35]
	}`

	df, err := FromJSON(strings.NewReader(input), OrientColumns)
	if err != nil {
		t.Fatalf("FromJSON(columns) error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("expected shape (3,2), got (%d,%d)", rows, cols)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name) error: %v", err)
	}
	if val != "Alice" {
		t.Errorf("expected At(0, name) = Alice, got %v", val)
	}
}

func TestFromJSON_Index(t *testing.T) {
	input := `{
		"row0": {"name": "Alice", "age": 30},
		"row1": {"name": "Bob", "age": 25},
		"row2": {"name": "Charlie", "age": 35}
	}`

	df, err := FromJSON(strings.NewReader(input), OrientIndex)
	if err != nil {
		t.Fatalf("FromJSON(index) error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("expected shape (3,2), got (%d,%d)", rows, cols)
	}
}

func TestFromJSON_Empty_Records(t *testing.T) {
	input := `[]`

	df, err := FromJSON(strings.NewReader(input), OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON(empty records) error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Fatalf("expected shape (0,0), got (%d,%d)", rows, cols)
	}
}
