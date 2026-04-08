package json_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	gojson "github.com/vinaychitepu/gopandas/dataio/json"
)

func TestToJSON_Records(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25}
	]`

	df, err := gojson.FromJSON(strings.NewReader(input), gojson.OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON error: %v", err)
	}

	var buf bytes.Buffer
	if err := gojson.ToJSON(df, &buf, gojson.OrientRecords); err != nil {
		t.Fatalf("ToJSON(records) error: %v", err)
	}

	var records []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &records); err != nil {
		t.Fatalf("output is not valid JSON array: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
}

func TestToJSON_Columns(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25}
	]`

	df, err := gojson.FromJSON(strings.NewReader(input), gojson.OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON error: %v", err)
	}

	var buf bytes.Buffer
	if err := gojson.ToJSON(df, &buf, gojson.OrientColumns); err != nil {
		t.Fatalf("ToJSON(columns) error: %v", err)
	}

	var columns map[string][]any
	if err := json.Unmarshal(buf.Bytes(), &columns); err != nil {
		t.Fatalf("output is not valid JSON map: %v", err)
	}
	if len(columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(columns))
	}
}

func TestToJSON_Index(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25}
	]`

	df, err := gojson.FromJSON(strings.NewReader(input), gojson.OrientRecords)
	if err != nil {
		t.Fatalf("FromJSON error: %v", err)
	}

	var buf bytes.Buffer
	if err := gojson.ToJSON(df, &buf, gojson.OrientIndex); err != nil {
		t.Fatalf("ToJSON(index) error: %v", err)
	}

	var indexed map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &indexed); err != nil {
		t.Fatalf("output is not valid JSON map of maps: %v", err)
	}
	if len(indexed) != 2 {
		t.Errorf("expected 2 index entries, got %d", len(indexed))
	}
}

func TestToJSON_RoundTrip_Records(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30, "city": "NYC"},
		{"name": "Bob", "age": 25, "city": "LA"},
		{"name": "Charlie", "age": 35, "city": "CHI"}
	]`

	// Read
	df1, err := gojson.FromJSON(strings.NewReader(input), gojson.OrientRecords)
	if err != nil {
		t.Fatalf("first FromJSON error: %v", err)
	}

	// Write
	var buf bytes.Buffer
	if err := gojson.ToJSON(df1, &buf, gojson.OrientRecords); err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}

	// Read again
	df2, err := gojson.FromJSON(strings.NewReader(buf.String()), gojson.OrientRecords)
	if err != nil {
		t.Fatalf("second FromJSON error: %v", err)
	}

	r1, c1 := df1.Shape()
	r2, c2 := df2.Shape()
	if r1 != r2 || c1 != c2 {
		t.Errorf("shape mismatch: original (%d,%d) vs roundtrip (%d,%d)", r1, c1, r2, c2)
	}
}
