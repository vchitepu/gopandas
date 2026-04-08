package csv

import (
	"strings"
	"testing"
	"time"

	"github.com/vchitepu/gopandas/dtype"
)

func TestFromCSV_Simple(t *testing.T) {
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\nCharlie,35,92.3\n"
	df, err := FromCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 3 {
		t.Fatalf("Shape = (%d, %d), want (3, 3)", rows, cols)
	}

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

	// Spot check a value
	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name): %v", err)
	}
	if val != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", val)
	}

	val, err = df.At(1, "age")
	if err != nil {
		t.Fatalf("At(1, age): %v", err)
	}
	if val != int64(25) {
		t.Errorf("At(1, age) = %v, want 25", val)
	}

	val, err = df.At(2, "score")
	if err != nil {
		t.Fatalf("At(2, score): %v", err)
	}
	if val != 92.3 {
		t.Errorf("At(2, score) = %v, want 92.3", val)
	}
}

func TestFromCSV_WithParseDates(t *testing.T) {
	input := "Date,Description,Amount\n12/31/2025,Coffee,3.25\n12/30/2025,Lunch,15.00\n"
	df, err := FromCSV(strings.NewReader(input), WithParseDates([]string{"Date"}))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	dtypes := df.DTypes()
	if dtypes["Date"] != dtype.Timestamp {
		t.Fatalf("Date dtype = %v, want Timestamp", dtypes["Date"])
	}

	val, err := df.At(0, "Date")
	if err != nil {
		t.Fatalf("At(0, Date): %v", err)
	}

	tm, ok := val.(time.Time)
	if !ok {
		t.Fatalf("Date value type = %T, want time.Time", val)
	}

	want := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	if !tm.Equal(want) {
		t.Fatalf("Date value = %v, want %v", tm, want)
	}
}

func TestFromCSV_WithDateFormats(t *testing.T) {
	input := "Date,Description\n2025-12-31,Coffee\n2025-12-30,Lunch\n"
	df, err := FromCSV(strings.NewReader(input),
		WithParseDates([]string{"Date"}),
		WithDateFormats([]string{"2006-01-02"}),
	)
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	dtypes := df.DTypes()
	if dtypes["Date"] != dtype.Timestamp {
		t.Fatalf("Date dtype = %v, want Timestamp", dtypes["Date"])
	}
}

func TestFromCSV_ParseDatesFallbackToString(t *testing.T) {
	input := "Date,Description\nnot-a-date,Coffee\nstill-not-a-date,Lunch\n"
	df, err := FromCSV(strings.NewReader(input), WithParseDates([]string{"Date"}))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	dtypes := df.DTypes()
	if dtypes["Date"] != dtype.String {
		t.Fatalf("Date dtype = %v, want String fallback", dtypes["Date"])
	}
}

func TestFromCSV_WithSep(t *testing.T) {
	input := "name\tage\tscore\nAlice\t30\t95.5\nBob\t25\t88.0\n"
	df, err := FromCSV(strings.NewReader(input), WithSep('\t'))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 3 {
		t.Fatalf("Shape = (%d, %d), want (2, 3)", rows, cols)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", val)
	}
}

func TestFromCSV_NoHeader(t *testing.T) {
	input := "Alice,30,95.5\nBob,25,88.0\n"
	df, err := FromCSV(strings.NewReader(input), WithHeader(false))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 3 {
		t.Fatalf("Shape = (%d, %d), want (2, 3)", rows, cols)
	}

	// Column names should be "0", "1", "2"
	colNames := df.Columns()
	expected := []string{"0", "1", "2"}
	for i, want := range expected {
		if colNames[i] != want {
			t.Errorf("column %d = %q, want %q", i, colNames[i], want)
		}
	}

	val, err := df.At(0, "0")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Alice" {
		t.Errorf("At(0, '0') = %v, want Alice", val)
	}
}

func TestFromCSV_WithNRows(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\nDave,40\n"
	df, err := FromCSV(strings.NewReader(input), WithNRows(2))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}

	val, err := df.At(1, "name")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Bob" {
		t.Errorf("At(1, name) = %v, want Bob", val)
	}
}

func TestFromCSV_WithSkipRows(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\nDave,40\n"
	df, err := FromCSV(strings.NewReader(input), WithSkipRows(2))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Charlie" {
		t.Errorf("At(0, name) = %v, want Charlie", val)
	}
}

func TestFromCSV_WithUseCols(t *testing.T) {
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\n"
	df, err := FromCSV(strings.NewReader(input), WithUseCols([]string{"name", "score"}))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	_, cols := df.Shape()
	if cols != 2 {
		t.Fatalf("cols = %d, want 2", cols)
	}

	colNames := df.Columns()
	if colNames[0] != "name" || colNames[1] != "score" {
		t.Errorf("columns = %v, want [name score]", colNames)
	}

	// "age" should not exist
	_, err = df.At(0, "age")
	if err == nil {
		t.Error("expected error accessing 'age' column, got nil")
	}
}

func TestFromCSV_WithNAValues(t *testing.T) {
	// age column has "NA" → int column with null → should promote to float64
	input := "name,age,score\nAlice,30,95.5\nBob,NA,88.0\nCharlie,35,92.3\n"
	df, err := FromCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	dtypes := df.DTypes()
	// age has an NA value, so all-int with NA → float64
	if dtypes["age"] != dtype.Float64 {
		t.Errorf("age dtype = %v, want Float64 (int col with NA promotes to float64)", dtypes["age"])
	}

	// Check that the non-null values are correct
	val, err := df.At(0, "age")
	if err != nil {
		t.Fatalf("At(0, age): %v", err)
	}
	if val != float64(30) {
		t.Errorf("At(0, age) = %v, want 30.0", val)
	}

	// Check that the null is actually null
	col, err := df.Col("age")
	if err != nil {
		t.Fatalf("Col(age): %v", err)
	}
	if !col.IsNull(1) {
		t.Error("expected age[1] to be null")
	}
}

func TestFromCSV_WithNAValues_Custom(t *testing.T) {
	input := "name,age\nAlice,30\nBob,MISSING\nCharlie,35\n"
	df, err := FromCSV(strings.NewReader(input), WithNAValues([]string{"MISSING"}))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	col, err := df.Col("age")
	if err != nil {
		t.Fatalf("Col(age): %v", err)
	}
	if !col.IsNull(1) {
		t.Error("expected age[1] to be null with custom NA value")
	}
}

func TestFromCSV_WithDTypeOverride(t *testing.T) {
	// age would normally be int64, but we force it to string
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\n"
	df, err := FromCSV(strings.NewReader(input), WithDTypeOverride("age", dtype.String))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	dtypes := df.DTypes()
	if dtypes["age"] != dtype.String {
		t.Errorf("age dtype = %v, want String (overridden)", dtypes["age"])
	}

	val, err := df.At(0, "age")
	if err != nil {
		t.Fatalf("At(0, age): %v", err)
	}
	if val != "30" {
		t.Errorf("At(0, age) = %v, want '30' (string)", val)
	}
}

func TestFromCSV_WithIndexCol(t *testing.T) {
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\nCharlie,35,92.3\n"
	df, err := FromCSV(strings.NewReader(input), WithIndexCol("name"))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	// name should be the index, not a data column
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Fatalf("Shape = (%d, %d), want (3, 2)", rows, cols)
	}

	// Check index labels
	labels := df.Index().Labels()
	expectedLabels := []any{"Alice", "Bob", "Charlie"}
	for i, want := range expectedLabels {
		if labels[i] != want {
			t.Errorf("index label[%d] = %v, want %v", i, labels[i], want)
		}
	}

	// "name" should not be a data column
	_, err = df.Col("name")
	if err == nil {
		t.Error("expected error accessing 'name' as data column, got nil")
	}

	// Data columns should be age and score
	colNames := df.Columns()
	if len(colNames) != 2 {
		t.Fatalf("columns = %v, want 2 columns", colNames)
	}

	// Use Loc to access by label
	val, err := df.Loc("Alice", "age")
	if err != nil {
		t.Fatalf("Loc(Alice, age): %v", err)
	}
	if val != int64(30) {
		t.Errorf("Loc(Alice, age) = %v, want 30", val)
	}
}

func TestFromCSV_SkipRowsAndNRows(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\nDave,40\nEve,28\n"
	df, err := FromCSV(strings.NewReader(input), WithSkipRows(1), WithNRows(2))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Bob" {
		t.Errorf("At(0, name) = %v, want Bob", val)
	}

	val, err = df.At(1, "name")
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	if val != "Charlie" {
		t.Errorf("At(1, name) = %v, want Charlie", val)
	}
}
