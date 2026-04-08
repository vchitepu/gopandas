package parquet_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/vinaychitepu/gopandas/dataio/parquet"
)

// writeTestParquet creates a Parquet buffer with 3 columns (name, age, score) and 3 rows.
func writeTestParquet(t *testing.T) *bytes.Buffer {
	t.Helper()
	alloc := memory.DefaultAllocator

	// Build Arrow arrays
	nameBldr := array.NewStringBuilder(alloc)
	defer nameBldr.Release()
	nameBldr.AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()

	ageBldr := array.NewInt64Builder(alloc)
	defer ageBldr.Release()
	ageBldr.AppendValues([]int64{30, 25, 35}, nil)
	ageArr := ageBldr.NewArray()
	defer ageArr.Release()

	scoreBldr := array.NewFloat64Builder(alloc)
	defer scoreBldr.Release()
	scoreBldr.AppendValues([]float64{9.5, 8.0, 7.5}, nil)
	scoreArr := scoreBldr.NewArray()
	defer scoreArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{nameArr, ageArr, scoreArr}, 3)
	defer rec.Release()

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("create parquet writer: %v", err)
	}
	if err := writer.Write(rec); err != nil {
		t.Fatalf("write parquet record: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}

	return &buf
}

// writeEmptyParquet creates a Parquet buffer with a schema but no rows.
func writeEmptyParquet(t *testing.T) *bytes.Buffer {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("create parquet writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}

	return &buf
}

func TestFromParquet_Simple(t *testing.T) {
	buf := writeTestParquet(t)

	df, err := parquet.FromParquet(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("FromParquet: %v", err)
	}

	// Verify shape: 3 rows, 3 columns
	rows, cols := df.Shape()
	if rows != 3 || cols != 3 {
		t.Errorf("Shape = (%d, %d), want (3, 3)", rows, cols)
	}

	// Verify column names
	columns := df.Columns()
	wantCols := []string{"name", "age", "score"}
	if len(columns) != len(wantCols) {
		t.Fatalf("Columns = %v, want %v", columns, wantCols)
	}
	for i, c := range columns {
		if c != wantCols[i] {
			t.Errorf("Column[%d] = %q, want %q", i, c, wantCols[i])
		}
	}

	// Verify values
	nameVal, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name): %v", err)
	}
	if nameVal != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", nameVal)
	}

	ageVal, err := df.At(1, "age")
	if err != nil {
		t.Fatalf("At(1, age): %v", err)
	}
	if ageVal != int64(25) {
		t.Errorf("At(1, age) = %v, want 25", ageVal)
	}

	scoreVal, err := df.At(2, "score")
	if err != nil {
		t.Fatalf("At(2, score): %v", err)
	}
	if scoreVal != 7.5 {
		t.Errorf("At(2, score) = %v, want 7.5", scoreVal)
	}
}

func TestFromParquet_Empty(t *testing.T) {
	buf := writeEmptyParquet(t)

	df, err := parquet.FromParquet(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("FromParquet: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 0 || cols != 1 {
		t.Errorf("Shape = (%d, %d), want (0, 1)", rows, cols)
	}
}
