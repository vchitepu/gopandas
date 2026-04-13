package parquet_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/dataio/parquet"
)

func TestToParquet_Simple(t *testing.T) {
	alloc := memory.DefaultAllocator

	// Build a DataFrame from Arrow
	nameBldr := array.NewStringBuilder(alloc)
	defer nameBldr.Release()
	nameBldr.AppendValues([]string{"Alice", "Bob"}, nil)
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()

	ageBldr := array.NewInt64Builder(alloc)
	defer ageBldr.Release()
	ageBldr.AppendValues([]int64{30, 25}, nil)
	ageArr := ageBldr.NewArray()
	defer ageArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{nameArr, ageArr}, 2)
	defer rec.Release()

	df, err := dataframe.FromArrow(rec)
	if err != nil {
		t.Fatalf("FromArrow: %v", err)
	}

	// Write to Parquet
	var buf bytes.Buffer
	if err := parquet.ToParquet(df, &buf); err != nil {
		t.Fatalf("ToParquet: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("ToParquet produced empty output")
	}
}

func TestToParquet_RoundTrip(t *testing.T) {
	// Step 1: Create a test Parquet buffer
	testBuf := writeTestParquet(t)

	// Step 2: Read with FromParquet
	df1, err := parquet.FromParquet(bytes.NewReader(testBuf.Bytes()))
	if err != nil {
		t.Fatalf("FromParquet (first read): %v", err)
	}

	// Step 3: Write with ToParquet
	var writeBuf bytes.Buffer
	if err := parquet.ToParquet(df1, &writeBuf); err != nil {
		t.Fatalf("ToParquet: %v", err)
	}

	// Step 4: Read again with FromParquet
	df2, err := parquet.FromParquet(bytes.NewReader(writeBuf.Bytes()))
	if err != nil {
		t.Fatalf("FromParquet (second read): %v", err)
	}

	// Step 5: Verify shape matches
	rows1, cols1 := df1.Shape()
	rows2, cols2 := df2.Shape()
	if rows1 != rows2 || cols1 != cols2 {
		t.Errorf("Shape mismatch: (%d, %d) vs (%d, %d)", rows1, cols1, rows2, cols2)
	}

	// Step 6: Verify column names match
	columns1 := df1.Columns()
	columns2 := df2.Columns()
	for i := range columns1 {
		if columns1[i] != columns2[i] {
			t.Errorf("Column[%d] mismatch: %q vs %q", i, columns1[i], columns2[i])
		}
	}

	// Step 7: Verify values match
	for _, col := range columns1 {
		for r := 0; r < rows1; r++ {
			v1, err := df1.At(r, col)
			if err != nil {
				t.Fatalf("df1.At(%d, %q): %v", r, col, err)
			}
			v2, err := df2.At(r, col)
			if err != nil {
				t.Fatalf("df2.At(%d, %q): %v", r, col, err)
			}
			if v1 != v2 {
				t.Errorf("At(%d, %q): %v != %v", r, col, v1, v2)
			}
		}
	}
}
