package parquet

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/vchitepu/gopandas/dataframe"
)

// FromParquet reads a Parquet file from the given ReaderAtSeeker and returns a DataFrame.
// The reader must support both ReadAt and Seek (e.g. *bytes.Reader, *os.File).
func FromParquet(r pq.ReaderAtSeeker) (dataframe.DataFrame, error) {
	pf, err := file.NewParquetReader(r)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("parquet open: %w", err)
	}
	defer pf.Close()

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 0,
	}, memory.DefaultAllocator)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("parquet arrow reader: %w", err)
	}

	tbl, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("parquet read table: %w", err)
	}
	defer tbl.Release()

	// Use a batch size equal to the total rows to consolidate all row groups
	// into a single record. This ensures multi-row-group files are fully read.
	batchSize := tbl.NumRows()
	if batchSize == 0 {
		// Empty table — build empty record preserving schema
		schema := tbl.Schema()
		emptyArrays := make([]arrow.Array, schema.NumFields())
		for i, field := range schema.Fields() {
			bldr := array.NewBuilder(memory.DefaultAllocator, field.Type)
			emptyArrays[i] = bldr.NewArray()
			bldr.Release()
		}
		rec := array.NewRecord(schema, emptyArrays, 0)
		for _, a := range emptyArrays {
			a.Release()
		}
		defer rec.Release()
		return dataframe.FromArrow(rec)
	}

	tr := array.NewTableReader(tbl, batchSize)
	defer tr.Release()

	if !tr.Next() {
		return dataframe.DataFrame{}, fmt.Errorf("parquet: unexpected empty batch from non-empty table")
	}

	rec := tr.Record()
	rec.Retain()
	defer rec.Release()

	return dataframe.FromArrow(rec)
}
