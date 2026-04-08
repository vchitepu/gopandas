package parquet

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/vinaychitepu/gopandas/dataframe"
	"github.com/vinaychitepu/gopandas/dtype"
)

// ToParquet writes the given DataFrame to the writer in Parquet format.
func ToParquet(df dataframe.DataFrame, w io.Writer) error {
	nRows, _ := df.Shape()
	columns := df.Columns()
	dtypes := df.DTypes()

	fields := make([]arrow.Field, len(columns))
	for i, name := range columns {
		arrowDT, err := dtype.DTypeToArrow(dtypes[name])
		if err != nil {
			return fmt.Errorf("parquet write: column %q: %w", name, err)
		}
		fields[i] = arrow.Field{Name: name, Type: arrowDT, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)

	arrays := make([]arrow.Array, len(columns))
	for i, name := range columns {
		col, err := df.Col(name)
		if err != nil {
			return fmt.Errorf("parquet write: %w", err)
		}
		arr := col.Array()
		arr.Retain()
		arrays[i] = arr
	}

	rec := array.NewRecord(schema, arrays, int64(nRows))
	defer rec.Release()
	for _, arr := range arrays {
		arr.Release()
	}

	writer, err := pqarrow.NewFileWriter(schema, w, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("parquet writer create: %w", err)
	}

	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("parquet write record: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("parquet writer close: %w", err)
	}

	return nil
}
