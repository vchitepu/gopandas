package json

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

// ToJSON writes the DataFrame to w in JSON format.
// The orient parameter specifies the output JSON structure.
func ToJSON(df dataframe.DataFrame, w io.Writer, orient JSONOrient) error {
	switch orient {
	case OrientRecords:
		return toRecords(df, w)
	case OrientColumns:
		return toColumns(df, w)
	case OrientIndex:
		return toIndex(df, w)
	default:
		return fmt.Errorf("json.ToJSON: unsupported orient %v", orient)
	}
}

// toRecords writes JSON in records orient: [{col: val, ...}, ...]
func toRecords(df dataframe.DataFrame, w io.Writer) error {
	nRows, _ := df.Shape()
	columns := df.Columns()

	records := make([]map[string]any, nRows)
	for i := 0; i < nRows; i++ {
		row := make(map[string]any, len(columns))
		for _, col := range columns {
			val, err := df.At(i, col)
			if err != nil {
				return fmt.Errorf("json.ToJSON(records): %w", err)
			}
			row[col] = val
		}
		records[i] = row
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

// toColumns writes JSON in columns orient: {col: [val, ...], ...}
func toColumns(df dataframe.DataFrame, w io.Writer) error {
	nRows, _ := df.Shape()
	columns := df.Columns()

	result := make(map[string][]any, len(columns))
	for _, col := range columns {
		vals := make([]any, nRows)
		for i := 0; i < nRows; i++ {
			val, err := df.At(i, col)
			if err != nil {
				return fmt.Errorf("json.ToJSON(columns): %w", err)
			}
			vals[i] = val
		}
		result[col] = vals
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// toIndex writes JSON in index orient: {idx: {col: val, ...}, ...}
func toIndex(df dataframe.DataFrame, w io.Writer) error {
	nRows, _ := df.Shape()
	columns := df.Columns()
	labels := df.Index().Labels()

	result := make(map[string]map[string]any, nRows)
	for i := 0; i < nRows; i++ {
		key := formatIndexLabel(labels[i])
		row := make(map[string]any, len(columns))
		for _, col := range columns {
			val, err := df.At(i, col)
			if err != nil {
				return fmt.Errorf("json.ToJSON(index): %w", err)
			}
			row[col] = val
		}
		result[key] = row
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// formatIndexLabel converts an index label (any) to a string key.
func formatIndexLabel(label any) string {
	return fmt.Sprintf("%v", label)
}
