package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// ToCSV writes the DataFrame to w in CSV format.
// Use CSVOption functions to customize output (e.g., WithSep).
func ToCSV(df dataframe.DataFrame, w io.Writer, opts ...CSVOption) error {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	writer := csv.NewWriter(w)
	writer.Comma = cfg.sep

	columns := df.Columns()
	nRows, _ := df.Shape()

	// Write header
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("csv write header: %w", err)
	}

	// Write data rows
	record := make([]string, len(columns))
	for row := 0; row < nRows; row++ {
		for colIdx, colName := range columns {
			val, err := df.At(row, colName)
			if err != nil {
				return fmt.Errorf("csv write: row %d, col %q: %w", row, colName, err)
			}
			record[colIdx] = formatValue(val)
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("csv write row %d: %w", row, err)
		}
	}

	writer.Flush()
	return writer.Error()
}

// formatValue converts a value to its string representation for CSV output.
func formatValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case string:
		return val
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
