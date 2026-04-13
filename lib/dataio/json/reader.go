package json

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

// FromJSON reads JSON data from the given io.Reader and returns a DataFrame.
// The orient parameter specifies the expected JSON structure.
func FromJSON(r io.Reader, orient JSONOrient) (dataframe.DataFrame, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON: %w", err)
	}

	switch orient {
	case OrientRecords:
		return fromRecords(data)
	case OrientColumns:
		return fromColumns(data)
	case OrientIndex:
		return fromIndex(data)
	default:
		return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON: unsupported orient %v", orient)
	}
}

// fromRecords parses JSON in records orient: [{col: val, ...}, ...]
func fromRecords(data []byte) (dataframe.DataFrame, error) {
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON(records): %w", err)
	}

	if len(records) == 0 {
		return dataframe.New(map[string]any{})
	}

	return dataframe.FromRecords(records)
}

// fromColumns parses JSON in columns orient: {col: [val, ...], ...}
func fromColumns(data []byte) (dataframe.DataFrame, error) {
	var columns map[string][]any
	if err := json.Unmarshal(data, &columns); err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON(columns): %w", err)
	}

	if len(columns) == 0 {
		return dataframe.New(map[string]any{})
	}

	// Determine number of rows from first column and validate equal lengths
	var nRows int
	first := true
	for col, vals := range columns {
		if first {
			nRows = len(vals)
			first = false
		} else if len(vals) != nRows {
			return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON(columns): column %q has length %d, expected %d", col, len(vals), nRows)
		}
	}

	// Convert to records format
	records := make([]map[string]any, nRows)
	for i := 0; i < nRows; i++ {
		records[i] = make(map[string]any)
	}
	for col, vals := range columns {
		for i, val := range vals {
			records[i][col] = val
		}
	}

	return dataframe.FromRecords(records)
}

// fromIndex parses JSON in index orient: {idx: {col: val, ...}, ...}
func fromIndex(data []byte) (dataframe.DataFrame, error) {
	var indexed map[string]map[string]any
	if err := json.Unmarshal(data, &indexed); err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("json.FromJSON(index): %w", err)
	}

	if len(indexed) == 0 {
		return dataframe.New(map[string]any{})
	}

	// Sort index keys for deterministic order
	keys := make([]string, 0, len(indexed))
	for k := range indexed {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Convert to records format
	records := make([]map[string]any, len(keys))
	for i, key := range keys {
		records[i] = indexed[key]
	}

	return dataframe.FromRecords(records)
}
