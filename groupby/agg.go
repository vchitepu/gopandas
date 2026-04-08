package groupby

import (
	"fmt"
	"strings"

	"github.com/vinaychitepu/gopandas/dataframe"
	"github.com/vinaychitepu/gopandas/dtype"
)

// aggFunc is a function that aggregates a slice of float64 values into a single float64.
type aggFunc func([]float64) float64

// valueColumns returns all columns that are not key columns.
func (gb GroupBy) valueColumns() []string {
	keySet := make(map[string]bool, len(gb.keys))
	for _, k := range gb.keys {
		keySet[k] = true
	}
	var cols []string
	for _, c := range gb.df.Columns() {
		if !keySet[c] {
			cols = append(cols, c)
		}
	}
	return cols
}

// numericValueColumns returns value columns that have numeric dtype (Int64 or Float64).
func (gb GroupBy) numericValueColumns() []string {
	dtypes := gb.df.DTypes()
	var cols []string
	for _, c := range gb.valueColumns() {
		dt := dtypes[c]
		if dt == dtype.Int64 || dt == dtype.Float64 {
			cols = append(cols, c)
		}
	}
	return cols
}

// extractFloat64Values reads values from a column at given positions, converting to float64.
func (gb GroupBy) extractFloat64Values(col string, positions []int) ([]float64, error) {
	vals := make([]float64, 0, len(positions))
	for _, pos := range positions {
		v, err := gb.df.At(pos, col)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		switch n := v.(type) {
		case float64:
			vals = append(vals, n)
		case int64:
			vals = append(vals, float64(n))
		default:
			return nil, fmt.Errorf("extractFloat64Values: unexpected type %T for column %q", v, col)
		}
	}
	return vals, nil
}

// aggregateNumeric applies aggFn to each numeric value column per group and returns a result DataFrame.
func (gb GroupBy) aggregateNumeric(aggFn aggFunc) (dataframe.DataFrame, error) {
	keys := gb.sortedGroupKeys()
	numCols := gb.numericValueColumns()

	records := make([]map[string]any, len(keys))
	for i, k := range keys {
		rec := make(map[string]any)
		// Add key columns
		parts := splitCompositeKey(k)
		for j, keyCol := range gb.keys {
			rec[keyCol] = parts[j]
		}
		// Aggregate numeric columns
		for _, col := range numCols {
			vals, err := gb.extractFloat64Values(col, gb.groups[k])
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			rec[col] = aggFn(vals)
		}
		records[i] = rec
	}
	return dataframe.FromRecords(records)
}

// splitCompositeKey splits a pipe-separated composite key back into parts.
func splitCompositeKey(key string) []string {
	return strings.Split(key, "|")
}

// Sum returns a DataFrame with the sum of each numeric column per group.
func (gb GroupBy) Sum() (dataframe.DataFrame, error) {
	return gb.aggregateNumeric(func(vals []float64) float64 {
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum
	})
}
