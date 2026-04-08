package groupby

import (
	"fmt"
	"math"
	"strings"

	"github.com/vchitepu/gopandas/dataframe"
	"github.com/vchitepu/gopandas/dtype"
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

// Count returns a DataFrame with the count of non-null values per group for ALL non-key columns.
func (gb GroupBy) Count() (dataframe.DataFrame, error) {
	keys := gb.sortedGroupKeys()
	valCols := gb.valueColumns()

	records := make([]map[string]any, len(keys))
	for i, k := range keys {
		rec := make(map[string]any)
		// Add key columns
		parts := splitCompositeKey(k)
		for j, keyCol := range gb.keys {
			rec[keyCol] = parts[j]
		}
		// Count non-null values in each value column
		for _, col := range valCols {
			var count int64
			for _, pos := range gb.groups[k] {
				v, err := gb.df.At(pos, col)
				if err != nil {
					return dataframe.DataFrame{}, err
				}
				if v != nil {
					count++
				}
			}
			rec[col] = count
		}
		records[i] = rec
	}
	return dataframe.FromRecords(records)
}

// Mean returns a DataFrame with the mean of each numeric column per group.
// Empty groups produce NaN.
func (gb GroupBy) Mean() (dataframe.DataFrame, error) {
	return gb.aggregateNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	})
}

// Min returns a DataFrame with the minimum of each numeric column per group.
func (gb GroupBy) Min() (dataframe.DataFrame, error) {
	return gb.aggregateNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	})
}

// Std returns a DataFrame with the sample standard deviation (ddof=1) of each numeric column per group.
// Single-element groups produce NaN.
func (gb GroupBy) Std() (dataframe.DataFrame, error) {
	return gb.aggregateNumeric(func(vals []float64) float64 {
		n := len(vals)
		if n < 2 {
			return math.NaN()
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		mean := sum / float64(n)
		var variance float64
		for _, v := range vals {
			d := v - mean
			variance += d * d
		}
		variance /= float64(n - 1)
		return math.Sqrt(variance)
	})
}

// Max returns a DataFrame with the maximum of each numeric column per group.
func (gb GroupBy) Max() (dataframe.DataFrame, error) {
	return gb.aggregateNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	})
}

// aggregateFirstLast returns the first (last=false) or last (last=true) row from each group.
// Includes ALL columns, not just numeric.
func (gb GroupBy) aggregateFirstLast(last bool) (dataframe.DataFrame, error) {
	keys := gb.sortedGroupKeys()
	allCols := gb.df.Columns()

	records := make([]map[string]any, len(keys))
	for i, k := range keys {
		positions := gb.groups[k]
		var pos int
		if last {
			pos = positions[len(positions)-1]
		} else {
			pos = positions[0]
		}
		rec := make(map[string]any, len(allCols))
		for _, col := range allCols {
			val, err := gb.df.At(pos, col)
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			rec[col] = val
		}
		records[i] = rec
	}
	return dataframe.FromRecords(records)
}

// First returns a DataFrame with the first row from each group. Includes ALL columns.
func (gb GroupBy) First() (dataframe.DataFrame, error) {
	return gb.aggregateFirstLast(false)
}

// Last returns a DataFrame with the last row from each group. Includes ALL columns.
func (gb GroupBy) Last() (dataframe.DataFrame, error) {
	return gb.aggregateFirstLast(true)
}
