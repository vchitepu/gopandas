package groupby

import (
	"fmt"
	"math"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// namedAggFuncs maps function name strings to aggFunc implementations.
var namedAggFuncs = map[string]aggFunc{
	"sum": func(vals []float64) float64 {
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum
	},
	"mean": func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	},
	"count": func(vals []float64) float64 {
		return float64(len(vals))
	},
	"min": func(vals []float64) float64 {
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
	},
	"max": func(vals []float64) float64 {
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
	},
	"std": func(vals []float64) float64 {
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
	},
	"first": func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		return vals[0]
	},
	"last": func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		return vals[len(vals)-1]
	},
}

// Agg applies per-column aggregation functions specified by name.
// colFuncs maps column name to function name ("sum", "mean", "count", "min", "max", "std", "first", "last").
// All results are float64.
func (gb GroupBy) Agg(colFuncs map[string]string) (dataframe.DataFrame, error) {
	// Validate all function names
	for col, fnName := range colFuncs {
		if _, ok := namedAggFuncs[fnName]; !ok {
			return dataframe.DataFrame{}, fmt.Errorf("Agg: unknown function %q for column %q", fnName, col)
		}
	}

	keys := gb.sortedGroupKeys()
	records := make([]map[string]any, len(keys))
	for i, k := range keys {
		rec := make(map[string]any)
		// Add key columns
		parts := splitCompositeKey(k)
		for j, keyCol := range gb.keys {
			rec[keyCol] = parts[j]
		}
		// Apply per-column aggregation
		for col, fnName := range colFuncs {
			vals, err := gb.extractFloat64Values(col, gb.groups[k])
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			rec[col] = namedAggFuncs[fnName](vals)
		}
		records[i] = rec
	}
	return dataframe.FromRecords(records)
}

// concatDataFrames concatenates a slice of DataFrames into a single DataFrame
// by extracting all rows as records and rebuilding via FromRecords.
func concatDataFrames(frames []dataframe.DataFrame) (dataframe.DataFrame, error) {
	var allRecords []map[string]any
	for _, frame := range frames {
		cols := frame.Columns()
		for i := 0; i < frame.Len(); i++ {
			rec := make(map[string]any, len(cols))
			for _, col := range cols {
				val, err := frame.At(i, col)
				if err != nil {
					return dataframe.DataFrame{}, err
				}
				rec[col] = val
			}
			allRecords = append(allRecords, rec)
		}
	}
	return dataframe.FromRecords(allRecords)
}

// Apply calls fn for each group's sub-DataFrame and concatenates the results.
func (gb GroupBy) Apply(fn func(dataframe.DataFrame) (dataframe.DataFrame, error)) (dataframe.DataFrame, error) {
	keys := gb.sortedGroupKeys()
	frames := make([]dataframe.DataFrame, 0, len(keys))

	for _, k := range keys {
		sub, err := gb.subDF(gb.groups[k])
		if err != nil {
			return dataframe.DataFrame{}, err
		}
		result, err := fn(sub)
		if err != nil {
			return dataframe.DataFrame{}, err
		}
		frames = append(frames, result)
	}
	return concatDataFrames(frames)
}

// TransformFunc is called per numeric column per group. It receives the column name,
// the original row positions in that group, and the source DataFrame.
// It must return a float64 slice with the same length as positions.
type TransformFunc func(col string, positions []int, src dataframe.DataFrame) ([]float64, error)

// Transform applies fn to each numeric value column of each group, placing
// the transformed values back at the original row positions.
// Returns a DataFrame with the same row count as the original, with numeric columns
// replaced by transformed values and key columns preserved.
func (gb GroupBy) Transform(fn TransformFunc) (dataframe.DataFrame, error) {
	nRows := gb.df.Len()
	numCols := gb.numericValueColumns()

	// Initialize result columns: map of col -> values at original positions
	resultCols := make(map[string][]float64, len(numCols))
	for _, col := range numCols {
		resultCols[col] = make([]float64, nRows)
	}

	// Process each group
	for _, positions := range gb.groups {
		for _, col := range numCols {
			transformed, err := fn(col, positions, gb.df)
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			if len(transformed) != len(positions) {
				return dataframe.DataFrame{}, fmt.Errorf("Transform: fn returned %d values, want %d", len(transformed), len(positions))
			}
			// Place transformed values at original positions
			for i, pos := range positions {
				resultCols[col][pos] = transformed[i]
			}
		}
	}

	// Build result records preserving original row order
	records := make([]map[string]any, nRows)
	allCols := gb.df.Columns()
	numColSet := make(map[string]bool, len(numCols))
	for _, c := range numCols {
		numColSet[c] = true
	}

	for i := 0; i < nRows; i++ {
		rec := make(map[string]any, len(allCols))
		for _, col := range allCols {
			if numColSet[col] {
				rec[col] = resultCols[col][i]
			} else {
				val, err := gb.df.At(i, col)
				if err != nil {
					return dataframe.DataFrame{}, err
				}
				rec[col] = val
			}
		}
		records[i] = rec
	}
	return dataframe.FromRecords(records)
}
