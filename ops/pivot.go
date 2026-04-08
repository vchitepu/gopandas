package ops

import (
	"fmt"
	"math"
	"sort"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// Pivot reshapes a long DataFrame to wide format.
// Unique index values become rows, unique column values become column headers.
// Duplicate (index, column) pairs cause an error.
func Pivot(df dataframe.DataFrame, index, columns, values string) (dataframe.DataFrame, error) {
	// Validate columns exist.
	for _, col := range []string{index, columns, values} {
		if _, err := df.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Pivot: column %q not found", col)
		}
	}

	// Collect unique index values and column values (preserving order of first appearance).
	indexVals := uniqueValues(df, index)
	colVals := uniqueValues(df, columns)

	// Build a map: (indexVal, colVal) -> value.
	type cellKey struct {
		idx string
		col string
	}
	cells := make(map[cellKey]any)

	for i := 0; i < df.Len(); i++ {
		idxVal, _ := df.At(i, index)
		colVal, _ := df.At(i, columns)
		val, _ := df.At(i, values)

		ck := cellKey{fmt.Sprintf("%v", idxVal), fmt.Sprintf("%v", colVal)}
		if _, exists := cells[ck]; exists {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Pivot: duplicate (index=%v, column=%v) pair", idxVal, colVal)
		}
		cells[ck] = val
	}

	// Build records.
	records := make([]map[string]any, len(indexVals))
	for i, idxVal := range indexVals {
		rec := map[string]any{index: idxVal}
		for _, colVal := range colVals {
			ck := cellKey{fmt.Sprintf("%v", idxVal), colVal}
			if v, ok := cells[ck]; ok {
				rec[colVal] = v
			} else {
				rec[colVal] = nil
			}
		}
		records[i] = rec
	}

	return dataframe.FromRecords(records)
}

// PivotTable reshapes like Pivot but aggregates duplicate (index, column) pairs.
// All value cells are float64. Missing cells are NaN.
func PivotTable(df dataframe.DataFrame, index, columns, values string, aggFunc AggFunc) (dataframe.DataFrame, error) {
	// Validate columns exist.
	for _, col := range []string{index, columns, values} {
		if _, err := df.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.PivotTable: column %q not found", col)
		}
	}

	indexVals := uniqueValues(df, index)
	colVals := uniqueValues(df, columns)

	// Build a map: (indexVal, colVal) -> []float64 values.
	type cellKey struct {
		idx string
		col string
	}
	cells := make(map[cellKey][]float64)

	for i := 0; i < df.Len(); i++ {
		idxVal, _ := df.At(i, index)
		colVal, _ := df.At(i, columns)
		val, _ := df.At(i, values)

		ck := cellKey{fmt.Sprintf("%v", idxVal), fmt.Sprintf("%v", colVal)}
		if fv, ok := toFloat64(val); ok {
			cells[ck] = append(cells[ck], fv)
		}
	}

	// Build records.
	records := make([]map[string]any, len(indexVals))
	for i, idxVal := range indexVals {
		rec := map[string]any{index: idxVal}
		for _, colVal := range colVals {
			ck := cellKey{fmt.Sprintf("%v", idxVal), colVal}
			if vals, ok := cells[ck]; ok && len(vals) > 0 {
				rec[colVal] = aggregate(vals, aggFunc)
			} else {
				rec[colVal] = math.NaN()
			}
		}
		records[i] = rec
	}

	return dataframe.FromRecords(records)
}

// toFloat64 converts a value to float64 if possible.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
}

// aggregate applies the given aggregation function to a slice of float64 values.
func aggregate(vals []float64, aggFunc AggFunc) float64 {
	if len(vals) == 0 {
		return math.NaN()
	}

	switch aggFunc {
	case AggSum:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum
	case AggMean:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case AggCount:
		return float64(len(vals))
	case AggMin:
		min := vals[0]
		for _, v := range vals[1:] {
			if v < min {
				min = v
			}
		}
		return min
	case AggMax:
		max := vals[0]
		for _, v := range vals[1:] {
			if v > max {
				max = v
			}
		}
		return max
	case AggStd:
		n := float64(len(vals))
		if n <= 1 {
			return 0
		}
		mean := 0.0
		for _, v := range vals {
			mean += v
		}
		mean /= n
		sumSq := 0.0
		for _, v := range vals {
			d := v - mean
			sumSq += d * d
		}
		return math.Sqrt(sumSq / (n - 1)) // sample std dev
	case AggFirst:
		return vals[0]
	case AggLast:
		return vals[len(vals)-1]
	default:
		return math.NaN()
	}
}

// uniqueValues returns unique values from a column, preserving order of first appearance.
func uniqueValues(df dataframe.DataFrame, col string) []string {
	seen := make(map[string]bool)
	var result []string
	for i := 0; i < df.Len(); i++ {
		v, _ := df.At(i, col)
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result
}
