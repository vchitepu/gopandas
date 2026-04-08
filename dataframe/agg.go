package dataframe

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/dtype"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
)

// numericColumns returns the names of numeric columns.
func (df DataFrame) numericColumns() []string {
	var cols []string
	for _, col := range df.columns {
		dt := df.data[col].DType()
		if dt == dtype.Int64 || dt == dtype.Float64 {
			cols = append(cols, col)
		}
	}
	return cols
}

// Sum returns a Series[any] with the sum of each numeric column.
func (df DataFrame) Sum() series.Series[any] {
	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))
	for i, col := range numCols {
		labels[i] = col
		s := df.data[col]
		sum, err := s.Sum()
		if err != nil {
			vals[i] = nil
		} else {
			// Convert to float64 for consistency
			switch v := sum.(type) {
			case int64:
				vals[i] = float64(v)
			case float64:
				vals[i] = v
			default:
				vals[i] = sum
			}
		}
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "sum")
}

// Count returns a Series[any] with the non-null count of each column.
func (df DataFrame) Count() series.Series[any] {
	labels := make([]string, len(df.columns))
	vals := make([]any, len(df.columns))
	for i, col := range df.columns {
		labels[i] = col
		vals[i] = int64(df.data[col].Count())
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "count")
}

// Mean returns a Series[any] with the mean of each numeric column.
func (df DataFrame) Mean() series.Series[any] {
	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))
	for i, col := range numCols {
		labels[i] = col
		s := df.data[col]
		mean, err := s.Mean()
		if err != nil {
			vals[i] = nil
		} else {
			vals[i] = mean
		}
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "mean")
}

// Std returns a Series[any] with the standard deviation of each numeric column.
func (df DataFrame) Std() series.Series[any] {
	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))
	for i, col := range numCols {
		labels[i] = col
		s := df.data[col]
		std, err := s.Std()
		if err != nil {
			vals[i] = nil
		} else {
			vals[i] = std
		}
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "std")
}

// Min returns a Series[any] with the minimum of each numeric column.
func (df DataFrame) Min() series.Series[any] {
	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))
	for i, col := range numCols {
		labels[i] = col
		s := df.data[col]
		min, err := s.Min()
		if err != nil {
			vals[i] = nil
		} else {
			// Convert to float64 for homogeneous type
			switch v := min.(type) {
			case int64:
				vals[i] = float64(v)
			case float64:
				vals[i] = v
			default:
				vals[i] = min
			}
		}
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "min")
}

// Max returns a Series[any] with the maximum of each numeric column.
func (df DataFrame) Max() series.Series[any] {
	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))
	for i, col := range numCols {
		labels[i] = col
		s := df.data[col]
		max, err := s.Max()
		if err != nil {
			vals[i] = nil
		} else {
			// Convert to float64 for homogeneous type
			switch v := max.(type) {
			case int64:
				vals[i] = float64(v)
			case float64:
				vals[i] = v
			default:
				vals[i] = max
			}
		}
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "max")
}

// Describe returns a DataFrame with summary statistics (count, mean, std, min, max)
// for each numeric column.
func (df DataFrame) Describe() DataFrame {
	numCols := df.numericColumns()
	rowLabels := []string{"count", "mean", "std", "min", "max"}
	rowIdx := index.NewStringIndex(rowLabels, "")

	data := make(map[string]*series.Series[any], len(numCols))
	for _, col := range numCols {
		s := df.data[col]
		vals := make([]any, 5)

		vals[0] = float64(s.Count())

		mean, err := s.Mean()
		if err != nil {
			vals[1] = nil
		} else {
			vals[1] = mean
		}

		std, err := s.Std()
		if err != nil {
			vals[2] = nil
		} else {
			vals[2] = std
		}

		minVal, err := s.Min()
		if err != nil {
			vals[3] = nil
		} else {
			switch v := minVal.(type) {
			case int64:
				vals[3] = float64(v)
			default:
				vals[3] = minVal
			}
		}

		maxVal, err := s.Max()
		if err != nil {
			vals[4] = nil
		} else {
			switch v := maxVal.(type) {
			case int64:
				vals[4] = float64(v)
			default:
				vals[4] = maxVal
			}
		}

		cs := series.New[any](memory.DefaultAllocator, vals, rowIdx, col)
		data[col] = &cs
	}

	return DataFrame{index: rowIdx, columns: numCols, data: data}
}

// Corr returns a DataFrame with the Pearson correlation matrix for numeric columns.
func (df DataFrame) Corr() DataFrame {
	numCols := df.numericColumns()
	n := len(numCols)

	// Extract float64 slices for each numeric column
	colVals := make(map[string][]float64, n)
	for _, col := range numCols {
		s := df.data[col]
		vals := make([]float64, df.Len())
		for i := 0; i < df.Len(); i++ {
			v, isNull := s.At(i)
			if isNull || v == nil {
				vals[i] = math.NaN()
			} else {
				if f, ok := toFloat64(v); ok {
					vals[i] = f
				} else {
					vals[i] = math.NaN()
				}
			}
		}
		colVals[col] = vals
	}

	rowIdx := index.NewStringIndex(numCols, "")
	data := make(map[string]*series.Series[any], n)

	for _, colJ := range numCols {
		vals := make([]any, n)
		for i, colI := range numCols {
			vals[i] = pearson(colVals[colI], colVals[colJ])
		}
		cs := series.New[any](memory.DefaultAllocator, vals, rowIdx, colJ)
		data[colJ] = &cs
	}

	return DataFrame{index: rowIdx, columns: numCols, data: data}
}

// pearson computes the Pearson correlation coefficient between two float64 slices.
func pearson(x, y []float64) float64 {
	n := len(x)
	if n == 0 || n != len(y) {
		return math.NaN()
	}

	var sumX, sumY, sumXY, sumX2, sumY2 float64
	var count float64
	for i := 0; i < n; i++ {
		if math.IsNaN(x[i]) || math.IsNaN(y[i]) {
			continue
		}
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
		count++
	}

	if count < 2 {
		return math.NaN()
	}

	num := count*sumXY - sumX*sumY
	den := math.Sqrt((count*sumX2 - sumX*sumX) * (count*sumY2 - sumY*sumY))
	if den == 0 {
		return math.NaN()
	}
	return num / den
}

// CorrWith returns a Series[any] with the Pearson correlation of each numeric column
// with the given series. Returns an error if the series length does not match
// the DataFrame length.
func (df DataFrame) CorrWith(s *series.Series[any]) (series.Series[any], error) {
	if s.Len() != df.Len() {
		return series.Series[any]{}, fmt.Errorf("dataframe.CorrWith: series length %d does not match DataFrame length %d", s.Len(), df.Len())
	}

	numCols := df.numericColumns()
	labels := make([]string, len(numCols))
	vals := make([]any, len(numCols))

	// Extract float64 values from the reference series
	refVals := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		v, isNull := s.At(i)
		if isNull || v == nil {
			refVals[i] = math.NaN()
		} else {
			if f, ok := toFloat64(v); ok {
				refVals[i] = f
			} else {
				refVals[i] = math.NaN()
			}
		}
	}

	for i, col := range numCols {
		labels[i] = col
		cs := df.data[col]
		colVals := make([]float64, cs.Len())
		for j := 0; j < cs.Len(); j++ {
			v, isNull := cs.At(j)
			if isNull || v == nil {
				colVals[j] = math.NaN()
			} else {
				if f, ok := toFloat64(v); ok {
					colVals[j] = f
				} else {
					colVals[j] = math.NaN()
				}
			}
		}
		vals[i] = pearson(colVals, refVals)
	}

	idx := index.NewStringIndex(labels, "")
	return series.New[any](memory.DefaultAllocator, vals, idx, "corrwith"), nil
}
