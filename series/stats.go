package series

import (
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/vchitepu/gopandas/arrowutil"
	"github.com/vchitepu/gopandas/dtype"
	"gonum.org/v1/gonum/stat"
)

// Sum returns the sum of all non-null values in the Series.
// Returns an error if the series dtype is not numeric (int64 or float64).
func (s Series[T]) Sum() (T, error) {
	var zero T
	dt := arrowutil.InferDType(s.arr)
	if dt != dtype.Int64 && dt != dtype.Float64 {
		return zero, fmt.Errorf("series.Sum: unsupported dtype %v (need int64 or float64)", dt)
	}

	n := s.Len()
	if n == 0 {
		return zero, nil
	}

	switch a := s.arr.(type) {
	case *array.Int64:
		var sum int64
		for i := 0; i < n; i++ {
			if !a.IsNull(i) {
				sum += a.Value(i)
			}
		}
		return any(sum).(T), nil
	case *array.Float64:
		var sum float64
		for i := 0; i < n; i++ {
			if !a.IsNull(i) {
				sum += a.Value(i)
			}
		}
		return any(sum).(T), nil
	default:
		return zero, fmt.Errorf("series.Sum: unsupported array type %T", s.arr)
	}
}

// toFloat64Slice extracts non-null values as float64s from the underlying Arrow array.
func (s Series[T]) toFloat64Slice() ([]float64, error) {
	dt := arrowutil.InferDType(s.arr)
	if dt != dtype.Int64 && dt != dtype.Float64 {
		return nil, fmt.Errorf("series: unsupported dtype %v for numeric operation", dt)
	}

	var result []float64
	n := s.Len()
	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			continue
		}
		val, _ := arrowutil.GetValue(s.arr, i)
		switch v := val.(type) {
		case int64:
			result = append(result, float64(v))
		case float64:
			result = append(result, v)
		}
	}
	return result, nil
}

// Mean returns the arithmetic mean of all non-null values.
// Returns NaN if the series is empty or has no non-null values.
func (s Series[T]) Mean() (float64, error) {
	vals, err := s.toFloat64Slice()
	if err != nil {
		return 0, fmt.Errorf("series.Mean: %w", err)
	}
	if len(vals) == 0 {
		return math.NaN(), nil
	}
	return stat.Mean(vals, nil), nil
}

// Var returns the sample variance of all non-null values.
// Returns NaN if fewer than 2 non-null values exist.
func (s Series[T]) Var() (float64, error) {
	vals, err := s.toFloat64Slice()
	if err != nil {
		return 0, fmt.Errorf("series.Var: %w", err)
	}
	if len(vals) < 2 {
		return math.NaN(), nil
	}
	return stat.Variance(vals, nil), nil
}

// Std returns the sample standard deviation of all non-null values.
// Returns NaN if fewer than 2 non-null values exist.
func (s Series[T]) Std() (float64, error) {
	v, err := s.Var()
	if err != nil {
		return 0, fmt.Errorf("series.Std: %w", err)
	}
	if math.IsNaN(v) {
		return math.NaN(), nil
	}
	return math.Sqrt(v), nil
}

// Min returns the minimum value among non-null elements.
// Returns an error if the series is empty or all values are null.
func (s Series[T]) Min() (T, error) {
	var zero T
	n := s.Len()
	if n == 0 {
		return zero, fmt.Errorf("series.Min: empty series")
	}
	var minIdx int = -1
	for i := 0; i < n; i++ {
		if !arrowutil.IsNull(s.arr, i) {
			minIdx = i
			break
		}
	}
	if minIdx == -1 {
		return zero, fmt.Errorf("series.Min: all values are null")
	}
	minVal, _ := arrowutil.GetValue(s.arr, minIdx)
	for i := minIdx + 1; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			continue
		}
		val, _ := arrowutil.GetValue(s.arr, i)
		if compareValues(val, minVal) < 0 {
			minVal = val
		}
	}
	return any(minVal).(T), nil
}

// Max returns the maximum value among non-null elements.
// Returns an error if the series is empty or all values are null.
func (s Series[T]) Max() (T, error) {
	var zero T
	n := s.Len()
	if n == 0 {
		return zero, fmt.Errorf("series.Max: empty series")
	}
	var maxIdx int = -1
	for i := 0; i < n; i++ {
		if !arrowutil.IsNull(s.arr, i) {
			maxIdx = i
			break
		}
	}
	if maxIdx == -1 {
		return zero, fmt.Errorf("series.Max: all values are null")
	}
	maxVal, _ := arrowutil.GetValue(s.arr, maxIdx)
	for i := maxIdx + 1; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			continue
		}
		val, _ := arrowutil.GetValue(s.arr, i)
		if compareValues(val, maxVal) > 0 {
			maxVal = val
		}
	}
	return any(maxVal).(T), nil
}

// compareValues compares two values of the same type for ordering.
// Returns -1, 0, or 1 if a < b, a == b, or a > b.
func compareValues(a, b any) int {
	switch va := a.(type) {
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case float64:
		vb := b.(float64)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case bool:
		vb := b.(bool)
		if !va && vb {
			return -1
		}
		if va && !vb {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// Median returns the median of all non-null values.
// Returns NaN if no non-null values exist.
func (s Series[T]) Median() (float64, error) {
	vals, err := s.toFloat64Slice()
	if err != nil {
		return 0, fmt.Errorf("series.Median: %w", err)
	}
	if len(vals) == 0 {
		return math.NaN(), nil
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2.0, nil
	}
	return sorted[n/2], nil
}

// Quantile returns the q-th quantile of all non-null values using linear interpolation.
// q must be in [0, 1].
func (s Series[T]) Quantile(q float64) (float64, error) {
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("series.Quantile: q=%v out of range [0, 1]", q)
	}
	vals, err := s.toFloat64Slice()
	if err != nil {
		return 0, fmt.Errorf("series.Quantile: %w", err)
	}
	if len(vals) == 0 {
		return math.NaN(), nil
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	n := float64(len(sorted))
	pos := q * (n - 1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo], nil
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac, nil
}

// Describe returns summary statistics for the Series as a map[string]float64.
// For non-numeric dtypes, only 'count' is returned.
func (s Series[T]) Describe() map[string]float64 {
	result := make(map[string]float64)
	result["count"] = float64(s.Count())

	dt := arrowutil.InferDType(s.arr)
	if dt != dtype.Int64 && dt != dtype.Float64 {
		return result
	}

	vals, err := s.toFloat64Slice()
	if err != nil || len(vals) == 0 {
		return result
	}

	mean, _ := s.Mean()
	result["mean"] = mean

	std, _ := s.Std()
	result["std"] = std

	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	result["min"] = sorted[0]
	result["max"] = sorted[len(sorted)-1]

	q25, _ := s.Quantile(0.25)
	result["25%"] = q25
	q50, _ := s.Quantile(0.50)
	result["50%"] = q50
	q75, _ := s.Quantile(0.75)
	result["75%"] = q75

	return result
}
