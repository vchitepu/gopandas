package dataframe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/index"
	"github.com/vinaychitepu/gopandas/series"
)

// Filter returns a new DataFrame containing only rows where mask is true.
func (df DataFrame) Filter(mask series.Series[bool]) (DataFrame, error) {
	if mask.Len() != df.Len() {
		return DataFrame{}, fmt.Errorf("dataframe.Filter: mask length %d != DataFrame length %d", mask.Len(), df.Len())
	}

	// Collect row positions where mask is true
	var positions []int
	for i := 0; i < mask.Len(); i++ {
		val, isNull := mask.At(i)
		if !isNull && val {
			positions = append(positions, i)
		}
	}

	nRows := len(positions)
	newIdx := index.NewRangeIndex(nRows, "")
	newData := make(map[string]*series.Series[any], len(df.columns))

	for _, col := range df.columns {
		src := df.data[col]
		vals := make([]any, nRows)
		for i, pos := range positions {
			val, _ := src.At(pos)
			vals[i] = val
		}
		s := series.New[any](memory.DefaultAllocator, vals, newIdx, col)
		newData[col] = &s
	}

	return DataFrame{index: newIdx, columns: df.Columns(), data: newData}, nil
}

// Query filters the DataFrame using a simple expression string.
// Supported operators: >, >=, <, <=, ==, !=
// Examples: "age > 20", "name == 'Bob'"
func (df DataFrame) Query(expr string) (DataFrame, error) {
	col, op, val, err := parseExpr(expr)
	if err != nil {
		return DataFrame{}, fmt.Errorf("dataframe.Query: %w", err)
	}

	s, ok := df.data[col]
	if !ok {
		return DataFrame{}, fmt.Errorf("dataframe.Query: column %q not found", col)
	}

	n := df.Len()
	maskVals := make([]bool, n)
	for i := 0; i < n; i++ {
		cellVal, isNull := s.At(i)
		if isNull {
			maskVals[i] = false
			continue
		}
		maskVals[i] = compareValues(cellVal, op, val)
	}

	mask := series.New[bool](memory.DefaultAllocator, maskVals, index.NewRangeIndex(n, ""), "mask")
	return df.Filter(mask)
}

// parseExpr parses a simple "column op value" expression like "col > 10" or "col == 'foo'".
// It supports only simple expressions with a single column, operator, and literal value.
// Supported operators: >=, <=, !=, ==, >, <.
func parseExpr(expr string) (col, op string, val any, err error) {
	// Try each operator (longest first to avoid ambiguity)
	operators := []string{">=", "<=", "!=", "==", ">", "<"}
	for _, o := range operators {
		idx := strings.Index(expr, o)
		if idx >= 0 {
			col = strings.TrimSpace(expr[:idx])
			op = o
			valStr := strings.TrimSpace(expr[idx+len(o):])
			val, err = parseValue(valStr)
			if err != nil {
				return "", "", nil, err
			}
			return col, op, val, nil
		}
	}
	return "", "", nil, fmt.Errorf("no supported operator found in %q", expr)
}

// parseValue parses a value string into the appropriate Go type.
func parseValue(s string) (any, error) {
	// String literal: 'foo' or "foo"
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		return s[1 : len(s)-1], nil
	}
	// Try int64
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	// Try float64
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	// Try bool
	if b, err := strconv.ParseBool(s); err == nil {
		return b, nil
	}
	return s, nil
}

// compareValues compares a cell value against a query value using the given operator.
func compareValues(cellVal any, op string, queryVal any) bool {
	// For == and != with string types, compare directly
	if op == "==" {
		return fmt.Sprintf("%v", cellVal) == fmt.Sprintf("%v", queryVal)
	}
	if op == "!=" {
		return fmt.Sprintf("%v", cellVal) != fmt.Sprintf("%v", queryVal)
	}

	// For numeric comparisons, convert both to float64
	cf, cOK := toFloat64(cellVal)
	qf, qOK := toFloat64(queryVal)
	if !cOK || !qOK {
		// Fall back to string comparison
		cs := fmt.Sprintf("%v", cellVal)
		qs := fmt.Sprintf("%v", queryVal)
		switch op {
		case ">":
			return cs > qs
		case ">=":
			return cs >= qs
		case "<":
			return cs < qs
		case "<=":
			return cs <= qs
		}
		return false
	}

	switch op {
	case ">":
		return cf > qf
	case ">=":
		return cf >= qf
	case "<":
		return cf < qf
	case "<=":
		return cf <= qf
	}
	return false
}

// toFloat64 converts a value to float64 if possible.
// Supports int, int32, int64, float32, and float64.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
}
