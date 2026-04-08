package ops

import (
	"fmt"
	"strings"

	"github.com/vchitepu/gopandas/dataframe"
)

// Merge performs an SQL-style join of two DataFrames on the specified key columns.
func Merge(left, right dataframe.DataFrame, on []string, how JoinType) (dataframe.DataFrame, error) {
	if len(on) == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("ops.Merge: on must not be empty")
	}

	// Validate key columns exist in both DataFrames.
	for _, col := range on {
		if _, err := left.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Merge: left DataFrame missing column %q", col)
		}
		if _, err := right.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Merge: right DataFrame missing column %q", col)
		}
	}

	// Build key map for the right DataFrame: composite key -> []row indices.
	rightKeyMap := buildKeyMap(right, on)

	// Determine non-key columns from each side.
	leftNonKey := nonKeyCols(left.Columns(), on)
	rightNonKey := nonKeyCols(right.Columns(), on)

	// Build row pairs.
	type rowPair struct {
		leftIdx  int // -1 means no match
		rightIdx int // -1 means no match
	}
	var pairs []rowPair

	leftMatched := make(map[int]bool)
	rightMatched := make(map[int]bool)

	// For each left row, find matching right rows.
	for li := 0; li < left.Len(); li++ {
		key := extractKey(left, li, on)
		if rightIndices, ok := rightKeyMap[key]; ok {
			for _, ri := range rightIndices {
				pairs = append(pairs, rowPair{li, ri})
				leftMatched[li] = true
				rightMatched[ri] = true
			}
		} else if how == Left || how == Outer {
			pairs = append(pairs, rowPair{li, -1})
			leftMatched[li] = true
		}
	}

	// For right/outer join, add unmatched right rows.
	if how == Right || how == Outer {
		for ri := 0; ri < right.Len(); ri++ {
			if !rightMatched[ri] {
				pairs = append(pairs, rowPair{-1, ri})
			}
		}
	}

	// Build records from pairs.
	records := make([]map[string]any, len(pairs))
	for i, p := range pairs {
		rec := make(map[string]any)

		// Key columns — prefer left, fallback to right.
		for _, col := range on {
			if p.leftIdx >= 0 {
				v, _ := left.At(p.leftIdx, col)
				rec[col] = v
			} else {
				v, _ := right.At(p.rightIdx, col)
				rec[col] = v
			}
		}

		// Left non-key columns.
		for _, col := range leftNonKey {
			if p.leftIdx >= 0 {
				v, _ := left.At(p.leftIdx, col)
				rec[col] = v
			} else {
				rec[col] = nil
			}
		}

		// Right non-key columns.
		for _, col := range rightNonKey {
			if p.rightIdx >= 0 {
				v, _ := right.At(p.rightIdx, col)
				rec[col] = v
			} else {
				rec[col] = nil
			}
		}

		records[i] = rec
	}

	return dataframe.FromRecords(records)
}

// buildKeyMap builds a map from composite key string to row indices for a DataFrame.
func buildKeyMap(df dataframe.DataFrame, on []string) map[string][]int {
	m := make(map[string][]int)
	for i := 0; i < df.Len(); i++ {
		key := extractKey(df, i, on)
		m[key] = append(m[key], i)
	}
	return m
}

// extractKey returns a composite key string for a row by joining column values.
func extractKey(df dataframe.DataFrame, row int, on []string) string {
	parts := make([]string, len(on))
	for i, col := range on {
		v, _ := df.At(row, col)
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "\x00")
}

// nonKeyCols returns columns from cols that are not in the on set.
func nonKeyCols(cols []string, on []string) []string {
	onSet := make(map[string]bool, len(on))
	for _, k := range on {
		onSet[k] = true
	}
	var out []string
	for _, c := range cols {
		if !onSet[c] {
			out = append(out, c)
		}
	}
	return out
}
