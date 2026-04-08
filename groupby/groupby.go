package groupby

import (
	"fmt"
	"strings"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// GroupBy holds the result of grouping a DataFrame by one or more key columns.
type GroupBy struct {
	df     dataframe.DataFrame
	keys   []string
	groups map[string][]int
}

// NewGroupBy creates a GroupBy from a DataFrame and one or more key column names.
func NewGroupBy(df dataframe.DataFrame, keys ...string) GroupBy {
	gb := GroupBy{df: df, keys: keys}
	gb.buildGroups()
	return gb
}

// buildGroups scans each row, builds a composite key, and records the row positions per group.
func (gb *GroupBy) buildGroups() {
	gb.groups = make(map[string][]int)
	nRows := gb.df.Len()
	for i := 0; i < nRows; i++ {
		key := gb.compositeKey(i)
		gb.groups[key] = append(gb.groups[key], i)
	}
}

// compositeKey creates a pipe-separated string key from the key columns for a given row.
func (gb *GroupBy) compositeKey(row int) string {
	parts := make([]string, len(gb.keys))
	for j, col := range gb.keys {
		val, err := gb.df.At(row, col)
		if err != nil {
			parts[j] = "<nil>"
		} else {
			parts[j] = fmt.Sprintf("%v", val)
		}
	}
	return strings.Join(parts, "|")
}

// NGroups returns the number of distinct groups.
func (gb GroupBy) NGroups() int {
	return len(gb.groups)
}
