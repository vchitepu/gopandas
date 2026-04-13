package groupby

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
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

// Groups returns a map from composite key to a copy of the row positions in that group.
func (gb GroupBy) Groups() map[string][]int {
	result := make(map[string][]int, len(gb.groups))
	for k, v := range gb.groups {
		cp := make([]int, len(v))
		copy(cp, v)
		result[k] = cp
	}
	return result
}

// sortedGroupKeys returns the group keys in sorted order.
func (gb GroupBy) sortedGroupKeys() []string {
	keys := make([]string, 0, len(gb.groups))
	for k := range gb.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Size returns a Series[int64] indexed by group keys, where each value is the number of rows in that group.
func (gb GroupBy) Size() series.Series[int64] {
	keys := gb.sortedGroupKeys()
	labels := make([]string, len(keys))
	values := make([]int64, len(keys))
	for i, k := range keys {
		labels[i] = k
		values[i] = int64(len(gb.groups[k]))
	}
	idx := index.NewStringIndex(labels, "")
	return series.New[int64](memory.DefaultAllocator, values, idx, "size")
}

// subDF creates a sub-DataFrame from the given row positions using dataframe.FromRecords.
func (gb GroupBy) subDF(positions []int) (dataframe.DataFrame, error) {
	cols := gb.df.Columns()
	records := make([]map[string]any, len(positions))
	for i, pos := range positions {
		rec := make(map[string]any, len(cols))
		for _, col := range cols {
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
