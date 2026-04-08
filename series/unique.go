package series

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vinaychitepu/gopandas/arrowutil"
	"github.com/vinaychitepu/gopandas/index"
)

// Unique returns a new Series containing only the unique values, preserving first-appearance order.
// The index is reset to a new RangeIndex.
func (s Series[T]) Unique() Series[T] {
	n := s.Len()
	seen := make(map[string]bool)
	var vals []T

	for i := 0; i < n; i++ {
		if arrowutil.IsNull(s.arr, i) {
			key := "<null>"
			if !seen[key] {
				seen[key] = true
				var zero T
				vals = append(vals, zero)
			}
			continue
		}
		v := getTypedValue[T](s.arr, i)
		key := fmt.Sprintf("%v", v)
		if !seen[key] {
			seen[key] = true
			vals = append(vals, v)
		}
	}
	if vals == nil {
		vals = []T{}
	}
	newIdx := index.NewRangeIndex(len(vals), "")
	return New[T](memory.DefaultAllocator, vals, newIdx, s.name)
}

// ValueCounts returns a Series[int64] with the count of each unique value.
// The index labels are the unique values (as strings), sorted by count descending.
func (s Series[T]) ValueCounts() Series[int64] {
	n := s.Len()

	type entry struct {
		key   string
		count int64
	}
	counts := make(map[string]*entry)
	var order []string

	for i := 0; i < n; i++ {
		var key string
		if arrowutil.IsNull(s.arr, i) {
			key = "NaN"
		} else {
			val, _ := arrowutil.GetValue(s.arr, i)
			key = fmt.Sprintf("%v", val)
		}
		if e, ok := counts[key]; ok {
			e.count++
		} else {
			counts[key] = &entry{key: key, count: 1}
			order = append(order, key)
		}
	}

	sort.SliceStable(order, func(i, j int) bool {
		ci, cj := counts[order[i]].count, counts[order[j]].count
		if ci != cj {
			return ci > cj
		}
		return order[i] < order[j]
	})

	labels := make([]string, len(order))
	values := make([]int64, len(order))
	for i, key := range order {
		labels[i] = key
		values[i] = counts[key].count
	}

	idx := index.NewStringIndex(labels, s.name)
	return New[int64](memory.DefaultAllocator, values, idx, s.name)
}
