package ops

import (
	"fmt"

	"github.com/vinaychitepu/gopandas/dataframe"
)

// Join performs an index-based join of two DataFrames.
// Overlapping column names get lsuffix/rsuffix appended.
func Join(left, right dataframe.DataFrame, how JoinType, lsuffix, rsuffix string) (dataframe.DataFrame, error) {
	leftLabels := left.Index().Labels()
	rightLabels := right.Index().Labels()

	// Build index map for right: label -> row index.
	rightIdxMap := make(map[string]int, len(rightLabels))
	for i, lbl := range rightLabels {
		key := fmt.Sprintf("%v", lbl)
		if _, exists := rightIdxMap[key]; !exists {
			rightIdxMap[key] = i
		}
	}

	// Build index map for left for right/outer joins.
	leftIdxMap := make(map[string]int, len(leftLabels))
	for i, lbl := range leftLabels {
		key := fmt.Sprintf("%v", lbl)
		if _, exists := leftIdxMap[key]; !exists {
			leftIdxMap[key] = i
		}
	}

	// Build row pairs.
	type rowPair struct {
		leftIdx  int // -1 = no match
		rightIdx int // -1 = no match
		label    any
	}
	var pairs []rowPair
	rightMatched := make(map[int]bool)

	for li, lbl := range leftLabels {
		key := fmt.Sprintf("%v", lbl)
		if ri, ok := rightIdxMap[key]; ok {
			pairs = append(pairs, rowPair{li, ri, lbl})
			rightMatched[ri] = true
		} else if how == Left || how == Outer {
			pairs = append(pairs, rowPair{li, -1, lbl})
		}
	}

	if how == Right || how == Outer {
		for ri, lbl := range rightLabels {
			if !rightMatched[ri] {
				pairs = append(pairs, rowPair{-1, ri, lbl})
			}
		}
	}

	// Determine output columns with suffix handling.
	leftCols := left.Columns()
	rightCols := right.Columns()

	// Find overlapping columns.
	leftColSet := make(map[string]bool, len(leftCols))
	for _, c := range leftCols {
		leftColSet[c] = true
	}
	overlap := make(map[string]bool)
	for _, c := range rightCols {
		if leftColSet[c] {
			overlap[c] = true
		}
	}

	// Build records.
	records := make([]map[string]any, len(pairs))
	for i, p := range pairs {
		rec := make(map[string]any)

		// Left columns.
		for _, col := range leftCols {
			outName := col
			if overlap[col] {
				outName = col + lsuffix
			}
			if p.leftIdx >= 0 {
				v, _ := left.At(p.leftIdx, col)
				rec[outName] = v
			} else {
				rec[outName] = nil
			}
		}

		// Right columns.
		for _, col := range rightCols {
			outName := col
			if overlap[col] {
				outName = col + rsuffix
			}
			if p.rightIdx >= 0 {
				v, _ := right.At(p.rightIdx, col)
				rec[outName] = v
			} else {
				rec[outName] = nil
			}
		}

		records[i] = rec
	}

	return dataframe.FromRecords(records)
}
