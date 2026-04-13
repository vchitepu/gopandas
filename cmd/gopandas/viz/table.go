package viz

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/series"
)

func RenderTable(df dataframe.DataFrame, th Theme, termWidth int) string {
	if len(df.Columns()) == 0 || df.Len() == 0 {
		return "Empty DataFrame"
	}

	cols := df.Columns()
	dtypes := df.DTypes()
	rows := rowCells(df, cols)

	if len(rows) > 50 {
		head := rows[:25]
		tail := rows[len(rows)-25:]
		omitted := len(rows) - 50
		msg := fmt.Sprintf("... %d rows omitted ...", omitted)
		gap := make([]string, len(cols))
		gap[0] = msg
		rows = append(head, append([][]string{gap}, tail...)...)
	}

	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = runeLen(col)
	}

	for _, row := range rows {
		for i, cell := range row {
			cellWidth := runeLen(cell)
			if cellWidth > widths[i] {
				widths[i] = cellWidth
			}
		}
	}

	widths = fitWidths(widths, termWidth)

	top := hSep("┌", "┬", "┐", widths)
	mid := hSep("├", "┼", "┤", widths)
	bot := hSep("└", "┴", "┘", widths)

	var b strings.Builder
	b.WriteString(th.Table.Render(top))
	b.WriteString("\n")

	var header []string
	for i, col := range cols {
		header = append(header, padCell(col, widths[i], false))
	}
	b.WriteString(th.TableHeader.Render("│" + strings.Join(header, "│") + "│"))
	b.WriteString("\n")
	b.WriteString(th.Table.Render(mid))
	b.WriteString("\n")

	for r, row := range rows {
		parts := make([]string, len(cols))
		for i, cell := range row {
			rightAlign := dtypes[cols[i]] == dtype.Int64 || dtypes[cols[i]] == dtype.Float64
			if strings.Contains(cell, "rows omitted") {
				rightAlign = false
			}
			parts[i] = padCell(cell, widths[i], rightAlign)
		}
		b.WriteString(th.Table.Render("│" + strings.Join(parts, "│") + "│"))
		if r < len(rows)-1 {
			b.WriteString("\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(th.Table.Render(bot))

	return b.String()
}

func rowCells(df dataframe.DataFrame, cols []string) [][]string {
	seriesByCol := make([]*series.Series[any], len(cols))
	for i, col := range cols {
		s, err := df.Col(col)
		if err != nil {
			continue
		}
		seriesByCol[i] = s
	}

	rows := make([][]string, 0, df.Len())
	for i := 0; i < df.Len(); i++ {
		row := make([]string, 0, len(cols))
		for j := range cols {
			s := seriesByCol[j]
			if s == nil {
				row = append(row, "<null>")
				continue
			}
			val, isNull := s.At(i)
			if isNull {
				row = append(row, "<null>")
				continue
			}
			row = append(row, fmt.Sprintf("%v", val))
		}
		rows = append(rows, row)
	}
	return rows
}

func hSep(left, mid, right string, widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("─", w+2)
	}
	return left + strings.Join(parts, mid) + right
}

func padCell(s string, width int, rightAlign bool) string {
	s = truncateCell(s, width)
	padding := width - runeLen(s)
	if padding < 0 {
		padding = 0
	}
	if rightAlign {
		return " " + strings.Repeat(" ", padding) + s + " "
	}
	return " " + s + strings.Repeat(" ", padding) + " "
}

func truncateCell(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if runeLen(s) <= width {
		return s
	}
	if width == 1 {
		return "…"
	}
	runes := []rune(s)
	return string(runes[:width-1]) + "…"
}

func runeLen(s string) int {
	return utf8.RuneCountInString(s)
}

func fitWidths(widths []int, termWidth int) []int {
	out := make([]int, len(widths))
	copy(out, widths)

	if termWidth <= 0 {
		return out
	}

	for tableTotalWidth(out) > termWidth {
		maxIdx := -1
		for i := range out {
			if out[i] > 6 && (maxIdx == -1 || out[i] > out[maxIdx]) {
				maxIdx = i
			}
		}
		if maxIdx == -1 {
			break
		}
		out[maxIdx]--
	}

	return out
}

func tableTotalWidth(widths []int) int {
	total := 1
	for _, w := range widths {
		total += w + 3
	}
	return total
}
