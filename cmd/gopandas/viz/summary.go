package viz

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/vchitepu/gopandas/lib/dataframe"
)

func RenderSummary(df dataframe.DataFrame, filename string, th Theme, termWidth int) string {
	innerHint := termWidth - 4
	if innerHint < 1 {
		innerHint = 1
	}

	lines := []string{
		filename,
		fmt.Sprintf("%d rows × %d columns", df.Len(), len(df.Columns())),
		"",
		sectionSep("COLUMNS", th, innerHint),
	}

	dtypes := df.DTypes()
	for _, col := range df.Columns() {
		lines = append(lines, fmt.Sprintf("%s: %s", col, dtypes[col]))
	}

	lines = append(lines, "", sectionSep("STATISTICS", th, innerHint))
	desc := df.Describe()
	if len(desc.Columns()) == 0 {
		lines = append(lines, "(no numeric columns)")
	} else {
		lines = append(lines, "metrics: count, mean, std, min, max")
		lines = append(lines, strings.Split(RenderTable(desc, th, innerHint), "\n")...)
	}

	lines = append(lines, "", sectionSep("PREVIEW", th, innerHint))
	lines = append(lines, strings.Split(RenderTable(df.Head(5), th, innerHint), "\n")...)

	innerWidth := 1
	for _, line := range lines {
		if w := visibleLen(line); w > innerWidth {
			innerWidth = w
		}
	}

	top := "┌" + strings.Repeat("─", innerWidth+2) + "┐"
	bot := "└" + strings.Repeat("─", innerWidth+2) + "┘"

	var b strings.Builder
	b.WriteString(top)
	b.WriteString("\n")
	for i, line := range lines {
		b.WriteString("│ ")
		b.WriteString(padRight(line, innerWidth))
		b.WriteString(" │")
		if i < len(lines)-1 {
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")
	b.WriteString(bot)

	return th.Panel.Render(b.String())
}

func sectionSep(label string, th Theme, width int) string {
	styledLabel := th.SectionHeader.Render(label)
	if width <= 0 {
		return styledLabel
	}

	labelWidth := visibleLen(label) + 2
	if labelWidth >= width {
		return styledLabel
	}

	dashes := width - labelWidth
	left := dashes / 2
	right := dashes - left

	return strings.Repeat("─", left) + " " + styledLabel + " " + strings.Repeat("─", right)
}

func padRight(s string, width int) string {
	if width <= 0 {
		return ""
	}
	padding := width - visibleLen(s)
	if padding <= 0 {
		return s
	}
	return s + strings.Repeat(" ", padding)
}

func visibleLen(s string) int {
	return lipgloss.Width(s)
}
