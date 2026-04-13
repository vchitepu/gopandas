package viz

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/vchitepu/gopandas/lib/dataframe"
)

var summaryANSIRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func RenderSummary(df dataframe.DataFrame, filename string, th Theme, termWidth int) string {
	contentWidth := termWidth - 8
	if contentWidth < 1 {
		contentWidth = 1
	}

	lines := []string{
		truncateSummaryLine(filename, contentWidth),
		fmt.Sprintf("%d rows × %d columns", df.Len(), len(df.Columns())),
		"",
		sectionSep("COLUMNS", th, contentWidth),
	}

	dtypes := df.DTypes()
	for _, col := range df.Columns() {
		lines = append(lines, fmt.Sprintf("%s: %s", col, dtypes[col]))
	}

	lines = append(lines, "", sectionSep("STATISTICS", th, contentWidth))
	desc := df.Describe()
	if len(desc.Columns()) == 0 {
		lines = append(lines, "(no numeric columns)")
	} else {
		lines = append(lines, "metrics: count, mean, std, min, max")
		lines = append(lines, strings.Split(stripSummaryANSI(RenderTable(desc, th, contentWidth)), "\n")...)
	}

	lines = append(lines, "", sectionSep("PREVIEW", th, contentWidth))
	previewLines := strings.Split(stripSummaryANSI(RenderTable(df.Head(5), th, contentWidth)), "\n")
	previewLines = stripOuterTableBorders(previewLines)
	lines = append(lines, previewLines...)

	innerWidth := contentWidth
	for i, line := range lines {
		lines[i] = padRight(truncateSummaryLine(line, innerWidth), innerWidth)
	}

	top := "┌" + strings.Repeat("─", innerWidth+2) + "┐"
	bot := "└" + strings.Repeat("─", innerWidth+2) + "┘"

	var b strings.Builder
	b.WriteString(top)
	b.WriteString("\n")
	for i, line := range lines {
		b.WriteString("│ ")
		b.WriteString(line)
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
	if width <= 0 {
		return ""
	}

	if width == 1 {
		return "├"
	}
	if width == 2 {
		return "├┤"
	}

	maxLabelWidth := width - 7
	if maxLabelWidth < 1 {
		maxLabelWidth = 1
	}
	plainLabel := truncateSummaryLine(label, maxLabelWidth)
	labelWidth := visibleLen(plainLabel)
	dashes := width - (labelWidth + 6)
	if dashes < 1 {
		dashes = 1
	}

	styledLabel := th.SectionHeader.Render(plainLabel)
	sep := "├─ " + styledLabel + " " + strings.Repeat("─", dashes) + "┤"
	if visibleLen(stripSummaryANSI(sep)) > width {
		return truncateSummaryLine(stripSummaryANSI(sep), width)
	}
	return sep
}

func stripOuterTableBorders(lines []string) []string {
	if len(lines) <= 2 {
		return lines
	}
	return lines[1 : len(lines)-1]
}

func stripSummaryANSI(s string) string {
	return summaryANSIRE.ReplaceAllString(s, "")
}

func truncateSummaryLine(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if visibleLen(s) <= width {
		return s
	}

	plain := stripSummaryANSI(s)
	if visibleLen(plain) <= width {
		return plain
	}
	if width == 1 {
		return "…"
	}

	runes := []rune(plain)
	if len(runes) <= width {
		return plain
	}
	return string(runes[:width-1]) + "…"
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
