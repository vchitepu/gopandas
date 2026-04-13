package viz

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func RenderBar(df dataframe.DataFrame, opts ChartOptions, th Theme, termWidth int) string {
	if df.Len() == 0 || len(df.Columns()) == 0 {
		return "No data to chart"
	}

	labels, err := df.Col(opts.XCol)
	if err != nil {
		return "No data to chart"
	}
	values, err := df.Col(opts.YCol)
	if err != nil {
		return "No data to chart"
	}

	type row struct {
		label string
		value float64
		text  string
	}

	rows := make([]row, 0, df.Len())
	maxLabelWidth := 0
	maxValueWidth := 1
	maxAbs := 0.0
	const maxValueColumnWidth = 12

	for i := 0; i < df.Len(); i++ {
		labelVal, labelNull := labels.At(i)
		valueVal, valueNull := values.At(i)

		label := "<null>"
		if !labelNull {
			label = fmt.Sprintf("%v", labelVal)
		}

		value := 0.0
		if !valueNull {
			value = toFloat64(valueVal)
		}
		valueText := formatFloat(value)
		if runeLen(valueText) > maxValueColumnWidth {
			valueText = truncateCell(valueText, maxValueColumnWidth)
		}

		if w := runeLen(label); w > maxLabelWidth {
			maxLabelWidth = w
		}
		if w := runeLen(valueText); w > maxValueWidth {
			maxValueWidth = w
		}

		absVal := math.Abs(value)
		if absVal > maxAbs {
			maxAbs = absVal
		}

		rows = append(rows, row{label: label, value: value, text: valueText})
	}

	if termWidth <= 0 {
		termWidth = 80
	}

	valueWidth := maxValueWidth
	if valueWidth > maxValueColumnWidth {
		valueWidth = maxValueColumnWidth
	}
	if valueWidth < 1 {
		valueWidth = 1
	}
	if maxAllowed := termWidth - 3; maxAllowed > 0 && valueWidth > maxAllowed {
		valueWidth = maxAllowed
	}

	available := termWidth - valueWidth - 2
	if available < 1 {
		available = 1
	}

	labelWidth := maxLabelWidth
	maxLabelBudget := available - 1
	if maxLabelBudget < 0 {
		maxLabelBudget = 0
	}
	if labelWidth > maxLabelBudget {
		labelWidth = maxLabelBudget
	}

	barWidth := available - labelWidth
	if barWidth < 1 {
		barWidth = 1
	}

	posStyle := th.Chart
	negStyle := th.Chart.Faint(true)

	var b strings.Builder
	if strings.TrimSpace(opts.Title) != "" {
		title := truncateCell(opts.Title, termWidth)
		b.WriteString(th.Section.Render(title))
		b.WriteString("\n")
	}

	for i, r := range rows {
		label := truncateCell(r.label, labelWidth)
		valueText := truncateCell(r.text, valueWidth)

		bar := "▏"
		if r.value != 0 && maxAbs > 0 {
			length := int(math.Round((math.Abs(r.value) / maxAbs) * float64(barWidth)))
			if length < 1 {
				length = 1
			}
			if r.value > 0 {
				bar = posStyle.Render(strings.Repeat("█", length))
			} else {
				bar = negStyle.Render(strings.Repeat("░", length))
			}
		}

		line := fmt.Sprintf("%-*s %s %*s", labelWidth, label, bar, valueWidth, valueText)
		b.WriteString(line)
		if i < len(rows)-1 {
			b.WriteString("\n")
		}
	}

	return b.String()
}

func RenderHistogram(df dataframe.DataFrame, opts ChartOptions, th Theme, termWidth int) string {
	if df.Len() == 0 || len(df.Columns()) == 0 {
		return "No data to chart"
	}

	col, err := df.Col(opts.XCol)
	if err != nil {
		return "No data to chart"
	}

	values := make([]float64, 0, df.Len())
	for i := 0; i < df.Len(); i++ {
		v, isNull := col.At(i)
		if isNull {
			continue
		}
		values = append(values, toFloat64(v))
	}
	if len(values) == 0 {
		return "No data to chart"
	}

	bins := opts.Bins
	if bins <= 0 {
		bins = 10
	}
	if termWidth <= 0 {
		termWidth = 80
	}

	minVal := values[0]
	maxVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	counts := make([]int, bins)
	if maxVal == minVal {
		counts[0] = len(values)
	} else {
		binWidth := (maxVal - minVal) / float64(bins)
		for _, v := range values {
			idx := int((v - minVal) / binWidth)
			if idx >= bins {
				idx = bins - 1
			}
			if idx < 0 {
				idx = 0
			}
			counts[idx]++
		}
	}

	maxCount := 0
	for _, c := range counts {
		if c > maxCount {
			maxCount = c
		}
	}
	if maxCount == 0 {
		return "No data to chart"
	}

	const plotRows = 8
	blocks := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	labelWidth := runeLen(strconv.Itoa(maxCount))
	if labelWidth < 1 {
		labelWidth = 1
	}

	prefixWidth := labelWidth + 2 // "<label> |"
	maxDisplayBins := maxHistogramDisplayBins(termWidth, prefixWidth)
	if bins > maxDisplayBins {
		counts = aggregateHistogramBins(counts, maxDisplayBins)
		bins = len(counts)

		maxCount = 0
		for _, c := range counts {
			if c > maxCount {
				maxCount = c
			}
		}
		if maxCount == 0 {
			return "No data to chart"
		}

		labelWidth = runeLen(strconv.Itoa(maxCount))
		if labelWidth < 1 {
			labelWidth = 1
		}
		prefixWidth = labelWidth + 2

		maxDisplayBins = maxHistogramDisplayBins(termWidth, prefixWidth)
		if bins > maxDisplayBins {
			counts = aggregateHistogramBins(counts, maxDisplayBins)
			bins = len(counts)

			maxCount = 0
			for _, c := range counts {
				if c > maxCount {
					maxCount = c
				}
			}
			if maxCount == 0 {
				return "No data to chart"
			}
			labelWidth = runeLen(strconv.Itoa(maxCount))
			if labelWidth < 1 {
				labelWidth = 1
			}
			prefixWidth = labelWidth + 2
		}
	}

	plotWidth := bins*2 - 1
	if plotWidth < 1 {
		plotWidth = 1
	}

	barUnits := make([]int, bins)
	for i, c := range counts {
		units := int(math.Round((float64(c) / float64(maxCount)) * float64(plotRows*8)))
		if c > 0 && units == 0 {
			units = 1
		}
		barUnits[i] = units
	}

	var b strings.Builder
	if strings.TrimSpace(opts.Title) != "" {
		title := opts.Title
		if termWidth > 0 {
			title = truncateCell(title, termWidth)
		}
		b.WriteString(th.Section.Render(title))
		b.WriteString("\n")
	}

	for row := plotRows; row >= 1; row-- {
		threshold := int(math.Round((float64(maxCount) / float64(plotRows)) * float64(row)))
		if threshold < 0 {
			threshold = 0
		}
		label := fmt.Sprintf("%*d", labelWidth, threshold)
		b.WriteString(th.Chart.Faint(true).Render(label))
		b.WriteString(" |")

		for i := 0; i < bins; i++ {
			remaining := barUnits[i] - (row-1)*8
			ch := ' '
			switch {
			case remaining >= 8:
				ch = '█'
			case remaining > 0:
				ch = blocks[remaining-1]
			}
			b.WriteRune(ch)
			if i < bins-1 {
				b.WriteRune(' ')
			}
		}

		b.WriteString("\n")
	}

	b.WriteString(strings.Repeat(" ", labelWidth))
	b.WriteString(" +")
	b.WriteString(strings.Repeat("─", plotWidth))
	b.WriteString("\n")

	labelLine := make([]rune, plotWidth)
	for i := range labelLine {
		labelLine[i] = ' '
	}

	type axisLabel struct {
		pos  int
		text string
	}
	axisLabels := []axisLabel{
		{pos: 0, text: formatFloat(minVal)},
		{pos: plotWidth / 2, text: formatFloat((minVal + maxVal) / 2)},
		{pos: plotWidth - 1, text: formatFloat(maxVal)},
	}
	for _, lbl := range axisLabels {
		r := []rune(lbl.text)
		start := lbl.pos - len(r)/2
		if start < 0 {
			start = 0
		}
		if start+len(r) > len(labelLine) {
			start = len(labelLine) - len(r)
			if start < 0 {
				start = 0
			}
		}
		for i := 0; i < len(r) && start+i < len(labelLine); i++ {
			labelLine[start+i] = r[i]
		}
	}

	b.WriteString(strings.Repeat(" ", labelWidth))
	b.WriteString("  ")
	b.WriteString(th.Chart.Faint(true).Render(string(labelLine)))

	return b.String()
}

func RenderLine(df dataframe.DataFrame, opts ChartOptions, th Theme, termWidth int) string {
	return "[line chart placeholder]"
}

func aggregateHistogramBins(counts []int, targetBins int) []int {
	if targetBins <= 0 || len(counts) == 0 || targetBins >= len(counts) {
		out := make([]int, len(counts))
		copy(out, counts)
		return out
	}

	aggregated := make([]int, targetBins)
	ratio := float64(len(counts)) / float64(targetBins)

	for i, c := range counts {
		idx := int(float64(i) / ratio)
		if idx >= targetBins {
			idx = targetBins - 1
		}
		aggregated[idx] += c
	}

	return aggregated
}

func maxHistogramDisplayBins(termWidth, prefixWidth int) int {
	plotWidthBudget := termWidth - prefixWidth
	if plotWidthBudget < 1 {
		plotWidthBudget = 1
	}

	maxDisplayBins := (plotWidthBudget + 1) / 2 // one block plus one spacer
	if maxDisplayBins < 1 {
		maxDisplayBins = 1
	}

	return maxDisplayBins
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err == nil {
			return f
		}
	}

	return 0
}

func formatFloat(v float64) string {
	if math.Abs(v) < 1e-9 {
		return "0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
