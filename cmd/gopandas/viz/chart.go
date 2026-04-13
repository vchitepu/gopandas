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
	return "[histogram placeholder]"
}

func RenderLine(df dataframe.DataFrame, opts ChartOptions, th Theme, termWidth int) string {
	return "[line chart placeholder]"
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
