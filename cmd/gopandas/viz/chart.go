package viz

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

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
	if df.Len() == 0 || len(df.Columns()) == 0 {
		return "No data to chart"
	}

	xCol, err := df.Col(opts.XCol)
	if err != nil {
		return "No data to chart"
	}
	yCol, err := df.Col(opts.YCol)
	if err != nil {
		return "No data to chart"
	}

	points := make([]point, 0, df.Len())
	for i := 0; i < df.Len(); i++ {
		xv, xNull := xCol.At(i)
		yv, yNull := yCol.At(i)
		if xNull || yNull {
			continue
		}
		points = append(points, point{x: toLineXFloat64(xv), y: toFloat64(yv)})
	}
	if len(points) == 0 {
		return "No data to chart"
	}

	sortPoints(points)

	minX, maxX := points[0].x, points[0].x
	minY, maxY := points[0].y, points[0].y
	for _, p := range points[1:] {
		if p.x < minX {
			minX = p.x
		}
		if p.x > maxX {
			maxX = p.x
		}
		if p.y < minY {
			minY = p.y
		}
		if p.y > maxY {
			maxY = p.y
		}
	}

	if termWidth <= 0 {
		termWidth = 80
	}

	yMaxLabel := formatFloat(maxY)
	yMidLabel := formatFloat((maxY + minY) / 2)
	yMinLabel := formatFloat(minY)

	yLabelWidth := runeLen(yMaxLabel)
	if w := runeLen(yMidLabel); w > yLabelWidth {
		yLabelWidth = w
	}
	if w := runeLen(yMinLabel); w > yLabelWidth {
		yLabelWidth = w
	}

	plotCols := termWidth - yLabelWidth - 3
	if plotCols < 1 {
		plotCols = 1
	}
	plotRows := 8

	dotWidth := plotCols * 2
	dotHeight := plotRows * 4

	canvas := make([][]uint8, plotRows)
	for y := range canvas {
		canvas[y] = make([]uint8, plotCols)
	}

	scaled := make([][2]int, len(points))
	for i, p := range points {
		dx := scaleToDots(p.x, minX, maxX, dotWidth)
		dy := scaleToDots(p.y, minY, maxY, dotHeight)
		drawY := (dotHeight - 1) - dy
		scaled[i] = [2]int{dx, drawY}
	}

	if len(scaled) == 1 {
		setBrailleDot(canvas, scaled[0][0], scaled[0][1])
	} else {
		for i := 1; i < len(scaled); i++ {
			plotLine(canvas, scaled[i-1][0], scaled[i-1][1], scaled[i][0], scaled[i][1])
		}
	}

	xMinLabel := formatFloat(minX)
	xMidLabel := formatFloat((minX + maxX) / 2)
	xMaxLabel := formatFloat(maxX)

	var b strings.Builder
	if strings.TrimSpace(opts.Title) != "" {
		title := truncateCell(opts.Title, termWidth)
		b.WriteString(th.Section.Render(title))
		b.WriteString("\n")
	}

	midRow := plotRows / 2
	for row := 0; row < plotRows; row++ {
		label := ""
		switch row {
		case 0:
			label = yMaxLabel
		case midRow:
			label = yMidLabel
		case plotRows - 1:
			label = yMinLabel
		}

		cells := make([]rune, plotCols)
		for col := 0; col < plotCols; col++ {
			cells[col] = brailleBase + rune(canvas[row][col])
		}

		b.WriteString(th.Chart.Faint(true).Render(fmt.Sprintf("%*s", yLabelWidth, label)))
		b.WriteString(" |")
		b.WriteString(th.Chart.Render(string(cells)))
		b.WriteString("\n")
	}

	b.WriteString(strings.Repeat(" ", yLabelWidth))
	b.WriteString(" +")
	b.WriteString(strings.Repeat("─", plotCols))
	b.WriteString("\n")

	xAxis := make([]rune, plotCols)
	for i := range xAxis {
		xAxis[i] = ' '
	}
	placeAxisLabel(xAxis, 0, xMinLabel)
	placeAxisLabel(xAxis, plotCols/2, xMidLabel)
	placeAxisLabel(xAxis, plotCols-1, xMaxLabel)

	b.WriteString(strings.Repeat(" ", yLabelWidth))
	b.WriteString("  ")
	b.WriteString(th.Chart.Faint(true).Render(string(xAxis)))

	return b.String()
}

type point struct {
	x float64
	y float64
}

const brailleBase rune = 0x2800

var brailleDotBits = [4][2]uint8{
	{0x01, 0x08},
	{0x02, 0x10},
	{0x04, 0x20},
	{0x40, 0x80},
}

func sortPoints(points []point) {
	for i := 1; i < len(points); i++ {
		j := i
		for j > 0 && points[j-1].x > points[j].x {
			points[j-1], points[j] = points[j], points[j-1]
			j--
		}
	}
}

func plotLine(canvas [][]uint8, x0, y0, x1, y1 int) {
	dx := absInt(x1 - x0)
	sx := -1
	if x0 < x1 {
		sx = 1
	}
	dy := -absInt(y1 - y0)
	sy := -1
	if y0 < y1 {
		sy = 1
	}
	err := dx + dy

	for {
		setBrailleDot(canvas, x0, y0)
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := 2 * err
		if e2 >= dy {
			err += dy
			x0 += sx
		}
		if e2 <= dx {
			err += dx
			y0 += sy
		}
	}
}

func setBrailleDot(canvas [][]uint8, dotX, dotY int) {
	if len(canvas) == 0 || len(canvas[0]) == 0 {
		return
	}
	if dotX < 0 || dotY < 0 {
		return
	}

	cellX := dotX / 2
	cellY := dotY / 4
	if cellY < 0 || cellY >= len(canvas) || cellX < 0 || cellX >= len(canvas[cellY]) {
		return
	}

	localX := dotX % 2
	localY := dotY % 4
	canvas[cellY][cellX] |= brailleDotBits[localY][localX]
}

func scaleToDots(v, minV, maxV float64, dotCount int) int {
	if dotCount <= 1 || math.Abs(maxV-minV) < 1e-9 {
		return 0
	}
	pos := int(math.Round(((v - minV) / (maxV - minV)) * float64(dotCount-1)))
	if pos < 0 {
		return 0
	}
	if pos >= dotCount {
		return dotCount - 1
	}
	return pos
}

func placeAxisLabel(line []rune, pos int, text string) {
	if len(line) == 0 {
		return
	}
	r := []rune(text)
	if len(r) == 0 {
		return
	}

	start := pos - len(r)/2
	if start < 0 {
		start = 0
	}
	if start+len(r) > len(line) {
		start = len(line) - len(r)
		if start < 0 {
			start = 0
		}
	}

	for i := 0; i < len(r) && start+i < len(line); i++ {
		line[start+i] = r[i]
	}
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
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

func toLineXFloat64(v any) float64 {
	if ts, ok := v.(time.Time); ok {
		return float64(ts.UnixNano())
	}

	return toFloat64(v)
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
