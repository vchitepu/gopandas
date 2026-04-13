package viz

import (
	"strings"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func maxLineRuneLen(s string) int {
	maxLen := 0
	for _, line := range strings.Split(s, "\n") {
		if l := runeLen(line); l > maxLen {
			maxLen = l
		}
	}
	return maxLen
}

func makeRenderBarDF(t *testing.T) dataframe.DataFrame {
	t.Helper()

	df, err := dataframe.New(map[string]any{
		"label": []string{"Alpha", "Beta", "Gamma", "Zero"},
		"value": []int64{10, 5, -2, 0},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	return df
}

func TestRenderBarContainsBlockChars(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)

	for _, token := range []string{"█", "░", "▏"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain %q, got: %q", token, out)
		}
	}
}

func TestRenderBarContainsLabels(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)

	for _, label := range []string{"Alpha", "Beta", "Gamma", "Zero"} {
		if !strings.Contains(out, label) {
			t.Fatalf("expected output to contain label %q", label)
		}
	}
}

func TestRenderBarContainsValues(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)

	for _, val := range []string{"10", "5", "-2", "0"} {
		if !strings.Contains(out, val) {
			t.Fatalf("expected output to contain value %q", val)
		}
	}
}

func TestRenderBarProportionalDifferences(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)

	lines := strings.Split(out, "\n")
	alphaBlocks := 0
	betaBlocks := 0

	for _, line := range lines {
		if strings.Contains(line, "Alpha") {
			alphaBlocks = strings.Count(line, "█")
		}
		if strings.Contains(line, "Beta") {
			betaBlocks = strings.Count(line, "█")
		}
	}

	if alphaBlocks <= betaBlocks {
		t.Fatalf("expected Alpha bar to be longer than Beta, got alpha=%d beta=%d", alphaBlocks, betaBlocks)
	}
}

func TestRenderBarTitlePresence(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value", Title: "Sample Bar Chart"}, Theme{}, 80)

	if !strings.Contains(out, "Sample Bar Chart") {
		t.Fatalf("expected title to be rendered, got: %q", out)
	}
}

func TestRenderBarTruncatesLongTitleToTerminalWidth(t *testing.T) {
	out := RenderBar(
		makeRenderBarDF(t),
		ChartOptions{XCol: "label", YCol: "value", Title: "VeryLong📊TitleThatShouldBeTrimmed"},
		Theme{},
		10,
	)

	lines := strings.Split(out, "\n")
	if len(lines) == 0 {
		t.Fatalf("expected output lines, got: %q", out)
	}

	titleLine := lines[0]
	if runeLen(titleLine) > 10 {
		t.Fatalf("expected title width <= 10 runes, got %d in %q", runeLen(titleLine), titleLine)
	}
	if !strings.Contains(titleLine, "…") {
		t.Fatalf("expected truncated title with ellipsis, got: %q", titleLine)
	}
}

func TestRenderBarZeroValueMarker(t *testing.T) {
	out := RenderBar(makeRenderBarDF(t), ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)

	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Zero") {
			if !strings.Contains(line, "▏") {
				t.Fatalf("expected zero row to include ▏ marker, got: %q", line)
			}
			return
		}
	}

	t.Fatal("expected to find Zero row in output")
}

func TestRenderBarEmptyDataFrameMessage(t *testing.T) {
	df, err := dataframe.New(map[string]any{"label": []string{}, "value": []int64{}})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderBar(df, ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 80)
	if out != "No data to chart" {
		t.Fatalf("expected empty dataframe message, got %q", out)
	}
}

func TestRenderBarNarrowWidthKeepsMarkers(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"label": []string{"Alpha", "Beta", "Gamma", "Zero"},
		"value": []int64{10, -3, 4, 0},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderBar(df, ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 12)

	for _, token := range []string{"█", "░", "▏"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain %q under narrow width, got: %q", token, out)
		}
	}
}

func TestRenderBarTruncatesLongLabelWithEllipsis(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"label": []string{"SuperLongLabelNameThatShouldBeTrimmed"},
		"value": []int64{7},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderBar(df, ChartOptions{XCol: "label", YCol: "value"}, Theme{}, 18)
	if !strings.Contains(out, "…") {
		t.Fatalf("expected truncated label with ellipsis, got: %q", out)
	}
}

func makeRenderHistogramDF(t *testing.T) dataframe.DataFrame {
	t.Helper()

	df, err := dataframe.New(map[string]any{
		"value": []float64{0, 1, 1.1, 2.2, 2.3, 3.9, 4.4, 5.8, 6.2, 7.6, 8.1, 9.5},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	return df
}

func TestRenderHistogramContainsBlockChars(t *testing.T) {
	out := RenderHistogram(makeRenderHistogramDF(t), ChartOptions{XCol: "value", Bins: 6}, Theme{}, 80)

	if !strings.ContainsAny(out, "▁▂▃▄▅▆▇█") {
		t.Fatalf("expected output to contain histogram block chars, got: %q", out)
	}
}

func TestRenderHistogramContainsAxisAndBoundaryLabels(t *testing.T) {
	out := RenderHistogram(makeRenderHistogramDF(t), ChartOptions{XCol: "value", Bins: 6}, Theme{}, 80)

	if !strings.Contains(out, "─") {
		t.Fatalf("expected x-axis line to be present, got: %q", out)
	}
	for _, token := range []string{"0", "4.75", "9.5"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected boundary label %q to be present, got: %q", token, out)
		}
	}
}

func TestRenderHistogramTitlePresence(t *testing.T) {
	out := RenderHistogram(makeRenderHistogramDF(t), ChartOptions{XCol: "value", Bins: 6, Title: "Value Distribution"}, Theme{}, 80)

	if !strings.Contains(out, "Value Distribution") {
		t.Fatalf("expected title to be rendered, got: %q", out)
	}
}

func TestRenderHistogramDefaultBinsAndNonEmptyOutput(t *testing.T) {
	out := RenderHistogram(makeRenderHistogramDF(t), ChartOptions{XCol: "value", Bins: 0}, Theme{}, 80)

	if strings.TrimSpace(out) == "" {
		t.Fatalf("expected non-empty histogram output")
	}
	if strings.Count(out, "─") < 10 {
		t.Fatalf("expected default 10-bin axis width, got output: %q", out)
	}
}

func TestRenderHistogramEmptyDataFrameMessage(t *testing.T) {
	df, err := dataframe.New(map[string]any{"value": []float64{}})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderHistogram(df, ChartOptions{XCol: "value", Bins: 8}, Theme{}, 80)
	if out != "No data to chart" {
		t.Fatalf("expected empty dataframe message, got %q", out)
	}
}

func TestRenderHistogramNarrowWidthDoesNotOverflow(t *testing.T) {
	values := make([]float64, 0, 120)
	for i := 0; i < 120; i++ {
		values = append(values, float64(i%40)+float64(i)/200)
	}

	df, err := dataframe.New(map[string]any{"value": values})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	const termWidth = 24
	out := RenderHistogram(df, ChartOptions{XCol: "value", Bins: 64}, Theme{}, termWidth)

	if got := maxLineRuneLen(out); got > termWidth {
		t.Fatalf("expected max line width <= %d, got %d\noutput:\n%s", termWidth, got, out)
	}
}

func TestRenderHistogramKeepsBlocksAfterBinAggregation(t *testing.T) {
	values := make([]float64, 0, 90)
	for i := 0; i < 90; i++ {
		values = append(values, float64(i%30))
	}

	df, err := dataframe.New(map[string]any{"value": values})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderHistogram(df, ChartOptions{XCol: "value", Bins: 48}, Theme{}, 22)

	if !strings.ContainsAny(out, "▁▂▃▄▅▆▇█") {
		t.Fatalf("expected output to contain histogram blocks after aggregation, got: %q", out)
	}
}

func makeRenderLineDF(t *testing.T) dataframe.DataFrame {
	t.Helper()

	df, err := dataframe.New(map[string]any{
		"x": []float64{0, 5, 10},
		"y": []float64{1, 3, 5},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	return df
}

func containsBraille(s string) bool {
	for _, r := range s {
		if r > 0x2800 && r <= 0x28FF {
			return true
		}
	}

	return false
}

func countRaisedBrailleCells(s string) int {
	count := 0
	for _, r := range s {
		if r > brailleBase && r <= 0x28FF {
			count++
		}
	}

	return count
}

func TestRenderLineContainsBrailleChars(t *testing.T) {
	out := RenderLine(makeRenderLineDF(t), ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)

	if !containsBraille(out) {
		t.Fatalf("expected line chart output to contain Braille chars, got: %q", out)
	}
}

func TestRenderLineTitlePresence(t *testing.T) {
	out := RenderLine(makeRenderLineDF(t), ChartOptions{XCol: "x", YCol: "y", Title: "Line Trend"}, Theme{}, 80)

	if !strings.Contains(out, "Line Trend") {
		t.Fatalf("expected title to be rendered, got: %q", out)
	}
}

func TestRenderLineContainsAxisLabels(t *testing.T) {
	out := RenderLine(makeRenderLineDF(t), ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)

	for _, token := range []string{"5", "3", "1", "0", "10"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected axis label %q to be present, got: %q", token, out)
		}
	}
}

func TestRenderLineEmptyDataFrameMessage(t *testing.T) {
	df, err := dataframe.New(map[string]any{"x": []float64{}, "y": []float64{}})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderLine(df, ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)
	if out != "No data to chart" {
		t.Fatalf("expected empty dataframe message, got %q", out)
	}
}

func TestRenderLineSinglePointRendersBraille(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"x": []float64{7},
		"y": []float64{11},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderLine(df, ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)
	if !containsBraille(out) {
		t.Fatalf("expected single-point line chart to contain Braille chars, got: %q", out)
	}
}

func TestRenderLineUnsortedXMatchesSortedOutput(t *testing.T) {
	sortedDF, err := dataframe.New(map[string]any{
		"x": []float64{0, 2, 4, 6, 8},
		"y": []float64{1, 5, 3, 7, 4},
	})
	if err != nil {
		t.Fatalf("failed to build sorted dataframe: %v", err)
	}

	unsortedDF, err := dataframe.New(map[string]any{
		"x": []float64{6, 0, 8, 2, 4},
		"y": []float64{7, 1, 4, 5, 3},
	})
	if err != nil {
		t.Fatalf("failed to build unsorted dataframe: %v", err)
	}

	sortedOut := RenderLine(sortedDF, ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)
	unsortedOut := RenderLine(unsortedDF, ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)

	if unsortedOut != sortedOut {
		t.Fatalf("expected unsorted x input to render same chart as sorted input\nsorted:\n%s\nunsorted:\n%s", sortedOut, unsortedOut)
	}
}

func TestRenderLineDiagonalUsesMultipleBrailleCells(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"x": []float64{0, 1, 2, 3, 4, 5},
		"y": []float64{0, 1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderLine(df, ChartOptions{XCol: "x", YCol: "y"}, Theme{}, 80)
	raised := countRaisedBrailleCells(out)

	if raised < 3 {
		t.Fatalf("expected diagonal line to raise multiple braille cells, got %d\noutput:\n%s", raised, out)
	}
}
