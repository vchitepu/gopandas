package viz

import (
	"strings"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

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
