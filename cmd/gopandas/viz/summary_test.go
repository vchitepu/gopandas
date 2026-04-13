package viz

import (
	"regexp"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/vchitepu/gopandas/lib/dataframe"
)

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string {
	return ansiRE.ReplaceAllString(s, "")
}

func makeSummaryDF(t *testing.T) dataframe.DataFrame {
	t.Helper()

	df, err := dataframe.New(map[string]any{
		"id":    []int64{1, 2, 3, 4, 5, 6},
		"name":  []string{"r1", "r2", "r3", "r4", "r5", "r6"},
		"score": []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	return df
}

func TestRenderSummaryOuterPanelChars(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	for _, token := range []string{"┌", "┐", "└", "┘", "│"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain %q, got: %q", token, out)
		}
	}
}

func TestRenderSummaryFilenamePresent(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "employees.csv", DarkTheme(), 100))
	if !strings.Contains(out, "employees.csv") {
		t.Fatalf("expected filename in output, got: %q", out)
	}
}

func TestRenderSummaryShapeLinePresent(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	if !strings.Contains(out, "6 rows × 3 columns") {
		t.Fatalf("expected shape line in output, got: %q", out)
	}
}

func TestRenderSummaryColumnsSectionPresent(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	for _, token := range []string{"COLUMNS", "id: int64", "name: string", "score: float64"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain %q, got: %q", token, out)
		}
	}
}

func TestRenderSummaryStatisticsSectionHasCountAndMean(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	for _, token := range []string{"STATISTICS", "count", "mean"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain %q, got: %q", token, out)
		}
	}
}

func TestRenderSummaryPreviewShowsFirstFiveRowsOnly(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	if !strings.Contains(out, "PREVIEW") {
		t.Fatalf("expected PREVIEW section, got: %q", out)
	}
	for _, token := range []string{"r1", "r2", "r3", "r4", "r5"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected preview to contain %q, got: %q", token, out)
		}
	}
	if strings.Contains(out, "r6") {
		t.Fatalf("expected preview to exclude 6th row token r6, got: %q", out)
	}
}

func TestRenderSummaryStatisticsNumericOnlyBehavior(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 100))
	start := strings.Index(out, "STATISTICS")
	end := strings.Index(out, "PREVIEW")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("could not isolate statistics section in output: %q", out)
	}

	stats := out[start:end]
	if strings.Contains(stats, "name") {
		t.Fatalf("expected non-numeric column name to be excluded from stats section, got: %q", stats)
	}
	for _, token := range []string{"id", "score"} {
		if !strings.Contains(stats, token) {
			t.Fatalf("expected stats section to contain numeric column %q, got: %q", token, stats)
		}
	}
}

func TestRenderSummaryNarrowWidthRenders(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 16))
	if strings.TrimSpace(out) == "" {
		t.Fatal("expected non-empty output for narrow width")
	}
	if !strings.Contains(out, "┌") || !strings.Contains(out, "┘") {
		t.Fatalf("expected bordered panel output for narrow width, got: %q", out)
	}
}

func TestRenderSummaryRespectsTermWidthCapForNarrowTerm(t *testing.T) {
	tinyWidth := 24
	longFilename := "this-is-a-very-long-filename-that-should-not-expand-summary.csv"
	out := stripANSI(RenderSummary(makeSummaryDF(t), longFilename, DarkTheme(), tinyWidth))

	maxLineWidth := 0
	for _, line := range strings.Split(out, "\n") {
		if w := lipgloss.Width(line); w > maxLineWidth {
			maxLineWidth = w
		}
	}

	const tolerance = 2
	if maxLineWidth > tinyWidth+tolerance {
		t.Fatalf("expected max line width <= %d, got %d; output: %q", tinyWidth+tolerance, maxLineWidth, out)
	}
}

func TestRenderSummarySectionSeparatorUsesSpecShape(t *testing.T) {
	out := stripANSI(RenderSummary(makeSummaryDF(t), "data.csv", DarkTheme(), 80))
	for _, token := range []string{"├", "┤"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain section separator token %q, got: %q", token, out)
		}
	}
}

func TestRenderSummaryNoNumericColumnsShowsExplicitMessage(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"name": []string{"a", "b", "c"},
		"city": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := stripANSI(RenderSummary(df, "strings.csv", DarkTheme(), 100))
	if !strings.Contains(out, "(no numeric columns)") {
		t.Fatalf("expected explicit no-numeric-columns message, got: %q", out)
	}
}
