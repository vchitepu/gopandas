package viz

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func makeRenderTableDF(t *testing.T) dataframe.DataFrame {
	t.Helper()

	df, err := dataframe.New(map[string]any{
		"name":   []string{"Alice", "Bob", "Charlie"},
		"age":    []int64{30, 25, 35},
		"salary": []float64{75000.5, 82000.0, 68000.75},
	})
	if err != nil {
		t.Fatalf("failed to build test dataframe: %v", err)
	}

	return df
}

func TestRenderTableBordersPresent(t *testing.T) {
	out := RenderTable(makeRenderTableDF(t), DarkTheme(), 120)

	for _, token := range []string{"┌", "┬", "┐", "│", "├", "┼", "┤", "└", "┴", "┘"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected output to contain border token %q", token)
		}
	}
}

func TestRenderTableHeadersPresent(t *testing.T) {
	out := RenderTable(makeRenderTableDF(t), DarkTheme(), 120)

	for _, header := range []string{"age", "name", "salary"} {
		if !strings.Contains(out, header) {
			t.Fatalf("expected output to contain header %q", header)
		}
	}
}

func TestRenderTableDataRowsPresent(t *testing.T) {
	out := RenderTable(makeRenderTableDF(t), DarkTheme(), 120)

	for _, rowToken := range []string{"Alice", "Bob", "Charlie"} {
		if !strings.Contains(out, rowToken) {
			t.Fatalf("expected output to contain row token %q", rowToken)
		}
	}
}

func TestRenderTableHeaderSeparatorPresent(t *testing.T) {
	out := RenderTable(makeRenderTableDF(t), DarkTheme(), 120)

	if !strings.Contains(out, "├") || !strings.Contains(out, "┼") || !strings.Contains(out, "┤") {
		t.Fatalf("expected output to contain header separator line, got: %q", out)
	}
}

func TestRenderTableRowTruncationIndicator(t *testing.T) {
	ids := make([]int64, 60)
	names := make([]string, 60)
	for i := 0; i < 60; i++ {
		ids[i] = int64(i + 1)
		names[i] = fmt.Sprintf("row-%d", i+1)
	}

	df, err := dataframe.New(map[string]any{
		"id":   ids,
		"name": names,
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderTable(df, DarkTheme(), 120)
	if !strings.Contains(out, "rows omitted") {
		t.Fatalf("expected output to include rows omitted indicator")
	}
}

func TestRenderTableNarrowWidthRenders(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"description": []string{"this is a very long sentence that should be truncated"},
		"value":       []int64{123456789},
	})
	if err != nil {
		t.Fatalf("failed to build dataframe: %v", err)
	}

	out := RenderTable(df, DarkTheme(), 20)
	if strings.TrimSpace(out) == "" {
		t.Fatal("expected non-empty output for narrow width")
	}
	if !strings.Contains(out, "┌") || !strings.Contains(out, "┘") {
		t.Fatalf("expected bordered table output for narrow width, got: %q", out)
	}
}
