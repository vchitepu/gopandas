package viz

import (
	"strings"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/dtype"
)

func testDF(t *testing.T) dataframe.DataFrame {
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

func TestRenderUnknownType(t *testing.T) {
	df := testDF(t)

	_, err := Render(df, VizOptions{Type: "pie"}, 80, true)
	if err == nil {
		t.Fatal("expected error for unknown viz type")
	}
	if !strings.Contains(err.Error(), "unknown viz type") {
		t.Fatalf("expected unknown viz type error, got: %v", err)
	}
}

func TestRenderEmptyTypeFallsBack(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != df.String() {
		t.Fatalf("expected plain dataframe string fallback")
	}
}

func TestRenderNotTTYFallsBack(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{Type: "table"}, 80, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != df.String() {
		t.Fatalf("expected plain dataframe string fallback")
	}
}

func TestResolveXDefault(t *testing.T) {
	df := testDF(t)

	col, err := resolveColumn(df, "", "x", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := df.Columns()[0]
	if col != want {
		t.Fatalf("expected first column %q, got %q", want, col)
	}
}

func TestResolveYNumericDefault(t *testing.T) {
	df := testDF(t)

	col, err := resolveColumn(df, "", "y", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dt := df.DTypes()[col]
	if dt != dtype.Int64 && dt != dtype.Float64 {
		t.Fatalf("expected numeric dtype, got %s for column %q", dt, col)
	}
}

func TestResolveColumnNotFound(t *testing.T) {
	df := testDF(t)

	_, err := resolveColumn(df, "missing", "x", false)
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got: %v", err)
	}
}

func TestResolveColumnNotNumeric(t *testing.T) {
	df := testDF(t)

	_, err := resolveColumn(df, "name", "y", true)
	if err == nil {
		t.Fatal("expected not numeric error")
	}
	if !strings.Contains(err.Error(), "not numeric") {
		t.Fatalf("expected not numeric error, got: %v", err)
	}
}
