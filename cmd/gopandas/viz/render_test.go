package viz

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vchitepu/gopandas/lib/arrowutil"
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

	col, err := resolveColumn(df, "", "x", columnRequirementAny)
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

	col, err := resolveColumn(df, "", "y", columnRequirementNumeric)
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

	_, err := resolveColumn(df, "missing", "x", columnRequirementAny)
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got: %v", err)
	}
}

func TestResolveColumnNotNumeric(t *testing.T) {
	df := testDF(t)

	_, err := resolveColumn(df, "name", "y", columnRequirementNumeric)
	if err == nil {
		t.Fatal("expected not numeric error")
	}
	if !strings.Contains(err.Error(), "not numeric") {
		t.Fatalf("expected not numeric error, got: %v", err)
	}
}

func TestRenderDispatchesBarRoute(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{Type: "bar", XCol: "name", YCol: "age", Filename: "employees.csv"}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "employees.csv | name vs age") {
		t.Fatalf("expected bar output to include title, got %q", out)
	}
	if !strings.Contains(out, "█") {
		t.Fatalf("expected bar output to include block glyphs, got %q", out)
	}
}

func TestRenderDispatchesHistogramRoute(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{Type: "histogram", XCol: "salary", Filename: "employees.csv"}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == "[histogram placeholder]" {
		t.Fatalf("expected histogram renderer output, got placeholder %q", out)
	}
	if !strings.Contains(out, "employees.csv | distribution of salary") {
		t.Fatalf("expected histogram title in output, got %q", out)
	}
}

func TestRenderDispatchesLineRoute(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{Type: "line", XCol: "age", YCol: "salary", Filename: "employees.csv"}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == "[line chart placeholder]" {
		t.Fatalf("expected line renderer output, got placeholder %q", out)
	}
	if !strings.Contains(out, "employees.csv | age vs salary") {
		t.Fatalf("expected line chart title in output, got %q", out)
	}
	if !containsRaisedBraille(out) {
		t.Fatalf("expected line chart output to contain Braille glyphs, got %q", out)
	}
}

func TestRenderLineRejectsNonNumericAndNonTimestampX(t *testing.T) {
	df := testDF(t)

	_, err := Render(df, VizOptions{Type: "line", XCol: "name", YCol: "salary", Filename: "employees.csv"}, 80, true)
	if err == nil {
		t.Fatal("expected error for line chart with string x column")
	}
	if !strings.Contains(err.Error(), "must be numeric or timestamp") {
		t.Fatalf("expected numeric or timestamp x error, got: %v", err)
	}
}

func TestRenderLineAcceptsTimestampX(t *testing.T) {
	alloc := memory.DefaultAllocator
	times := []time.Time{
		time.Unix(1704067200, 0).UTC(),
		time.Unix(1704070800, 0).UTC(),
		time.Unix(1704074400, 0).UTC(),
	}
	whenArr := arrowutil.BuildTimestampArray(alloc, times)
	defer whenArr.Release()
	valueArr := arrowutil.BuildFloat64Array(alloc, []float64{1.2, 3.4, 2.1})
	defer valueArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "when", Type: whenArr.DataType()},
		{Name: "value", Type: valueArr.DataType()},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{whenArr, valueArr}, int64(len(times)))
	defer rec.Release()

	df, err := dataframe.FromArrow(rec)
	if err != nil {
		t.Fatalf("failed to build test dataframe: %v", err)
	}

	out, err := Render(df, VizOptions{Type: "line", XCol: "when", YCol: "value", Filename: "series.csv"}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == "[line chart placeholder]" {
		t.Fatalf("expected line renderer output, got placeholder %q", out)
	}
	if !strings.Contains(out, "series.csv | when vs value") {
		t.Fatalf("expected line chart title in output, got %q", out)
	}
}

func containsRaisedBraille(s string) bool {
	for _, r := range s {
		if r > 0x2800 && r <= 0x28FF {
			return true
		}
	}

	return false
}

func TestRenderHistogramBinsZeroUsesDefaultWithoutError(t *testing.T) {
	df := testDF(t)

	out, err := Render(df, VizOptions{Type: "histogram", XCol: "age", Bins: 0, Filename: "employees.csv"}, 80, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == "[histogram placeholder]" {
		t.Fatalf("expected rendered histogram output, got placeholder %q", out)
	}
	if !strings.Contains(out, "employees.csv | distribution of age") {
		t.Fatalf("expected histogram title in output, got %q", out)
	}
}

func TestBuildChartOptionsTitleFormatHistogram(t *testing.T) {
	df := testDF(t)

	chartOpts, err := buildChartOptions(df, VizOptions{Type: "histogram", XCol: "salary", Filename: "employees.csv"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chartOpts.Title != "employees.csv | distribution of salary" {
		t.Fatalf("expected histogram title format, got %q", chartOpts.Title)
	}
}

func TestBuildChartOptionsTitleFormatBar(t *testing.T) {
	df := testDF(t)

	chartOpts, err := buildChartOptions(df, VizOptions{Type: "bar", XCol: "name", YCol: "age", Filename: "employees.csv"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chartOpts.Title != "employees.csv | name vs age" {
		t.Fatalf("expected bar title format, got %q", chartOpts.Title)
	}
}
