package viz

import (
	"fmt"
	"strings"

	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/vchitepu/gopandas/lib/dtype"
)

type VizOptions struct {
	Type      string
	XCol      string
	YCol      string
	Bins      int
	ThemeMode string
	Filename  string
}

type ChartOptions struct {
	Type  string
	XCol  string
	YCol  string
	Bins  int
	Title string
}

var validVizTypes = []string{"bar", "histogram", "line", "table", "summary"}

func Render(df dataframe.DataFrame, opts VizOptions, termWidth int, isTTY bool) (string, error) {
	if opts.Type == "" {
		return df.String(), nil
	}

	if !isTTY {
		return df.String(), nil
	}

	if !isValidVizType(opts.Type) {
		return "", fmt.Errorf("unknown viz type %q (supported: %s)", opts.Type, strings.Join(validVizTypes, ", "))
	}

	th := Detect(opts.ThemeMode)

	switch opts.Type {
	case "table":
		return RenderTable(df, th, termWidth), nil
	case "summary":
		return RenderSummary(df, opts.Filename, th, termWidth), nil
	case "bar", "histogram", "line":
		chartOpts, err := buildChartOptions(df, opts)
		if err != nil {
			return "", err
		}

		switch opts.Type {
		case "bar":
			return RenderBar(df, chartOpts, th, termWidth), nil
		case "histogram":
			return RenderHistogram(df, chartOpts, th, termWidth), nil
		case "line":
			return RenderLine(df, chartOpts, th, termWidth), nil
		}
	}

	return df.String(), nil
}

func buildChartOptions(df dataframe.DataFrame, opts VizOptions) (ChartOptions, error) {
	requireNumericX := opts.Type == "histogram"
	requireNumericY := opts.Type == "bar" || opts.Type == "line"

	xCol, err := resolveColumn(df, opts.XCol, "x", requireNumericX)
	if err != nil {
		return ChartOptions{}, err
	}

	yCol := ""
	if opts.Type != "histogram" {
		yCol, err = resolveColumn(df, opts.YCol, "y", requireNumericY)
		if err != nil {
			return ChartOptions{}, err
		}
	}

	bins := opts.Bins
	if bins <= 0 {
		bins = 10
	}

	var title string
	if opts.Type == "histogram" {
		title = fmt.Sprintf("%s | distribution of %s", opts.Filename, xCol)
	} else {
		title = fmt.Sprintf("%s | %s vs %s", opts.Filename, xCol, yCol)
	}

	return ChartOptions{
		Type:  opts.Type,
		XCol:  xCol,
		YCol:  yCol,
		Bins:  bins,
		Title: title,
	}, nil
}

func resolveColumn(df dataframe.DataFrame, explicit string, axis string, requireNumeric bool) (string, error) {
	columns := df.Columns()
	dtypes := df.DTypes()

	if explicit != "" {
		if _, ok := dtypes[explicit]; !ok {
			return "", fmt.Errorf("--%s column %q not found (available: %s)", axis, explicit, strings.Join(columns, ", "))
		}

		if requireNumeric {
			dt := dtypes[explicit]
			if dt != dtype.Int64 && dt != dtype.Float64 {
				return "", fmt.Errorf("--%s column %q is not numeric (dtype: %s)", axis, explicit, dt)
			}
		}

		return explicit, nil
	}

	if requireNumeric {
		for _, col := range columns {
			dt := dtypes[col]
			if dt == dtype.Int64 || dt == dtype.Float64 {
				return col, nil
			}
		}
		return "", fmt.Errorf("no numeric column found for --%s default (available: %s)", axis, strings.Join(columns, ", "))
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("DataFrame has no columns")
	}

	return columns[0], nil
}

func isValidVizType(vizType string) bool {
	for _, valid := range validVizTypes {
		if vizType == valid {
			return true
		}
	}

	return false
}
