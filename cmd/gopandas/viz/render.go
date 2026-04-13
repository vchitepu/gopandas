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
	xRequirement := columnRequirementAny
	if opts.Type == "histogram" {
		xRequirement = columnRequirementNumeric
	}
	if opts.Type == "line" {
		xRequirement = columnRequirementNumericOrTimestamp
	}

	yRequirement := columnRequirementAny
	if opts.Type == "bar" || opts.Type == "line" {
		yRequirement = columnRequirementNumeric
	}

	xCol, err := resolveColumn(df, opts.XCol, "x", xRequirement)
	if err != nil {
		return ChartOptions{}, err
	}

	yCol := ""
	if opts.Type != "histogram" {
		yCol, err = resolveColumn(df, opts.YCol, "y", yRequirement)
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

type columnRequirement int

const (
	columnRequirementAny columnRequirement = iota
	columnRequirementNumeric
	columnRequirementNumericOrTimestamp
)

func resolveColumn(df dataframe.DataFrame, explicit string, axis string, requirement columnRequirement) (string, error) {
	columns := df.Columns()
	dtypes := df.DTypes()

	if explicit != "" {
		if _, ok := dtypes[explicit]; !ok {
			return "", fmt.Errorf("--%s column %q not found (available: %s)", axis, explicit, strings.Join(columns, ", "))
		}

		dt := dtypes[explicit]
		if !columnSatisfiesRequirement(dt, requirement) {
			switch requirement {
			case columnRequirementNumeric:
				return "", fmt.Errorf("--%s column %q is not numeric (dtype: %s)", axis, explicit, dt)
			case columnRequirementNumericOrTimestamp:
				return "", fmt.Errorf("--%s column %q must be numeric or timestamp (dtype: %s)", axis, explicit, dt)
			}
		}

		return explicit, nil
	}

	if requirement != columnRequirementAny {
		for _, col := range columns {
			dt := dtypes[col]
			if columnSatisfiesRequirement(dt, requirement) {
				return col, nil
			}
		}

		switch requirement {
		case columnRequirementNumeric:
			return "", fmt.Errorf("no numeric column found for --%s default (available: %s)", axis, strings.Join(columns, ", "))
		case columnRequirementNumericOrTimestamp:
			return "", fmt.Errorf("no numeric or timestamp column found for --%s default (available: %s)", axis, strings.Join(columns, ", "))
		}
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("DataFrame has no columns")
	}

	return columns[0], nil
}

func columnSatisfiesRequirement(dt dtype.DType, requirement columnRequirement) bool {
	switch requirement {
	case columnRequirementAny:
		return true
	case columnRequirementNumeric:
		return dt == dtype.Int64 || dt == dtype.Float64
	case columnRequirementNumericOrTimestamp:
		return dt == dtype.Int64 || dt == dtype.Float64 || dt == dtype.Timestamp
	default:
		return false
	}
}

func isValidVizType(vizType string) bool {
	for _, valid := range validVizTypes {
		if vizType == valid {
			return true
		}
	}

	return false
}
