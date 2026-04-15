package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vchitepu/gopandas/cmd/gopandas/viz"
	"github.com/vchitepu/gopandas/lib/dataframe"
	csvio "github.com/vchitepu/gopandas/lib/dataio/csv"
	"github.com/vchitepu/gopandas/lib/groupby"
	"golang.org/x/term"
)

// Flag variables for the read subcommand.
var (
	readHead       int
	readTail       int
	readDescribe   bool
	readShape      bool
	readDTypes     bool
	readSelect     string
	readFilter     string
	readGroupBy    string
	readAgg        string
	readSort       string
	readSortDesc   bool
	readOutput     string
	readFormat     string
	readParseDates string
	readDateFormat string
	readViz        string
	readX          string
	readY          string
	readBins       int
	readTheme      string
)

var (
	termIsTerminal = term.IsTerminal
	termGetSize    = term.GetSize
)

var readCmd = &cobra.Command{
	Use:   "read <file>",
	Short: "Read and display a data file",
	Long:  "Read a CSV, JSON, Parquet, or XLSX file and apply optional transformations (select, filter, groupby, sort).",
	Args:  cobra.ExactArgs(1),
	RunE:  runRead,
}

func init() {
	readCmd.Flags().IntVar(&readHead, "head", 0, "Print first N rows (default: 5 if no other display flag set)")
	readCmd.Flags().IntVar(&readTail, "tail", 0, "show last N rows")
	readCmd.Flags().BoolVar(&readDescribe, "describe", false, "show summary statistics")
	readCmd.Flags().BoolVar(&readShape, "shape", false, "show (rows, cols)")
	readCmd.Flags().BoolVar(&readDTypes, "dtypes", false, "show column data types")
	readCmd.Flags().StringVar(&readSelect, "select", "", "comma-separated columns to select")
	readCmd.Flags().StringVar(&readFilter, "filter", "", "filter expression (e.g. \"age > 30\")")
	readCmd.Flags().StringVar(&readGroupBy, "groupby", "", "column to group by")
	readCmd.Flags().StringVar(&readAgg, "agg", "", "aggregation: sum, mean, count, min, max")
	readCmd.Flags().StringVar(&readSort, "sort", "", "column to sort by")
	readCmd.Flags().BoolVar(&readSortDesc, "sort-desc", false, "sort descending")
	readCmd.Flags().StringVar(&readOutput, "output", "", "write result to file")
	readCmd.Flags().StringVar(&readFormat, "format", "", "output format: csv, json, parquet, xlsx (overrides extension)")
	readCmd.Flags().StringVar(&readParseDates, "parse-dates", "", "comma-separated CSV columns to parse as dates")
	readCmd.Flags().StringVar(&readDateFormat, "date-format", "", "CSV date format layout (Go time format), e.g. 01/02/2006")
	readCmd.Flags().StringVar(&readViz, "viz", "", "render visualization: bar, histogram, line, table, summary")
	readCmd.Flags().StringVar(&readX, "x", "", "x-axis column for --viz")
	readCmd.Flags().StringVar(&readY, "y", "", "y-axis column for --viz")
	readCmd.Flags().IntVar(&readBins, "bins", 10, "number of bins for histogram --viz")
	readCmd.Flags().StringVar(&readTheme, "theme", "", "viz theme: dark, light, auto")

	rootCmd.AddCommand(readCmd)
}

func runRead(cmd *cobra.Command, args []string) error {
	path := args[0]

	// Determine input format from extension
	format, err := inferFormat(path)
	if err != nil {
		return err
	}

	// Load the file
	var csvOpts []csvio.CSVOption
	if format == "csv" {
		if readParseDates != "" {
			cols := splitCSVList(readParseDates)
			if len(cols) > 0 {
				csvOpts = append(csvOpts, csvio.WithParseDates(cols))
			}
		}
		if readDateFormat != "" {
			csvOpts = append(csvOpts, csvio.WithDateFormats([]string{readDateFormat}))
		}
	}

	df, err := loadFile(path, format, csvOpts...)
	if err != nil {
		return err
	}

	// --- Pipeline: select → filter → groupby/agg → sort ---

	// Select columns
	if readSelect != "" {
		cols := strings.Split(readSelect, ",")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}
		df, err = df.Select(cols...)
		if err != nil {
			return fmt.Errorf("select: %w", err)
		}
	}

	// Filter
	if readFilter != "" {
		df, err = df.Query(readFilter)
		if err != nil {
			return fmt.Errorf("filter: %w", err)
		}
	}

	// GroupBy + Aggregation
	if readGroupBy != "" {
		gb := groupby.NewGroupBy(df, readGroupBy)
		df, err = applyAgg(gb, readAgg)
		if err != nil {
			return err
		}
	}

	// Sort
	if readSort != "" {
		ascending := []bool{!readSortDesc}
		df, err = df.SortBy([]string{readSort}, ascending)
		if err != nil {
			return fmt.Errorf("sort: %w", err)
		}
	}

	// --- Output ---

	// Write to file if requested
	if readOutput != "" {
		outFormat := readFormat
		if outFormat == "" {
			outFormat, err = inferFormat(readOutput)
			if err != nil {
				return err
			}
		}
		if err := writeFile(df, readOutput, outFormat); err != nil {
			return fmt.Errorf("output: %w", err)
		}
		return nil
	}

	// Display mode
	out := cmd.OutOrStdout()
	if readViz != "" {
		termWidth := 80
		isTTY := false
		if f, ok := out.(*os.File); ok {
			fd := int(f.Fd())
			if termIsTerminal(fd) {
				isTTY = true
				if w, _, err := termGetSize(fd); err == nil && w > 0 {
					termWidth = w
				}
			}
		}

		vizOut, err := viz.Render(df, viz.VizOptions{
			Type:      strings.ToLower(strings.TrimSpace(readViz)),
			XCol:      strings.TrimSpace(readX),
			YCol:      strings.TrimSpace(readY),
			Bins:      readBins,
			ThemeMode: strings.TrimSpace(readTheme),
			Filename:  filepath.Base(path),
		}, termWidth, isTTY)
		if err != nil {
			return err
		}

		fmt.Fprintln(out, vizOut)
		return nil
	}

	if readShape {
		rows, cols := df.Shape()
		fmt.Fprintf(out, "(%d, %d)\n", rows, cols)
		return nil
	}

	if readDTypes {
		dtypes := df.DTypes()
		for _, col := range df.Columns() {
			fmt.Fprintf(out, "%s: %s\n", col, dtypes[col])
		}
		return nil
	}

	if readDescribe {
		fmt.Fprintln(out, df.Describe().String())
		return nil
	}

	if readTail > 0 {
		fmt.Fprintln(out, df.Tail(readTail).String())
		return nil
	}

	if readHead > 0 {
		fmt.Fprintln(out, df.Head(readHead).String())
		return nil
	}

	// Default: head (5 if not specified).
	n := 5
	rows, _ := df.Shape()
	if n > rows {
		n = rows
	}
	fmt.Fprintln(out, df.Head(n).String())
	return nil
}

func splitCSVList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func applyAgg(gb groupby.GroupBy, agg string) (dataframe.DataFrame, error) {
	switch strings.ToLower(agg) {
	case "sum":
		return gb.Sum()
	case "mean":
		return gb.Mean()
	case "count":
		return gb.Count()
	case "min":
		return gb.Min()
	case "max":
		return gb.Max()
	case "":
		return gb.Count() // default
	default:
		return dataframe.DataFrame{}, fmt.Errorf("unsupported aggregation: %s (use sum, mean, count, min, max)", agg)
	}
}
