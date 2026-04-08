package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vinaychitepu/gopandas/dataframe"
	"github.com/vinaychitepu/gopandas/groupby"
)

// Flag variables for the read subcommand.
var (
	readHead     int
	readTail     int
	readDescribe bool
	readShape    bool
	readDTypes   bool
	readSelect   string
	readFilter   string
	readGroupBy  string
	readAgg      string
	readSort     string
	readSortDesc bool
	readOutput   string
	readFormat   string
)

var readCmd = &cobra.Command{
	Use:   "read <file>",
	Short: "Read and display a data file",
	Long:  "Read a CSV, JSON, or Parquet file and apply optional transformations (select, filter, groupby, sort).",
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
	readCmd.Flags().StringVar(&readFormat, "format", "", "output format: csv, json, parquet (overrides extension)")

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
	df, err := loadFile(path, format)
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
