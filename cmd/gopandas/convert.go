package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	convertFrom   string
	convertTo     string
	convertSelect string
)

var convertCmd = &cobra.Command{
	Use:   "convert <input> <output>",
	Short: "Convert between data file formats",
	Long:  "Convert a data file from one format to another (CSV, JSON, Parquet). Format is inferred from file extensions unless --from/--to are specified.",
	Args:  cobra.ExactArgs(2),
	RunE:  runConvert,
}

func init() {
	convertCmd.Flags().StringVar(&convertFrom, "from", "", "Input format: csv, json, parquet (default: infer from extension)")
	convertCmd.Flags().StringVar(&convertTo, "to", "", "Output format: csv, json, parquet (default: infer from extension)")
	convertCmd.Flags().StringVar(&convertSelect, "select", "", "Select columns (comma-separated)")

	rootCmd.AddCommand(convertCmd)
}

func runConvert(cmd *cobra.Command, args []string) error {
	inputPath := args[0]
	outputPath := args[1]

	inFormat := convertFrom
	if inFormat == "" {
		var err error
		inFormat, err = inferFormat(inputPath)
		if err != nil {
			return fmt.Errorf("cannot infer input format: %w (use --from to specify)", err)
		}
	}

	outFormat := convertTo
	if outFormat == "" {
		var err error
		outFormat, err = inferFormat(outputPath)
		if err != nil {
			return fmt.Errorf("cannot infer output format: %w (use --to to specify)", err)
		}
	}

	df, err := loadFile(inputPath, inFormat)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", inputPath, err)
	}

	if convertSelect != "" {
		cols := strings.Split(convertSelect, ",")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}
		df, err = df.Select(cols...)
		if err != nil {
			return fmt.Errorf("select: %w", err)
		}
	}

	if err := writeFile(df, outputPath, outFormat); err != nil {
		return fmt.Errorf("failed to write %s: %w", outputPath, err)
	}

	rows, cols := df.Shape()
	fmt.Fprintf(cmd.OutOrStdout(), "Converted %s (%s) -> %s (%s) [%d rows, %d cols]\n",
		inputPath, inFormat, outputPath, outFormat, rows, cols)
	return nil
}
