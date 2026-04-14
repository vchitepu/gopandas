package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gopandas",
	Short: "A command-line data analysis tool",
	Long:  "gopandas is a CLI for reading, filtering, aggregating, and converting data files (CSV, JSON, Parquet, XLSX).",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
