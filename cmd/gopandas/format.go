package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vinaychitepu/gopandas/dataframe"
	csvio "github.com/vinaychitepu/gopandas/dataio/csv"
	jsonio "github.com/vinaychitepu/gopandas/dataio/json"
	parquetio "github.com/vinaychitepu/gopandas/dataio/parquet"
)

// inferFormat returns the file format based on the file extension.
func inferFormat(path string) (string, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return "csv", nil
	case ".json":
		return "json", nil
	case ".parquet", ".pq":
		return "parquet", nil
	default:
		return "", fmt.Errorf("unsupported file extension %q (use --format to specify)", ext)
	}
}

// loadFile reads a data file and returns a DataFrame.
func loadFile(path, format string) (dataframe.DataFrame, error) {
	switch format {
	case "csv":
		f, err := os.Open(path)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return csvio.FromCSV(f)

	case "json":
		f, err := os.Open(path)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return jsonio.FromJSON(f, jsonio.OrientRecords)

	case "parquet":
		f, err := os.Open(path)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return parquetio.FromParquet(f)

	default:
		return dataframe.DataFrame{}, fmt.Errorf("unsupported format %q", format)
	}
}

// writeFile writes a DataFrame to a file in the specified format.
func writeFile(df dataframe.DataFrame, path, format string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	switch format {
	case "csv":
		return csvio.ToCSV(df, f)
	case "json":
		return jsonio.ToJSON(df, f, jsonio.OrientRecords)
	case "parquet":
		return parquetio.ToParquet(df, f)
	default:
		return fmt.Errorf("unsupported output format %q", format)
	}
}
