package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vchitepu/gopandas/lib/dataframe"
	csvio "github.com/vchitepu/gopandas/lib/dataio/csv"
	excelio "github.com/vchitepu/gopandas/lib/dataio/excel"
	jsonio "github.com/vchitepu/gopandas/lib/dataio/json"
	parquetio "github.com/vchitepu/gopandas/lib/dataio/parquet"
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
	case ".xlsx":
		return "xlsx", nil
	default:
		return "", fmt.Errorf("unsupported file extension %q", ext)
	}
}

// loadFile reads a data file and returns a DataFrame.
func loadFile(path, format string, csvOpts ...csvio.CSVOption) (dataframe.DataFrame, error) {
	switch format {
	case "csv":
		f, err := os.Open(path)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return csvio.FromCSV(f, csvOpts...)

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

	case "xlsx":
		f, err := os.Open(path)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		return excelio.FromXLSX(f)

	default:
		return dataframe.DataFrame{}, fmt.Errorf("unsupported format %q", format)
	}
}

// writeFile writes a DataFrame to a file in the specified format.
// On error, it removes any partially written file.
func writeFile(df dataframe.DataFrame, path, format string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}

	writeErr := func() error {
		defer f.Close()
		switch format {
		case "csv":
			return csvio.ToCSV(df, f)
		case "json":
			return jsonio.ToJSON(df, f, jsonio.OrientRecords)
		case "parquet":
			return parquetio.ToParquet(df, f)
		case "xlsx":
			return excelio.ToXLSX(df, f)
		default:
			return fmt.Errorf("unsupported output format %q", format)
		}
	}()

	if writeErr != nil {
		os.Remove(path) // clean up partial/empty file
		return writeErr
	}
	return nil
}
