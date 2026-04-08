package csv

import (
	"github.com/vinaychitepu/gopandas/dtype"
)

// csvConfig holds all configuration for CSV reading/writing.
type csvConfig struct {
	sep           rune
	header        bool
	indexCol      string
	useCols       []string
	naValues      map[string]bool
	nRows         int // 0 means read all
	skipRows      int
	dtypeOverride map[string]dtype.DType
}

func defaultConfig() csvConfig {
	return csvConfig{
		sep:           ',',
		header:        true,
		naValues:      map[string]bool{"": true, "NA": true, "NaN": true, "null": true, "<NA>": true},
		dtypeOverride: make(map[string]dtype.DType),
	}
}

// CSVOption is a functional option for CSV operations.
type CSVOption func(*csvConfig)

// WithSep sets the field separator (default: ',').
func WithSep(sep rune) CSVOption {
	return func(c *csvConfig) { c.sep = sep }
}

// WithHeader controls whether the first row is a header (default: true).
func WithHeader(has bool) CSVOption {
	return func(c *csvConfig) { c.header = has }
}

// WithIndexCol sets which column to use as the index.
func WithIndexCol(col string) CSVOption {
	return func(c *csvConfig) { c.indexCol = col }
}

// WithUseCols limits which columns to read.
func WithUseCols(cols []string) CSVOption {
	return func(c *csvConfig) { c.useCols = cols }
}

// WithNAValues sets the strings to interpret as NA/null.
func WithNAValues(vals []string) CSVOption {
	return func(c *csvConfig) {
		c.naValues = make(map[string]bool, len(vals))
		for _, v := range vals {
			c.naValues[v] = true
		}
	}
}

// WithNRows limits the number of data rows to read (0 = all).
func WithNRows(n int) CSVOption {
	return func(c *csvConfig) { c.nRows = n }
}

// WithSkipRows skips the first n data rows (after header).
func WithSkipRows(n int) CSVOption {
	return func(c *csvConfig) { c.skipRows = n }
}

// WithDTypeOverride forces a column to a specific dtype instead of inferring.
func WithDTypeOverride(col string, d dtype.DType) CSVOption {
	return func(c *csvConfig) { c.dtypeOverride[col] = d }
}
