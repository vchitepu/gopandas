package excel

// xlsxConfig holds configuration for XLSX reading/writing.
type xlsxConfig struct {
	sheetName  string // empty means "use default"
	sheetIndex int    // -1 means "not set"
}

func defaultConfig() xlsxConfig {
	return xlsxConfig{
		sheetName:  "",
		sheetIndex: -1,
	}
}

// XLSXOption is a functional option for XLSX operations.
type XLSXOption func(*xlsxConfig)

// WithSheetName selects a sheet by name for reading,
// or sets the sheet name for writing (default: "Sheet1").
func WithSheetName(name string) XLSXOption {
	return func(c *xlsxConfig) { c.sheetName = name }
}

// WithSheetIndex selects a sheet by 0-based index for reading.
func WithSheetIndex(i int) XLSXOption {
	return func(c *xlsxConfig) { c.sheetIndex = i }
}
