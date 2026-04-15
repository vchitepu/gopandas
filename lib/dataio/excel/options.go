package excel

const unsetSheetIndex = -1

// xlsxConfig holds configuration for XLSX reading/writing.
type xlsxConfig struct {
	// sheetName is an explicit sheet selector when non-empty.
	// When both sheetName and sheetIndex are set, sheetName takes precedence.
	sheetName string
	// sheetIndex is a 0-based sheet selector.
	// unsetSheetIndex (-1) means "not set". Values < -1 are invalid.
	sheetIndex int
}

func defaultConfig() xlsxConfig {
	return xlsxConfig{
		sheetName:  "",
		sheetIndex: unsetSheetIndex,
	}
}

// XLSXOption is a functional option for XLSX operations.
type XLSXOption func(*xlsxConfig)

// WithSheetName sets an explicit sheet name.
//
// Read behavior contract:
//   - If set, readers must use this sheet name.
//   - If both name and index are set, name takes precedence.
//   - If unset (empty string), readers fall back to index/default selection.
//
// Write behavior contract:
//   - If set, writers must use this sheet name.
//   - If unset, writers use their default sheet name (currently "Sheet1").
func WithSheetName(name string) XLSXOption {
	return func(c *xlsxConfig) { c.sheetName = name }
}

// WithSheetIndex sets an explicit 0-based sheet index for reading.
//
// Read behavior contract:
//   - Used only when sheet name is not set.
//   - -1 means "not set" and falls back to reader default selection.
//   - Values < -1 are invalid and must not be treated as fallback values.
func WithSheetIndex(i int) XLSXOption {
	return func(c *xlsxConfig) { c.sheetIndex = i }
}
