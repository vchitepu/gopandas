package ops

import (
	"fmt"

	"github.com/vchitepu/gopandas/dataframe"
)

// Melt unpivots a wide DataFrame to long format.
// idVars are kept as-is; valueVars are melted into varName/valueName columns.
// Empty varName defaults to "variable", empty valueName defaults to "value".
func Melt(df dataframe.DataFrame, idVars, valueVars []string, varName, valueName string) (dataframe.DataFrame, error) {
	if varName == "" {
		varName = "variable"
	}
	if valueName == "" {
		valueName = "value"
	}

	// Validate columns exist.
	for _, col := range idVars {
		if _, err := df.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Melt: id column %q not found", col)
		}
	}
	for _, col := range valueVars {
		if _, err := df.Col(col); err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("ops.Melt: value column %q not found", col)
		}
	}

	// Build records: nRows * nValueVars rows.
	nRows := df.Len()
	records := make([]map[string]any, 0, nRows*len(valueVars))

	for _, vv := range valueVars {
		for i := 0; i < nRows; i++ {
			rec := make(map[string]any, len(idVars)+2)
			// Copy id vars.
			for _, id := range idVars {
				v, _ := df.At(i, id)
				rec[id] = v
			}
			rec[varName] = vv
			val, _ := df.At(i, vv)
			rec[valueName] = val
			records = append(records, rec)
		}
	}

	return dataframe.FromRecords(records)
}
