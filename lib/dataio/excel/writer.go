package excel

import (
	"fmt"
	"io"
	"time"

	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/xuri/excelize/v2"
)

// ToXLSX writes df to w as an xlsx workbook with a single sheet.
// Use WithSheetName to set the sheet name (default: "Sheet1").
func ToXLSX(df dataframe.DataFrame, w io.Writer, opts ...XLSXOption) error {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	sheetName := cfg.sheetName
	if sheetName == "" {
		sheetName = "Sheet1"
	}

	f := excelize.NewFile()
	defer f.Close()

	defaultSheet := f.GetSheetName(0)
	if defaultSheet != sheetName {
		if err := f.SetSheetName(defaultSheet, sheetName); err != nil {
			return fmt.Errorf("excel.ToXLSX: rename sheet: %w", err)
		}
	}

	columns := df.Columns()
	nRows, _ := df.Shape()

	for c, colName := range columns {
		cell, err := excelize.CoordinatesToCellName(c+1, 1)
		if err != nil {
			return fmt.Errorf("excel.ToXLSX: header cell: %w", err)
		}
		if err := f.SetCellStr(sheetName, cell, colName); err != nil {
			return fmt.Errorf("excel.ToXLSX: write header %q: %w", colName, err)
		}
	}

	for r := 0; r < nRows; r++ {
		for c, colName := range columns {
			cell, err := excelize.CoordinatesToCellName(c+1, r+2)
			if err != nil {
				return fmt.Errorf("excel.ToXLSX: cell coord: %w", err)
			}

			val, err := df.At(r, colName)
			if err != nil {
				return fmt.Errorf("excel.ToXLSX: row %d, col %q: %w", r, colName, err)
			}

			if val == nil {
				continue
			}

			switch v := val.(type) {
			case int64:
				if err := f.SetCellInt(sheetName, cell, v); err != nil {
					return fmt.Errorf("excel.ToXLSX: write int: %w", err)
				}
			case float64:
				if err := f.SetCellFloat(sheetName, cell, v, -1, 64); err != nil {
					return fmt.Errorf("excel.ToXLSX: write float: %w", err)
				}
			case bool:
				if err := f.SetCellBool(sheetName, cell, v); err != nil {
					return fmt.Errorf("excel.ToXLSX: write bool: %w", err)
				}
			case time.Time:
				if err := f.SetCellValue(sheetName, cell, v); err != nil {
					return fmt.Errorf("excel.ToXLSX: write time: %w", err)
				}
			case string:
				if err := f.SetCellStr(sheetName, cell, v); err != nil {
					return fmt.Errorf("excel.ToXLSX: write string: %w", err)
				}
			default:
				if err := f.SetCellStr(sheetName, cell, fmt.Sprintf("%v", v)); err != nil {
					return fmt.Errorf("excel.ToXLSX: write default: %w", err)
				}
			}
		}
	}

	if _, err := f.WriteTo(w); err != nil {
		return fmt.Errorf("excel.ToXLSX: write: %w", err)
	}

	return nil
}
