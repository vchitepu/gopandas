package excel

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/xuri/excelize/v2"
)

func createTestXLSX(t *testing.T) *bytes.Buffer {
	t.Helper()

	f := excelize.NewFile()
	defer f.Close()

	sheet := "Employees"
	idx, err := f.NewSheet(sheet)
	if err != nil {
		t.Fatalf("NewSheet(Employees): %v", err)
	}
	f.SetActiveSheet(idx)
	if sheet != "Sheet1" {
		if err := f.DeleteSheet("Sheet1"); err != nil {
			t.Fatalf("DeleteSheet(Sheet1): %v", err)
		}
	}

	f.SetCellStr(sheet, "A1", "name")
	f.SetCellStr(sheet, "B1", "age")
	f.SetCellStr(sheet, "C1", "salary")
	f.SetCellStr(sheet, "D1", "hire_date")

	f.SetCellStr(sheet, "A2", "Alice")
	f.SetCellInt(sheet, "B2", 30)
	f.SetCellFloat(sheet, "C2", 95000.50, 2, 64)
	f.SetCellStr(sheet, "D2", "2019-03-15")

	f.SetCellStr(sheet, "A3", "Bob")
	f.SetCellInt(sheet, "B3", 25)
	f.SetCellFloat(sheet, "C3", 72000.00, 2, 64)
	f.SetCellStr(sheet, "D3", "2020-07-01")

	f.SetCellStr(sheet, "A4", "Charlie")
	f.SetCellInt(sheet, "B4", 35)
	f.SetCellFloat(sheet, "C4", 68000.75, 2, 64)
	f.SetCellStr(sheet, "D4", "2018-11-20")

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatalf("write xlsx: %v", err)
	}

	return &buf
}

func createMultiSheetXLSX(t *testing.T) *bytes.Buffer {
	t.Helper()

	f := excelize.NewFile()
	defer f.Close()

	f.SetCellStr("Sheet1", "A1", "col1")
	f.SetCellStr("Sheet1", "A2", "sheet1_value")

	if _, err := f.NewSheet("Sales"); err != nil {
		t.Fatalf("NewSheet(Sales): %v", err)
	}
	f.SetCellStr("Sales", "A1", "product")
	f.SetCellStr("Sales", "B1", "units")
	f.SetCellStr("Sales", "A2", "Widget")
	f.SetCellInt("Sales", "B2", 100)
	f.SetCellStr("Sales", "A3", "Gadget")
	f.SetCellInt("Sales", "B3", 200)

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatalf("write xlsx: %v", err)
	}

	return &buf
}

func TestFromXLSX_Simple(t *testing.T) {
	xlsxBuf := createTestXLSX(t)

	df, err := FromXLSX(xlsxBuf)
	if err != nil {
		t.Fatalf("FromXLSX: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 || cols != 4 {
		t.Fatalf("Shape = (%d, %d), want (3, 4)", rows, cols)
	}

	colNames := df.Columns()
	wantCols := []string{"name", "age", "salary", "hire_date"}
	for i, want := range wantCols {
		if colNames[i] != want {
			t.Errorf("column[%d] = %q, want %q", i, colNames[i], want)
		}
	}

	dtypes := df.DTypes()
	if dtypes["name"] != dtype.String {
		t.Errorf("name dtype = %v, want String", dtypes["name"])
	}
	if dtypes["age"] != dtype.Int64 {
		t.Errorf("age dtype = %v, want Int64", dtypes["age"])
	}
	if dtypes["salary"] != dtype.Float64 {
		t.Errorf("salary dtype = %v, want Float64", dtypes["salary"])
	}
	if dtypes["hire_date"] != dtype.Timestamp {
		t.Errorf("hire_date dtype = %v, want Timestamp", dtypes["hire_date"])
	}

	val, err := df.At(0, "name")
	if err != nil {
		t.Fatalf("At(0, name): %v", err)
	}
	if val != "Alice" {
		t.Errorf("At(0, name) = %v, want Alice", val)
	}

	val, err = df.At(1, "age")
	if err != nil {
		t.Fatalf("At(1, age): %v", err)
	}
	if val != int64(25) {
		t.Errorf("At(1, age) = %v, want 25", val)
	}

	val, err = df.At(0, "salary")
	if err != nil {
		t.Fatalf("At(0, salary): %v", err)
	}
	if val != 95000.50 {
		t.Errorf("At(0, salary) = %v, want 95000.50", val)
	}

	val, err = df.At(0, "hire_date")
	if err != nil {
		t.Fatalf("At(0, hire_date): %v", err)
	}
	tm, ok := val.(time.Time)
	if !ok {
		t.Fatalf("hire_date value type = %T, want time.Time", val)
	}
	want := time.Date(2019, 3, 15, 0, 0, 0, 0, time.UTC)
	if !tm.Equal(want) {
		t.Errorf("hire_date value = %v, want %v", tm, want)
	}
}

func TestFromXLSX_SheetByName(t *testing.T) {
	xlsxBuf := createMultiSheetXLSX(t)

	df, err := FromXLSX(xlsxBuf, WithSheetName("Sales"))
	if err != nil {
		t.Fatalf("FromXLSX with SheetName: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 2 {
		t.Fatalf("Shape = (%d, %d), want (2, 2)", rows, cols)
	}

	val, err := df.At(0, "product")
	if err != nil {
		t.Fatalf("At(0, product): %v", err)
	}
	if val != "Widget" {
		t.Errorf("At(0, product) = %v, want Widget", val)
	}
}

func TestFromXLSX_SheetByIndex(t *testing.T) {
	xlsxBuf := createMultiSheetXLSX(t)

	df, err := FromXLSX(xlsxBuf, WithSheetIndex(1))
	if err != nil {
		t.Fatalf("FromXLSX with SheetIndex: %v", err)
	}

	rows, _ := df.Shape()
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}

	val, err := df.At(1, "product")
	if err != nil {
		t.Fatalf("At(1, product): %v", err)
	}
	if val != "Gadget" {
		t.Errorf("At(1, product) = %v, want Gadget", val)
	}
}

func TestFromXLSX_SheetNamePrecedenceOverIndex(t *testing.T) {
	xlsxBuf := createMultiSheetXLSX(t)

	df, err := FromXLSX(xlsxBuf, WithSheetName("Sales"), WithSheetIndex(0))
	if err != nil {
		t.Fatalf("FromXLSX with both SheetName and SheetIndex: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 2 || cols != 2 {
		t.Fatalf("Shape = (%d, %d), want (2, 2)", rows, cols)
	}

	val, err := df.At(0, "product")
	if err != nil {
		t.Fatalf("At(0, product): %v", err)
	}
	if val != "Widget" {
		t.Errorf("At(0, product) = %v, want Widget", val)
	}
}

func TestFromXLSX_BadSheetName(t *testing.T) {
	xlsxBuf := createTestXLSX(t)

	_, err := FromXLSX(xlsxBuf, WithSheetName("NonExistent"))
	if err == nil {
		t.Fatal("expected error for bad sheet name, got nil")
	}
	if !strings.Contains(err.Error(), "sheet \"NonExistent\" not found") {
		t.Fatalf("error = %q, want sheet not found", err)
	}
}

func TestFromXLSX_SheetIndexOutOfRange(t *testing.T) {
	xlsxBuf := createTestXLSX(t)

	_, err := FromXLSX(xlsxBuf, WithSheetIndex(99))
	if err == nil {
		t.Fatal("expected error for sheet index out of range, got nil")
	}
	if !strings.Contains(err.Error(), "sheet index 99 out of range") {
		t.Fatalf("error = %q, want out of range", err)
	}
}

func TestFromXLSX_EmptySheet(t *testing.T) {
	f := excelize.NewFile()
	defer f.Close()

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatalf("write empty xlsx: %v", err)
	}

	_, err := FromXLSX(&buf)
	if err == nil {
		t.Fatal("expected error for empty sheet, got nil")
	}
	if !strings.Contains(err.Error(), "empty sheet") {
		t.Fatalf("error = %q, want empty sheet", err)
	}
}
