package excel

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/vchitepu/gopandas/lib/dataframe"
	"github.com/xuri/excelize/v2"
)

func TestToXLSX_Simple(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"name":   []string{"Alice", "Bob"},
		"age":    []int64{30, 25},
		"salary": []float64{95000.50, 72000.00},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var buf bytes.Buffer
	if err := ToXLSX(df, &buf); err != nil {
		t.Fatalf("ToXLSX: %v", err)
	}

	f, err := excelize.OpenReader(&buf)
	if err != nil {
		t.Fatalf("excelize.OpenReader: %v", err)
	}
	defer f.Close()

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		t.Fatalf("GetRows: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}

	header := rows[0]
	headerIndex := make(map[string]int, len(header))
	for i, h := range header {
		headerIndex[h] = i
	}

	hasName, hasAge, hasSalary := false, false, false
	for _, h := range header {
		switch h {
		case "name":
			hasName = true
		case "age":
			hasAge = true
		case "salary":
			hasSalary = true
		}
	}
	if !hasName || !hasAge || !hasSalary {
		t.Errorf("header = %v, want columns name, age, salary", header)
	}

	row1 := rows[1]
	ageIdx, nameIdx, salaryIdx := headerIndex["age"], headerIndex["name"], headerIndex["salary"]
	if row1[ageIdx] != "30" || row1[nameIdx] != "Alice" {
		t.Fatalf("row1 = %v, want age=30 and name=Alice", row1)
	}
	salary, err := strconv.ParseFloat(row1[salaryIdx], 64)
	if err != nil {
		t.Fatalf("ParseFloat(row1 salary %q): %v", row1[salaryIdx], err)
	}
	if salary != 95000.5 {
		t.Fatalf("row1 salary = %v, want 95000.5", salary)
	}
}

func TestToXLSX_CustomSheetName(t *testing.T) {
	df, err := dataframe.New(map[string]any{"x": []int64{1}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var buf bytes.Buffer
	if err := ToXLSX(df, &buf, WithSheetName("MyData")); err != nil {
		t.Fatalf("ToXLSX: %v", err)
	}

	f, err := excelize.OpenReader(&buf)
	if err != nil {
		t.Fatalf("excelize.OpenReader: %v", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	found := false
	for _, s := range sheets {
		if s == "MyData" {
			found = true
		}
	}
	if !found {
		t.Errorf("sheet list = %v, want to contain 'MyData'", sheets)
	}
}

func TestToXLSX_RoundTrip(t *testing.T) {
	xlsxBuf := createTestXLSX(t)

	df1, err := FromXLSX(xlsxBuf)
	if err != nil {
		t.Fatalf("FromXLSX (first): %v", err)
	}

	var writeBuf bytes.Buffer
	if err := ToXLSX(df1, &writeBuf); err != nil {
		t.Fatalf("ToXLSX: %v", err)
	}

	df2, err := FromXLSX(&writeBuf)
	if err != nil {
		t.Fatalf("FromXLSX (second): %v", err)
	}

	r1, c1 := df1.Shape()
	r2, c2 := df2.Shape()
	if r1 != r2 || c1 != c2 {
		t.Fatalf("shape mismatch: original (%d, %d) vs round-trip (%d, %d)", r1, c1, r2, c2)
	}

	cols1 := df1.Columns()
	cols2 := df2.Columns()
	for i := range cols1 {
		if cols1[i] != cols2[i] {
			t.Errorf("column %d: %q vs %q", i, cols1[i], cols2[i])
		}
	}

	val1, _ := df1.At(0, "name")
	val2, _ := df2.At(0, "name")
	if val1 != val2 {
		t.Errorf("At(0, name): original=%v, roundtrip=%v", val1, val2)
	}
}
