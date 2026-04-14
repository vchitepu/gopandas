# Design: Excel I/O (.xlsx) Support

**Date:** 2026-04-14  
**Status:** Approved

---

## Summary

Add read and write support for `.xlsx` files to gopandas, following the same package and API conventions as the existing `csv`, `json`, and `parquet` I/O packages. Also add rich sample testdata files and update the README examples to use them.

---

## Scope

- `lib/dataio/excel/` package: `FromXLSX` reader and `ToXLSX` writer
- CLI integration: `.xlsx` wired into existing `read` and `convert` commands via `format.go`
- Sample testdata: `employees.csv`, `employees.xlsx`, `sales.csv`
- README update: examples rewritten to use testdata files

**Out of scope:** multi-sheet export, advanced Excel formatting/styles, formula evaluation, `.xls` (legacy format).

---

## Dependency

**`github.com/xuri/excelize/v2`** — the standard Go Excel library. Pure Go, no CGo, well-maintained. Added to `go.mod`.

---

## Package Layout

```
lib/dataio/excel/
    options.go      # XLSXOption type + option funcs
    reader.go       # FromXLSX
    writer.go       # ToXLSX
    reader_test.go
    writer_test.go
```

---

## Public API

### Reader

```go
// FromXLSX reads an xlsx workbook from r and returns a DataFrame.
// By default, the first sheet is read. Use WithSheetName or WithSheetIndex to select another.
func FromXLSX(r io.Reader, opts ...XLSXOption) (dataframe.DataFrame, error)
```

### Writer

```go
// ToXLSX writes df to w as an xlsx workbook with a single sheet.
func ToXLSX(df dataframe.DataFrame, w io.Writer, opts ...XLSXOption) error
```

### Options

```go
// WithSheetName selects a sheet by name for reading, or sets the sheet name for writing.
// Default sheet name for writing: "Sheet1".
func WithSheetName(name string) XLSXOption

// WithSheetIndex selects a sheet by 0-based index for reading.
func WithSheetIndex(i int) XLSXOption
```

---

## Reading Logic

1. Open the workbook via `excelize.OpenReader(r)`.
2. Resolve the target sheet:
   - Default: first sheet (index 0), obtained via `f.GetSheetList()[0]`.
   - `WithSheetName(name)`: verify the name exists; error if not.
   - `WithSheetIndex(i)`: bounds-check against `f.GetSheetList()`; error if out of range.
3. Call `f.GetRows(sheetName)` → `[][]string`.
4. Trim trailing empty rows (excelize sometimes appends them). A row is considered empty if every cell in it is an empty string.
5. Row 0 is the header; remaining rows are data rows.
6. Error if fewer than 1 row (empty sheet).
7. Pad short rows to the header length with empty strings (matches CSV behavior for ragged rows).
8. Run the same type-inference pipeline as the CSV reader:
   - `inferColumnType`: tries `int64` → `float64` → `timestamp` (using default date format list) → `string`.
   - `buildArrowArray`: constructs typed Arrow arrays.
   - The functions are duplicated into the `excel` package (not shared via an internal package) to keep each package self-contained and avoid cross-package coupling.
9. Build an Arrow record and return `dataframe.FromArrow(rec)`.

**Date detection:** excelize returns cell values as strings formatted according to the cell's number format. The existing date-format list in the type-inference pipeline handles common formats (`2006-01-02`, `01/02/2006`, etc.). No special Excel serial-date handling is needed.

---

## Writing Logic

1. Create `excelize.NewFile()`.
2. Resolve sheet name: `WithSheetName` option or `"Sheet1"` default.
3. Rename the default sheet to the target name.
4. Write header row (row 1): `df.Columns()`.
5. Iterate over `df` rows; for each cell:
   - `int64` → `SetCellInt`
   - `float64` → `SetCellFloat` (precision 6, bitSize 64)
   - `bool` → `SetCellBool`
   - `timestamp` → `SetCellValue` with `time.Time` (excelize handles date formatting)
   - `string` / other → `SetCellStr`
   - null/nil → leave cell empty
6. Stream the result via `f.Write(w)`.

---

## CLI Integration

Changes confined to `cmd/gopandas/format.go`:

```go
// inferFormat: add
case ".xlsx":
    return "xlsx", nil

// loadFile: add
case "xlsx":
    f, err := os.Open(path)
    ...
    return excelio.FromXLSX(f)

// writeFile: add
case "xlsx":
    return excelio.ToXLSX(df, f)
```

`read.go` and `convert.go` require no changes — they go through `loadFile`/`writeFile`.

Example CLI usage (after this feature):
```bash
gopandas read testdata/employees.xlsx
gopandas convert testdata/employees.xlsx output.csv
gopandas read testdata/employees.xlsx --filter "salary > 70000" --output filtered.xlsx
```

---

## Error Handling

| Condition | Error message |
|-----------|--------------|
| Sheet name not found | `excel.FromXLSX: sheet "Foo" not found` |
| Sheet index out of range | `excel.FromXLSX: sheet index 3 out of range (file has 2 sheets)` |
| Empty sheet (no rows at all) | `excel.FromXLSX: empty sheet` |
| Sheet has only a header row | Returns empty DataFrame (0 rows), no error |
| Write failure | Propagated from excelize with context prefix |

---

## Testing

### `lib/dataio/excel/reader_test.go`
- Read `testdata/employees.xlsx` → verify shape, column names, dtypes
- Verify `hire_date` column infers as `timestamp`
- Select sheet by name → correct data
- Select sheet by 0-based index → correct data
- Bad sheet name → error
- Sheet index out of range → error
- Empty sheet → error

### `lib/dataio/excel/writer_test.go`
- Write a small DataFrame to a `bytes.Buffer`, re-read with excelize, verify cell values
- Roundtrip: `FromXLSX` → `ToXLSX` → `FromXLSX`, verify shape and column values match
- Custom sheet name via `WithSheetName`

### `cmd/gopandas/` CLI tests
- Extend existing `main_test.go` pattern:
  - `gopandas read testdata/employees.xlsx` → correct head output
  - `gopandas convert testdata/employees.xlsx /tmp/out.csv` → valid CSV produced
  - `gopandas convert testdata/employees.csv /tmp/out.xlsx` → valid xlsx produced

---

## Sample Testdata

### `testdata/employees.csv` (and `testdata/employees.xlsx`)

~20 rows. Columns:

| column | dtype |
|--------|-------|
| `id` | int64 |
| `name` | string |
| `department` | string |
| `salary` | float64 |
| `hire_date` | timestamp (`2006-01-02` format) |
| `active` | bool (true/false) |

Departments: Engineering, Marketing, Sales, HR.

### `testdata/sales.csv`

~30 rows. Columns:

| column | dtype |
|--------|-------|
| `date` | timestamp (`2006-01-02` format) |
| `region` | string (North, South, East, West) |
| `product` | string (Widget, Gadget, Doohickey) |
| `units` | int64 |
| `revenue` | float64 |

---

## README Updates

Replace the existing generic examples in the README with concrete examples that use `testdata/employees.csv` and `testdata/sales.csv`:

- **Read and display**: `gopandas read testdata/employees.csv --head 5`
- **Filter + sort**: `gopandas read testdata/employees.csv --filter "salary > 70000" --sort salary --sort-desc`
- **GroupBy**: `gopandas read testdata/sales.csv --groupby region --agg sum`
- **Date filter**: `gopandas read testdata/employees.csv --parse-dates hire_date --filter "hire_date > '2020-01-01'"`
- **Excel read**: `gopandas read testdata/employees.xlsx`
- **Convert**: `gopandas convert testdata/employees.xlsx output.csv`
- **Library usage** (Go code): use `employees.csv` data inline in code examples (keep as-is, readable without a file)
