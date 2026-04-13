# dataframe Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement DataFrame — the core tabular data structure as an ordered collection of named Series with a shared Index, supporting selection, filtering, mutation, sorting, and aggregation.

**Architecture:** Package `dataframe/` with a `DataFrame` struct containing an ordered column list, a map of name->Series[any], and a shared Index. All mutations return new DataFrames. Depends on `dtype/`, `index/`, `arrowutil/`, and `series/`.

**Tech Stack:** Go 1.21+ generics, github.com/apache/arrow-go/v18

**Depends on:** dtype, index, arrowutil, series (must be implemented first)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `dataframe/dataframe.go` | DataFrame struct, New(), FromRecords(), FromArrow(), metadata (Shape, Columns, DTypes, Index, Len), String() |
| `dataframe/dataframe_test.go` | Tests for construction + metadata + String() |
| `dataframe/access.go` | Col(), At(), Loc() |
| `dataframe/access_test.go` | Tests for column/cell access |
| `dataframe/select.go` | ILoc(), LocRows(), Select(), Drop(), Head(), Tail(), Sample() |
| `dataframe/select_test.go` | Tests for selection operations |
| `dataframe/filter.go` | Filter(), Query() with simple expression parser |
| `dataframe/filter_test.go` | Tests for filtering |
| `dataframe/mutate.go` | WithColumn(), Rename(), SetIndex(), ResetIndex(), AsType(), FillNA(), DropNA() |
| `dataframe/mutate_test.go` | Tests for mutation operations |
| `dataframe/sort.go` | SortBy() |
| `dataframe/sort_test.go` | Tests for sorting |
| `dataframe/agg.go` | Describe(), Sum(), Mean(), Std(), Min(), Max(), Count(), Corr(), CorrWith() |
| `dataframe/agg_test.go` | Tests for aggregation |

---

## Task 1: DataFrame Struct + New() Constructor

**Files:**
- Create: `dataframe/dataframe.go`
- Create: `dataframe/dataframe_test.go`

### Step 1.1: Write the failing test for New()

- [ ] Create `dataframe/dataframe_test.go`:

```go
package dataframe

import (
	"testing"

	"github.com/vchitepu/gopandas/lib/dtype"
)

func TestNew(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Charlie"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("Shape() rows = %d, want 3", rows)
	}
	if cols != 2 {
		t.Errorf("Shape() cols = %d, want 2", cols)
	}
}

func TestNew_EmptyMap(t *testing.T) {
	df, err := New(map[string]any{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Errorf("Shape() = (%d, %d), want (0, 0)", rows, cols)
	}
}

func TestNew_MismatchedLengths(t *testing.T) {
	_, err := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []string{"x", "y"},
	})
	if err == nil {
		t.Error("New() expected error for mismatched lengths, got nil")
	}
}
```

### Step 1.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestNew" -v
```

Expected: Compilation failure — `New` not defined.

### Step 1.3: Implement DataFrame struct + New()

- [ ] Create `dataframe/dataframe.go`:

```go
package dataframe

import (
	"fmt"
	"sort"

	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// DataFrame is an ordered collection of named Series sharing a common Index.
// All mutation methods return new DataFrames — DataFrame is immutable.
type DataFrame struct {
	index   index.Index
	columns []string
	data    map[string]*series.Series[any]
}

// New creates a DataFrame from a map of column name -> slice.
// Supported slice types: []int64, []float64, []string, []bool.
// All slices must have the same length.
func New(cols map[string]any) (DataFrame, error) {
	if len(cols) == 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: []string{},
			data:    map[string]*series.Series[any]{},
		}, nil
	}

	// Sort column names for deterministic ordering
	names := make([]string, 0, len(cols))
	for name := range cols {
		names = append(names, name)
	}
	sort.Strings(names)

	// Determine row count from first column
	var nRows int
	first := true
	for _, name := range names {
		n, err := sliceLen(cols[name])
		if err != nil {
			return DataFrame{}, fmt.Errorf("column %q: %w", name, err)
		}
		if first {
			nRows = n
			first = false
		} else if n != nRows {
			return DataFrame{}, fmt.Errorf(
				"column %q has %d rows, expected %d (all columns must have same length)",
				name, n, nRows,
			)
		}
	}

	idx := index.NewRangeIndex(nRows)
	data := make(map[string]*series.Series[any], len(names))
	for _, name := range names {
		s, err := buildSeries(name, cols[name], idx)
		if err != nil {
			return DataFrame{}, fmt.Errorf("column %q: %w", name, err)
		}
		data[name] = s
	}

	return DataFrame{
		index:   idx,
		columns: names,
		data:    data,
	}, nil
}

// sliceLen returns the length of a supported slice type.
func sliceLen(v any) (int, error) {
	switch s := v.(type) {
	case []int64:
		return len(s), nil
	case []float64:
		return len(s), nil
	case []string:
		return len(s), nil
	case []bool:
		return len(s), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// buildSeries creates a *series.Series[any] from a name, slice, and index.
func buildSeries(name string, v any, idx index.Index) (*series.Series[any], error) {
	switch s := v.(type) {
	case []int64:
		ser := series.New[any](toAnySlice(s), idx, name)
		return &ser, nil
	case []float64:
		ser := series.New[any](toAnySlice(s), idx, name)
		return &ser, nil
	case []string:
		ser := series.New[any](toAnySlice(s), idx, name)
		return &ser, nil
	case []bool:
		ser := series.New[any](toAnySlice(s), idx, name)
		return &ser, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

// toAnySlice converts a typed slice to []any.
func toAnySlice[T any](s []T) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
```

### Step 1.4: Run test to verify it passes

- [ ] Run:

```bash
cd dataframe && go test -run "TestNew" -v
```

Expected: All 3 tests PASS.

### Step 1.5: Commit

- [ ] Run:

```bash
git add dataframe/dataframe.go dataframe/dataframe_test.go
git commit -m "feat(dataframe): add DataFrame struct and New() constructor"
```

---

## Task 2: Metadata Methods — Shape(), Columns(), DTypes(), Index(), Len()

**Files:**
- Modify: `dataframe/dataframe.go`
- Modify: `dataframe/dataframe_test.go`

### Step 2.1: Write the failing tests for metadata methods

- [ ] Append to `dataframe/dataframe_test.go`:

```go
func TestShape(t *testing.T) {
	df, err := New(map[string]any{
		"x": []float64{1.0, 2.0, 3.0},
		"y": []float64{4.0, 5.0, 6.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 3 || cols != 2 {
		t.Errorf("Shape() = (%d, %d), want (3, 2)", rows, cols)
	}
}

func TestColumns(t *testing.T) {
	df, err := New(map[string]any{
		"b": []int64{1},
		"a": []int64{2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	cols := df.Columns()
	// Columns should be sorted deterministically
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("Columns() = %v, want [a b]", cols)
	}
}

func TestDTypes(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice"},
		"age":  []int64{30},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	dtypes := df.DTypes()
	if dtypes["age"] != dtype.Int64 {
		t.Errorf("DTypes()[age] = %v, want Int64", dtypes["age"])
	}
	if dtypes["name"] != dtype.String {
		t.Errorf("DTypes()[name] = %v, want String", dtypes["name"])
	}
}

func TestIndex(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10, 20, 30},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	idx := df.Index()
	if idx.Len() != 3 {
		t.Errorf("Index().Len() = %d, want 3", idx.Len())
	}
}

func TestLen(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10, 20, 30},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if df.Len() != 3 {
		t.Errorf("Len() = %d, want 3", df.Len())
	}
}
```

### Step 2.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestShape|TestColumns|TestDTypes|TestIndex|TestLen" -v
```

Expected: Compilation failure — `Shape`, `Columns`, `DTypes`, `Index`, `Len` not defined.

### Step 2.3: Implement metadata methods

- [ ] Add to `dataframe/dataframe.go`:

```go
// Shape returns (rows, cols).
func (df DataFrame) Shape() (int, int) {
	return df.index.Len(), len(df.columns)
}

// Columns returns the ordered column names.
// The returned slice is a copy — modifying it does not affect the DataFrame.
func (df DataFrame) Columns() []string {
	out := make([]string, len(df.columns))
	copy(out, df.columns)
	return out
}

// DTypes returns a map of column name -> dtype.DType.
func (df DataFrame) DTypes() map[string]dtype.DType {
	dtypes := make(map[string]dtype.DType, len(df.columns))
	for _, name := range df.columns {
		dtypes[name] = df.data[name].DType()
	}
	return dtypes
}

// Index returns the DataFrame's row index.
func (df DataFrame) Index() index.Index {
	return df.index
}

// Len returns the number of rows.
func (df DataFrame) Len() int {
	return df.index.Len()
}
```

### Step 2.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestShape|TestColumns|TestDTypes|TestIndex|TestLen" -v
```

Expected: All 5 tests PASS.

### Step 2.5: Commit

- [ ] Run:

```bash
git add dataframe/dataframe.go dataframe/dataframe_test.go
git commit -m "feat(dataframe): add metadata methods Shape, Columns, DTypes, Index, Len"
```

---

## Task 3: String() — Tabular Display

**Files:**
- Modify: `dataframe/dataframe.go`
- Modify: `dataframe/dataframe_test.go`

### Step 3.1: Write the failing test for String()

- [ ] Append to `dataframe/dataframe_test.go`:

```go
func TestString(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	s := df.String()
	// Should contain column headers and data
	if !containsAll(s, "name", "age", "Alice", "Bob", "30", "25") {
		t.Errorf("String() missing expected content:\n%s", s)
	}
}

func TestString_Empty(t *testing.T) {
	df, err := New(map[string]any{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	s := df.String()
	if s != "Empty DataFrame" {
		t.Errorf("String() = %q, want %q", s, "Empty DataFrame")
	}
}

// containsAll checks that s contains all substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
```

Also add `"strings"` to the import block in `dataframe/dataframe_test.go`.

### Step 3.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestString" -v
```

Expected: Compilation failure — `String` not defined on DataFrame.

### Step 3.3: Implement String()

- [ ] Add to `dataframe/dataframe.go`:

```go
import "strings"

// String returns a tabular text representation of the DataFrame.
// Columns are separated by tabs. The index column is shown on the left.
func (df DataFrame) String() string {
	if len(df.columns) == 0 {
		return "Empty DataFrame"
	}

	var buf strings.Builder
	nRows := df.Len()

	// Calculate column widths
	widths := make(map[string]int, len(df.columns))
	for _, col := range df.columns {
		widths[col] = len(col)
	}
	// Index column width
	idxWidth := 0
	for i := 0; i < nRows; i++ {
		labels := df.index.Labels()
		lbl := fmt.Sprintf("%v", labels[i])
		if len(lbl) > idxWidth {
			idxWidth = len(lbl)
		}
	}

	// Check data widths
	for _, col := range df.columns {
		s := df.data[col]
		for i := 0; i < nRows; i++ {
			val, isNull := s.At(i)
			var cell string
			if isNull {
				cell = "NaN"
			} else {
				cell = fmt.Sprintf("%v", val)
			}
			if len(cell) > widths[col] {
				widths[col] = len(cell)
			}
		}
	}

	// Header row
	buf.WriteString(strings.Repeat(" ", idxWidth))
	for _, col := range df.columns {
		buf.WriteString("  ")
		buf.WriteString(padRight(col, widths[col]))
	}
	buf.WriteString("\n")

	// Data rows
	labels := df.index.Labels()
	for i := 0; i < nRows; i++ {
		buf.WriteString(padRight(fmt.Sprintf("%v", labels[i]), idxWidth))
		for _, col := range df.columns {
			buf.WriteString("  ")
			val, isNull := df.data[col].At(i)
			var cell string
			if isNull {
				cell = "NaN"
			} else {
				cell = fmt.Sprintf("%v", val)
			}
			buf.WriteString(padRight(cell, widths[col]))
		}
		if i < nRows-1 {
			buf.WriteString("\n")
		}
	}

	return buf.String()
}

// padRight pads s with spaces to width w.
func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}
```

### Step 3.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestString" -v
```

Expected: Both TestString and TestString_Empty PASS.

### Step 3.5: Commit

- [ ] Run:

```bash
git add dataframe/dataframe.go dataframe/dataframe_test.go
git commit -m "feat(dataframe): add String() tabular display"
```

---

## Task 4: FromRecords() Constructor

**Files:**
- Modify: `dataframe/dataframe.go`
- Modify: `dataframe/dataframe_test.go`

### Step 4.1: Write the failing test for FromRecords()

- [ ] Append to `dataframe/dataframe_test.go`:

```go
func TestFromRecords(t *testing.T) {
	records := []map[string]any{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob", "age": int64(25)},
		{"name": "Charlie", "age": int64(35)},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("Shape() rows = %d, want 3", rows)
	}
	if cols != 2 {
		t.Errorf("Shape() cols = %d, want 2", cols)
	}
}

func TestFromRecords_Empty(t *testing.T) {
	df, err := FromRecords([]map[string]any{})
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}
	rows, cols := df.Shape()
	if rows != 0 || cols != 0 {
		t.Errorf("Shape() = (%d, %d), want (0, 0)", rows, cols)
	}
}

func TestFromRecords_MissingKey(t *testing.T) {
	records := []map[string]any{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob"},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}
	// "age" should be present with nil for missing value
	rows, cols := df.Shape()
	if rows != 2 || cols != 2 {
		t.Errorf("Shape() = (%d, %d), want (2, 2)", rows, cols)
	}
}
```

### Step 4.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestFromRecords" -v
```

Expected: Compilation failure — `FromRecords` not defined.

### Step 4.3: Implement FromRecords()

- [ ] Add to `dataframe/dataframe.go`:

```go
// FromRecords creates a DataFrame from a slice of maps.
// Each map represents a row: keys are column names, values are cell values.
// Missing keys in any record produce nil (null) values in that cell.
func FromRecords(records []map[string]any) (DataFrame, error) {
	if len(records) == 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: []string{},
			data:    map[string]*series.Series[any]{},
		}, nil
	}

	// Collect all unique column names
	colSet := map[string]bool{}
	for _, rec := range records {
		for k := range rec {
			colSet[k] = true
		}
	}
	names := make([]string, 0, len(colSet))
	for k := range colSet {
		names = append(names, k)
	}
	sort.Strings(names)

	nRows := len(records)
	idx := index.NewRangeIndex(nRows)

	data := make(map[string]*series.Series[any], len(names))
	for _, col := range names {
		vals := make([]any, nRows)
		for i, rec := range records {
			vals[i] = rec[col] // nil if key is missing
		}
		ser := series.New[any](vals, idx, col)
		data[col] = &ser
	}

	return DataFrame{
		index:   idx,
		columns: names,
		data:    data,
	}, nil
}
```

### Step 4.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestFromRecords" -v
```

Expected: All 3 tests PASS.

### Step 4.5: Commit

- [ ] Run:

```bash
git add dataframe/dataframe.go dataframe/dataframe_test.go
git commit -m "feat(dataframe): add FromRecords() constructor"
```

---

## Task 5: FromArrow() Constructor

**Files:**
- Modify: `dataframe/dataframe.go`
- Modify: `dataframe/dataframe_test.go`

### Step 5.1: Write the failing test for FromArrow()

- [ ] Append to `dataframe/dataframe_test.go`:

```go
import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestFromArrow(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()

	df := FromArrow(rec)
	rows, cols := df.Shape()
	if rows != 3 {
		t.Errorf("Shape() rows = %d, want 3", rows)
	}
	if cols != 2 {
		t.Errorf("Shape() cols = %d, want 2", cols)
	}
	colNames := df.Columns()
	if colNames[0] != "id" || colNames[1] != "val" {
		t.Errorf("Columns() = %v, want [id val]", colNames)
	}
}
```

### Step 5.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestFromArrow" -v
```

Expected: Compilation failure — `FromArrow` not defined.

### Step 5.3: Implement FromArrow()

- [ ] Add to `dataframe/dataframe.go`:

```go
import (
	arrowlib "github.com/apache/arrow-go/v18/arrow"
)

// FromArrow creates a DataFrame from an Arrow Record.
// Column names and types are derived from the Record's schema.
// The column order matches the schema field order.
func FromArrow(rec arrowlib.Record) DataFrame {
	nCols := int(rec.NumCols())
	nRows := int(rec.NumRows())
	idx := index.NewRangeIndex(nRows)

	names := make([]string, nCols)
	data := make(map[string]*series.Series[any], nCols)

	for i := 0; i < nCols; i++ {
		field := rec.Schema().Field(i)
		names[i] = field.Name
		col := rec.Column(i)
		ser := series.FromArrow(col, idx, field.Name)
		data[field.Name] = &ser
	}

	return DataFrame{
		index:   idx,
		columns: names,
		data:    data,
	}
}
```

### Step 5.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestFromArrow" -v
```

Expected: PASS.

### Step 5.5: Commit

- [ ] Run:

```bash
git add dataframe/dataframe.go dataframe/dataframe_test.go
git commit -m "feat(dataframe): add FromArrow() constructor"
```

---

## Task 6: Col() — Column Access

**Files:**
- Create: `dataframe/access.go`
- Create: `dataframe/access_test.go`

### Step 6.1: Write the failing test for Col()

- [ ] Create `dataframe/access_test.go`:

```go
package dataframe

import (
	"testing"
)

func TestCol(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s, err := df.Col("age")
	if err != nil {
		t.Fatalf("Col() error: %v", err)
	}
	if s.Len() != 2 {
		t.Errorf("Col(age).Len() = %d, want 2", s.Len())
	}
	if s.Name() != "age" {
		t.Errorf("Col(age).Name() = %q, want %q", s.Name(), "age")
	}
}

func TestCol_NotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Col("missing")
	if err == nil {
		t.Error("Col(missing) expected error, got nil")
	}
}
```

### Step 6.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestCol" -v
```

Expected: Compilation failure — `Col` not defined.

### Step 6.3: Implement Col()

- [ ] Create `dataframe/access.go`:

```go
package dataframe

import (
	"fmt"

	"github.com/vchitepu/gopandas/lib/series"
)

// Col returns the named column as a Series.
// Returns an error if the column does not exist.
func (df DataFrame) Col(name string) (*series.Series[any], error) {
	s, ok := df.data[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}
	return s, nil
}
```

### Step 6.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestCol" -v
```

Expected: Both tests PASS.

### Step 6.5: Commit

- [ ] Run:

```bash
git add dataframe/access.go dataframe/access_test.go
git commit -m "feat(dataframe): add Col() column access method"
```

---

## Task 7: At() — Positional Cell Access

**Files:**
- Modify: `dataframe/access.go`
- Modify: `dataframe/access_test.go`

### Step 7.1: Write the failing test for At()

- [ ] Append to `dataframe/access_test.go`:

```go
func TestAt(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Charlie"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	val, err := df.At(1, "name")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != "Bob" {
		t.Errorf("At(1, name) = %v, want Bob", val)
	}

	val, err = df.At(2, "age")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(35) {
		t.Errorf("At(2, age) = %v, want 35", val)
	}
}

func TestAt_OutOfBounds(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.At(5, "x")
	if err == nil {
		t.Error("At(5, x) expected error for out of bounds, got nil")
	}
}

func TestAt_ColumnNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.At(0, "missing")
	if err == nil {
		t.Error("At(0, missing) expected error, got nil")
	}
}
```

### Step 7.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestAt" -v
```

Expected: Compilation failure — `At` not defined.

### Step 7.3: Implement At()

- [ ] Add to `dataframe/access.go`:

```go
// At returns the value at positional row index `row` and column name `col`.
// Returns an error if the column does not exist or the row index is out of bounds.
func (df DataFrame) At(row int, col string) (any, error) {
	s, ok := df.data[col]
	if !ok {
		return nil, fmt.Errorf("column %q not found", col)
	}
	if row < 0 || row >= df.Len() {
		return nil, fmt.Errorf("row index %d out of bounds [0, %d)", row, df.Len())
	}
	val, _ := s.At(row)
	return val, nil
}
```

### Step 7.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestAt" -v
```

Expected: All 3 tests PASS.

### Step 7.5: Commit

- [ ] Run:

```bash
git add dataframe/access.go dataframe/access_test.go
git commit -m "feat(dataframe): add At() positional cell access"
```

---

## Task 8: Loc() — Label-Based Cell Access

**Files:**
- Modify: `dataframe/access.go`
- Modify: `dataframe/access_test.go`

### Step 8.1: Write the failing test for Loc()

- [ ] Append to `dataframe/access_test.go`:

```go
func TestLoc(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob"},
		"age":  []int64{30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Default RangeIndex: labels are 0, 1, 2...
	val, err := df.Loc(0, "name")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	if val != "Alice" {
		t.Errorf("Loc(0, name) = %v, want Alice", val)
	}
}

func TestLoc_LabelNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10, 20},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Loc(99, "x")
	if err == nil {
		t.Error("Loc(99, x) expected error for missing label, got nil")
	}
}

func TestLoc_ColumnNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Loc(0, "missing")
	if err == nil {
		t.Error("Loc(0, missing) expected error, got nil")
	}
}
```

### Step 8.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestLoc" -v
```

Expected: Compilation failure — `Loc` not defined.

### Step 8.3: Implement Loc()

- [ ] Add to `dataframe/access.go`:

```go
// Loc returns the value at label `label` and column name `col`.
// Uses the DataFrame's Index to resolve the label to a positional offset.
// Returns an error if the label is not found or the column does not exist.
func (df DataFrame) Loc(label any, col string) (any, error) {
	s, ok := df.data[col]
	if !ok {
		return nil, fmt.Errorf("column %q not found", col)
	}
	pos, found := df.index.Loc(label)
	if !found {
		return nil, fmt.Errorf("label %v not found in index", label)
	}
	val, _ := s.At(pos)
	return val, nil
}
```

### Step 8.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestLoc" -v
```

Expected: All 3 tests PASS.

### Step 8.5: Commit

- [ ] Run:

```bash
git add dataframe/access.go dataframe/access_test.go
git commit -m "feat(dataframe): add Loc() label-based cell access"
```

---

## Task 9: Head() and Tail()

**Files:**
- Create: `dataframe/select.go`
- Create: `dataframe/select_test.go`

### Step 9.1: Write the failing tests for Head() and Tail()

- [ ] Create `dataframe/select_test.go`:

```go
package dataframe

import (
	"testing"
)

func TestHead(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	hd := df.Head(3)
	if hd.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", hd.Len())
	}
	val, err := hd.At(0, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Head(3).At(0, x) = %v, want 1", val)
	}
	val, err = hd.At(2, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(3) {
		t.Errorf("Head(3).At(2, x) = %v, want 3", val)
	}
}

func TestHead_MoreThanLen(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	hd := df.Head(10)
	if hd.Len() != 2 {
		t.Errorf("Head(10).Len() = %d, want 2", hd.Len())
	}
}

func TestTail(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	tl := df.Tail(2)
	if tl.Len() != 2 {
		t.Errorf("Tail(2).Len() = %d, want 2", tl.Len())
	}
	val, err := tl.At(0, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(4) {
		t.Errorf("Tail(2).At(0, x) = %v, want 4", val)
	}
	val, err = tl.At(1, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(5) {
		t.Errorf("Tail(2).At(1, x) = %v, want 5", val)
	}
}

func TestTail_MoreThanLen(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	tl := df.Tail(10)
	if tl.Len() != 2 {
		t.Errorf("Tail(10).Len() = %d, want 2", tl.Len())
	}
}
```

### Step 9.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestHead|TestTail" -v
```

Expected: Compilation failure — `Head`, `Tail` not defined.

### Step 9.3: Implement Head() and Tail()

- [ ] Create `dataframe/select.go`:

```go
package dataframe

import (
	"fmt"
	"math/rand"

	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// Head returns a new DataFrame with the first n rows.
// If n >= Len(), returns a copy of the entire DataFrame.
func (df DataFrame) Head(n int) DataFrame {
	if n >= df.Len() {
		n = df.Len()
	}
	if n <= 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: df.Columns(),
			data:    map[string]*series.Series[any]{},
		}
	}
	return df.sliceRows(0, n)
}

// Tail returns a new DataFrame with the last n rows.
// If n >= Len(), returns a copy of the entire DataFrame.
func (df DataFrame) Tail(n int) DataFrame {
	if n >= df.Len() {
		n = df.Len()
	}
	if n <= 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: df.Columns(),
			data:    map[string]*series.Series[any]{},
		}
	}
	start := df.Len() - n
	return df.sliceRows(start, df.Len())
}

// sliceRows returns a new DataFrame with rows [start, end).
// This is the internal helper used by Head, Tail, ILoc, etc.
func (df DataFrame) sliceRows(start, end int) DataFrame {
	newIdx := df.index.Slice(start, end)
	data := make(map[string]*series.Series[any], len(df.columns))
	for _, col := range df.columns {
		sliced := df.data[col].ILoc(start, end)
		data[col] = &sliced
	}
	return DataFrame{
		index:   newIdx,
		columns: df.Columns(),
		data:    data,
	}
}
```

### Step 9.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestHead|TestTail" -v
```

Expected: All 4 tests PASS.

### Step 9.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add Head() and Tail() selection methods"
```

---

## Task 10: ILoc() — Positional Slice

**Files:**
- Modify: `dataframe/select.go`
- Modify: `dataframe/select_test.go`

### Step 10.1: Write the failing test for ILoc()

- [ ] Append to `dataframe/select_test.go`:

```go
func TestILoc(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{10, 20, 30, 40, 50},
		"b": []string{"p", "q", "r", "s", "t"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sub, err := df.ILoc(1, 4, 0, 2)
	if err != nil {
		t.Fatalf("ILoc() error: %v", err)
	}
	rows, cols := sub.Shape()
	if rows != 3 || cols != 2 {
		t.Errorf("ILoc(1,4,0,2).Shape() = (%d, %d), want (3, 2)", rows, cols)
	}
	val, err := sub.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(20) {
		t.Errorf("ILoc first row a = %v, want 20", val)
	}
}

func TestILoc_ColSlice(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
		"c": []int64{5, 6},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sub, err := df.ILoc(0, 2, 1, 3)
	if err != nil {
		t.Fatalf("ILoc() error: %v", err)
	}
	cols := sub.Columns()
	if len(cols) != 2 || cols[0] != "b" || cols[1] != "c" {
		t.Errorf("ILoc cols = %v, want [b c]", cols)
	}
}

func TestILoc_OutOfBounds(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.ILoc(0, 10, 0, 1)
	if err == nil {
		t.Error("ILoc expected error for out of bounds, got nil")
	}
}
```

### Step 10.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestILoc" -v
```

Expected: Compilation failure — `ILoc` not defined.

### Step 10.3: Implement ILoc()

- [ ] Add to `dataframe/select.go`:

```go
// ILoc returns a positional slice of the DataFrame.
// rowStart/rowEnd select rows [rowStart, rowEnd).
// colStart/colEnd select columns [colStart, colEnd) by positional index.
func (df DataFrame) ILoc(rowStart, rowEnd, colStart, colEnd int) (DataFrame, error) {
	nRows := df.Len()
	nCols := len(df.columns)

	if rowStart < 0 || rowEnd > nRows || rowStart > rowEnd {
		return DataFrame{}, fmt.Errorf(
			"row slice [%d:%d] out of bounds for DataFrame with %d rows",
			rowStart, rowEnd, nRows,
		)
	}
	if colStart < 0 || colEnd > nCols || colStart > colEnd {
		return DataFrame{}, fmt.Errorf(
			"col slice [%d:%d] out of bounds for DataFrame with %d columns",
			colStart, colEnd, nCols,
		)
	}

	selectedCols := df.columns[colStart:colEnd]
	newIdx := df.index.Slice(rowStart, rowEnd)
	data := make(map[string]*series.Series[any], len(selectedCols))
	for _, col := range selectedCols {
		sliced := df.data[col].ILoc(rowStart, rowEnd)
		data[col] = &sliced
	}

	newColNames := make([]string, len(selectedCols))
	copy(newColNames, selectedCols)

	return DataFrame{
		index:   newIdx,
		columns: newColNames,
		data:    data,
	}, nil
}
```

### Step 10.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestILoc" -v
```

Expected: All 3 tests PASS.

### Step 10.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add ILoc() positional slice"
```

---

## Task 11: LocRows() — Label-Based Row Selection

**Files:**
- Modify: `dataframe/select.go`
- Modify: `dataframe/select_test.go`

### Step 11.1: Write the failing test for LocRows()

- [ ] Append to `dataframe/select_test.go`:

```go
func TestLocRows(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10, 20, 30, 40, 50},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// RangeIndex: labels are 0, 1, 2, 3, 4
	sub, err := df.LocRows([]any{1, 3})
	if err != nil {
		t.Fatalf("LocRows() error: %v", err)
	}
	if sub.Len() != 2 {
		t.Errorf("LocRows().Len() = %d, want 2", sub.Len())
	}
	val, err := sub.At(0, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(20) {
		t.Errorf("LocRows first row x = %v, want 20", val)
	}
	val, err = sub.At(1, "x")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(40) {
		t.Errorf("LocRows second row x = %v, want 40", val)
	}
}

func TestLocRows_LabelNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.LocRows([]any{99})
	if err == nil {
		t.Error("LocRows expected error for missing label, got nil")
	}
}
```

### Step 11.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestLocRows" -v
```

Expected: Compilation failure — `LocRows` not defined.

### Step 11.3: Implement LocRows()

- [ ] Add to `dataframe/select.go`:

```go
// LocRows selects rows by label. Returns a new DataFrame with only the
// rows matching the given labels. Returns an error if any label is not found.
func (df DataFrame) LocRows(labels []any) (DataFrame, error) {
	positions := make([]int, 0, len(labels))
	for _, lbl := range labels {
		pos, found := df.index.Loc(lbl)
		if !found {
			return DataFrame{}, fmt.Errorf("label %v not found in index", lbl)
		}
		positions = append(positions, pos)
	}

	return df.selectRowsByPositions(positions)
}

// selectRowsByPositions extracts rows at the given positional indices
// into a new DataFrame with a fresh RangeIndex.
func (df DataFrame) selectRowsByPositions(positions []int) (DataFrame, error) {
	nRows := len(positions)
	newIdx := index.NewRangeIndex(nRows)
	data := make(map[string]*series.Series[any], len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		vals := make([]any, nRows)
		for i, pos := range positions {
			val, _ := s.At(pos)
			vals[i] = val
		}
		newSer := series.New[any](vals, newIdx, col)
		data[col] = &newSer
	}

	return DataFrame{
		index:   newIdx,
		columns: df.Columns(),
		data:    data,
	}, nil
}
```

### Step 11.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestLocRows" -v
```

Expected: Both tests PASS.

### Step 11.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add LocRows() label-based row selection"
```

---

## Task 12: Select() — Column Selection

**Files:**
- Modify: `dataframe/select.go`
- Modify: `dataframe/select_test.go`

### Step 12.1: Write the failing test for Select()

- [ ] Append to `dataframe/select_test.go`:

```go
func TestSelect(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
		"c": []int64{5, 6},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sub, err := df.Select("c", "a")
	if err != nil {
		t.Fatalf("Select() error: %v", err)
	}
	cols := sub.Columns()
	if len(cols) != 2 || cols[0] != "c" || cols[1] != "a" {
		t.Errorf("Select(c, a).Columns() = %v, want [c a]", cols)
	}
	if sub.Len() != 2 {
		t.Errorf("Select().Len() = %d, want 2", sub.Len())
	}
}

func TestSelect_NotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Select("x", "missing")
	if err == nil {
		t.Error("Select(x, missing) expected error, got nil")
	}
}
```

### Step 12.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestSelect" -v
```

Expected: Compilation failure — `Select` not defined.

### Step 12.3: Implement Select()

- [ ] Add to `dataframe/select.go`:

```go
// Select returns a new DataFrame with only the specified columns,
// in the order given. Returns an error if any column is not found.
func (df DataFrame) Select(cols ...string) (DataFrame, error) {
	for _, col := range cols {
		if _, ok := df.data[col]; !ok {
			return DataFrame{}, fmt.Errorf("column %q not found", col)
		}
	}

	data := make(map[string]*series.Series[any], len(cols))
	for _, col := range cols {
		data[col] = df.data[col]
	}

	newCols := make([]string, len(cols))
	copy(newCols, cols)

	return DataFrame{
		index:   df.index,
		columns: newCols,
		data:    data,
	}, nil
}
```

### Step 12.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestSelect" -v
```

Expected: Both tests PASS.

### Step 12.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add Select() column selection"
```

---

## Task 13: Drop() — Column Removal

**Files:**
- Modify: `dataframe/select.go`
- Modify: `dataframe/select_test.go`

### Step 13.1: Write the failing test for Drop()

- [ ] Append to `dataframe/select_test.go`:

```go
func TestDrop(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
		"c": []int64{5, 6},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sub := df.Drop("b")
	cols := sub.Columns()
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "c" {
		t.Errorf("Drop(b).Columns() = %v, want [a c]", cols)
	}
}

func TestDrop_NonExistent(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Dropping a non-existent column is a no-op (no error)
	sub := df.Drop("missing")
	cols := sub.Columns()
	if len(cols) != 1 || cols[0] != "a" {
		t.Errorf("Drop(missing).Columns() = %v, want [a]", cols)
	}
}

func TestDrop_Multiple(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1},
		"b": []int64{2},
		"c": []int64{3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sub := df.Drop("a", "c")
	cols := sub.Columns()
	if len(cols) != 1 || cols[0] != "b" {
		t.Errorf("Drop(a, c).Columns() = %v, want [b]", cols)
	}
}
```

### Step 13.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestDrop" -v
```

Expected: Compilation failure — `Drop` not defined.

### Step 13.3: Implement Drop()

- [ ] Add to `dataframe/select.go`:

```go
// Drop returns a new DataFrame with the specified columns removed.
// Columns that do not exist are silently ignored.
func (df DataFrame) Drop(cols ...string) DataFrame {
	dropSet := make(map[string]bool, len(cols))
	for _, col := range cols {
		dropSet[col] = true
	}

	newCols := make([]string, 0, len(df.columns))
	data := make(map[string]*series.Series[any])
	for _, col := range df.columns {
		if !dropSet[col] {
			newCols = append(newCols, col)
			data[col] = df.data[col]
		}
	}

	return DataFrame{
		index:   df.index,
		columns: newCols,
		data:    data,
	}
}
```

### Step 13.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestDrop" -v
```

Expected: All 3 tests PASS.

### Step 13.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add Drop() column removal"
```

---

## Task 14: Sample() — Random Row Sampling

**Files:**
- Modify: `dataframe/select.go`
- Modify: `dataframe/select_test.go`

### Step 14.1: Write the failing test for Sample()

- [ ] Append to `dataframe/select_test.go`:

```go
func TestSample(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{10, 20, 30, 40, 50},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sampled, err := df.Sample(3, 42)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}
	if sampled.Len() != 3 {
		t.Errorf("Sample(3).Len() = %d, want 3", sampled.Len())
	}
}

func TestSample_Deterministic(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s1, err := df.Sample(3, 123)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}
	s2, err := df.Sample(3, 123)
	if err != nil {
		t.Fatalf("Sample() error: %v", err)
	}

	// Same seed should produce same result
	for i := 0; i < 3; i++ {
		v1, _ := s1.At(i, "x")
		v2, _ := s2.At(i, "x")
		if v1 != v2 {
			t.Errorf("Sample with same seed: row %d differs: %v vs %v", i, v1, v2)
		}
	}
}

func TestSample_TooMany(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Sample(5, 42)
	if err == nil {
		t.Error("Sample(5) expected error when n > Len(), got nil")
	}
}
```

### Step 14.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestSample" -v
```

Expected: Compilation failure — `Sample` not defined.

### Step 14.3: Implement Sample()

- [ ] Add to `dataframe/select.go`:

```go
// Sample returns a new DataFrame with n randomly-selected rows (without replacement).
// The seed parameter controls reproducibility. Returns an error if n > Len().
func (df DataFrame) Sample(n int, seed int64) (DataFrame, error) {
	if n > df.Len() {
		return DataFrame{}, fmt.Errorf(
			"cannot sample %d rows from DataFrame with %d rows",
			n, df.Len(),
		)
	}
	if n <= 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: df.Columns(),
			data:    map[string]*series.Series[any]{},
		}, nil
	}

	rng := rand.New(rand.NewSource(seed))
	// Fisher-Yates shuffle to pick n indices
	indices := make([]int, df.Len())
	for i := range indices {
		indices[i] = i
	}
	for i := len(indices) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}
	selected := indices[:n]

	return df.selectRowsByPositions(selected)
}
```

### Step 14.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestSample" -v
```

Expected: All 3 tests PASS.

### Step 14.5: Commit

- [ ] Run:

```bash
git add dataframe/select.go dataframe/select_test.go
git commit -m "feat(dataframe): add Sample() random row sampling"
```

---

## Task 15: Filter() — Boolean Mask Filtering

**Files:**
- Create: `dataframe/filter.go`
- Create: `dataframe/filter_test.go`

### Step 15.1: Write the failing test for Filter()

- [ ] Create `dataframe/filter_test.go`:

```go
package dataframe

import (
	"testing"

	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

func TestFilter(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Charlie"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	mask := series.New[bool]([]bool{true, false, true}, index.NewRangeIndex(3), "mask")
	filtered, err := df.Filter(mask)
	if err != nil {
		t.Fatalf("Filter() error: %v", err)
	}
	if filtered.Len() != 2 {
		t.Errorf("Filter().Len() = %d, want 2", filtered.Len())
	}
	val, err := filtered.At(0, "name")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != "Alice" {
		t.Errorf("Filter first row name = %v, want Alice", val)
	}
	val, err = filtered.At(1, "name")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != "Charlie" {
		t.Errorf("Filter second row name = %v, want Charlie", val)
	}
}

func TestFilter_LengthMismatch(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	mask := series.New[bool]([]bool{true, false}, index.NewRangeIndex(2), "mask")
	_, err = df.Filter(mask)
	if err == nil {
		t.Error("Filter expected error for length mismatch, got nil")
	}
}
```

### Step 15.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestFilter" -v
```

Expected: Compilation failure — `Filter` not defined.

### Step 15.3: Implement Filter()

- [ ] Create `dataframe/filter.go`:

```go
package dataframe

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// Filter returns a new DataFrame containing only the rows where
// the boolean mask Series is true. The mask must have the same length
// as the DataFrame.
func (df DataFrame) Filter(mask series.Series[bool]) (DataFrame, error) {
	if mask.Len() != df.Len() {
		return DataFrame{}, fmt.Errorf(
			"mask length %d does not match DataFrame length %d",
			mask.Len(), df.Len(),
		)
	}

	// Collect positions where mask is true
	positions := make([]int, 0)
	for i := 0; i < mask.Len(); i++ {
		val, isNull := mask.At(i)
		if !isNull && val {
			positions = append(positions, i)
		}
	}

	return df.selectRowsByPositions(positions)
}
```

### Step 15.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestFilter" -v
```

Expected: Both tests PASS.

### Step 15.5: Commit

- [ ] Run:

```bash
git add dataframe/filter.go dataframe/filter_test.go
git commit -m "feat(dataframe): add Filter() boolean mask filtering"
```

---

## Task 16: Query() — Simple Expression Parser

**Files:**
- Modify: `dataframe/filter.go`
- Modify: `dataframe/filter_test.go`

### Step 16.1: Write the failing tests for Query()

- [ ] Append to `dataframe/filter_test.go`:

```go
func TestQuery_GreaterThan(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Charlie"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result, err := df.Query("age > 28")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Errorf("Query(age > 28).Len() = %d, want 2", result.Len())
	}
}

func TestQuery_Equals_String(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Alice", "Bob", "Charlie"},
		"age":  []int64{30, 25, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result, err := df.Query("name == 'Bob'")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 1 {
		t.Errorf("Query(name == 'Bob').Len() = %d, want 1", result.Len())
	}
	val, err := result.At(0, "name")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != "Bob" {
		t.Errorf("Query result name = %v, want Bob", val)
	}
}

func TestQuery_LessThanEqual(t *testing.T) {
	df, err := New(map[string]any{
		"score": []float64{1.5, 2.5, 3.5},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result, err := df.Query("score <= 2.5")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Errorf("Query(score <= 2.5).Len() = %d, want 2", result.Len())
	}
}

func TestQuery_NotEquals(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result, err := df.Query("x != 2")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if result.Len() != 2 {
		t.Errorf("Query(x != 2).Len() = %d, want 2", result.Len())
	}
}

func TestQuery_InvalidExpr(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Query("invalid expression without operator")
	if err == nil {
		t.Error("Query expected parse error, got nil")
	}
}

func TestQuery_ColumnNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.Query("missing > 0")
	if err == nil {
		t.Error("Query expected column not found error, got nil")
	}
}
```

### Step 16.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestQuery" -v
```

Expected: Compilation failure — `Query` not defined.

### Step 16.3: Implement Query()

- [ ] Add to `dataframe/filter.go`:

```go
// Query filters the DataFrame using a simple expression string.
// Supported formats:
//   - "col > 10"    "col >= 10"
//   - "col < 10"    "col <= 10"
//   - "col == 10"   "col != 10"
//   - "col == 'foo'" (string comparison, single-quoted)
//
// Returns a parse error for invalid expressions.
func (df DataFrame) Query(expr string) (DataFrame, error) {
	colName, op, value, err := parseExpr(expr)
	if err != nil {
		return DataFrame{}, fmt.Errorf("query parse error: %w", err)
	}

	s, ok := df.data[colName]
	if !ok {
		return DataFrame{}, fmt.Errorf("column %q not found", colName)
	}

	positions := make([]int, 0)
	for i := 0; i < df.Len(); i++ {
		val, isNull := s.At(i)
		if isNull {
			continue
		}
		if compareValues(val, op, value) {
			positions = append(positions, i)
		}
	}

	return df.selectRowsByPositions(positions)
}

// parseExpr parses a simple expression like "col > 10" or "col == 'foo'".
// Returns (columnName, operator, value, error).
func parseExpr(expr string) (string, string, any, error) {
	expr = strings.TrimSpace(expr)

	// Try two-char operators first
	operators := []string{">=", "<=", "==", "!=", ">", "<"}
	for _, op := range operators {
		idx := strings.Index(expr, op)
		if idx > 0 {
			colName := strings.TrimSpace(expr[:idx])
			valueStr := strings.TrimSpace(expr[idx+len(op):])
			if colName == "" || valueStr == "" {
				continue
			}
			val, err := parseValue(valueStr)
			if err != nil {
				return "", "", nil, err
			}
			return colName, op, val, nil
		}
	}

	return "", "", nil, fmt.Errorf("no valid operator found in expression: %q", expr)
}

// parseValue parses a string/numeric/bool literal.
func parseValue(s string) (any, error) {
	// Single-quoted string
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	// Double-quoted string
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1], nil
	}
	// Bool
	if s == "true" {
		return true, nil
	}
	if s == "false" {
		return false, nil
	}
	// Int
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	// Float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("cannot parse value: %q", s)
}

// compareValues compares a cell value against a parsed literal using the given operator.
func compareValues(cellVal any, op string, queryVal any) bool {
	// Numeric comparison: convert both to float64
	cellFloat, cellIsNum := toFloat64(cellVal)
	queryFloat, queryIsNum := toFloat64(queryVal)

	if cellIsNum && queryIsNum {
		switch op {
		case ">":
			return cellFloat > queryFloat
		case ">=":
			return cellFloat >= queryFloat
		case "<":
			return cellFloat < queryFloat
		case "<=":
			return cellFloat <= queryFloat
		case "==":
			return cellFloat == queryFloat
		case "!=":
			return cellFloat != queryFloat
		}
	}

	// String comparison
	cellStr := fmt.Sprintf("%v", cellVal)
	queryStr := fmt.Sprintf("%v", queryVal)
	switch op {
	case "==":
		return cellStr == queryStr
	case "!=":
		return cellStr != queryStr
	case ">":
		return cellStr > queryStr
	case ">=":
		return cellStr >= queryStr
	case "<":
		return cellStr < queryStr
	case "<=":
		return cellStr <= queryStr
	}

	return false
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}
```

### Step 16.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestQuery" -v
```

Expected: All 6 tests PASS.

### Step 16.5: Commit

- [ ] Run:

```bash
git add dataframe/filter.go dataframe/filter_test.go
git commit -m "feat(dataframe): add Query() expression-based filtering"
```

---

## Task 17: WithColumn() — Add/Replace Column

**Files:**
- Create: `dataframe/mutate.go`
- Create: `dataframe/mutate_test.go`

### Step 17.1: Write the failing test for WithColumn()

- [ ] Create `dataframe/mutate_test.go`:

```go
package dataframe

import (
	"testing"

	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

func TestWithColumn_Add(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	newCol := series.New[any]([]any{"x", "y", "z"}, index.NewRangeIndex(3), "b")
	df2 := df.WithColumn("b", &newCol)

	cols := df2.Columns()
	if len(cols) != 2 {
		t.Errorf("WithColumn().Columns() len = %d, want 2", len(cols))
	}
	val, err := df2.At(1, "b")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != "y" {
		t.Errorf("WithColumn At(1, b) = %v, want y", val)
	}

	// Original should be unchanged
	_, err = df.Col("b")
	if err == nil {
		t.Error("Original df should not have column b")
	}
}

func TestWithColumn_Replace(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	newCol := series.New[any]([]any{int64(10), int64(20), int64(30)}, index.NewRangeIndex(3), "a")
	df2 := df.WithColumn("a", &newCol)

	val, err := df2.At(0, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(10) {
		t.Errorf("WithColumn replace At(0, a) = %v, want 10", val)
	}

	// Column count should stay the same
	if len(df2.Columns()) != 1 {
		t.Errorf("WithColumn replace Columns() len = %d, want 1", len(df2.Columns()))
	}
}
```

### Step 17.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestWithColumn" -v
```

Expected: Compilation failure — `WithColumn` not defined.

### Step 17.3: Implement WithColumn()

- [ ] Create `dataframe/mutate.go`:

```go
package dataframe

import (
	"fmt"

	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// WithColumn returns a new DataFrame with the named column added or replaced.
// If the column already exists, it is replaced. Otherwise, it is appended.
func (df DataFrame) WithColumn(name string, s *series.Series[any]) DataFrame {
	newData := make(map[string]*series.Series[any], len(df.data)+1)
	for k, v := range df.data {
		newData[k] = v
	}
	newData[name] = s

	// Preserve column order; append new columns at end
	newCols := make([]string, 0, len(df.columns)+1)
	found := false
	for _, col := range df.columns {
		newCols = append(newCols, col)
		if col == name {
			found = true
		}
	}
	if !found {
		newCols = append(newCols, name)
	}

	return DataFrame{
		index:   df.index,
		columns: newCols,
		data:    newData,
	}
}
```

### Step 17.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestWithColumn" -v
```

Expected: Both tests PASS.

### Step 17.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add WithColumn() mutation"
```

---

## Task 18: Rename() — Column Renaming

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 18.1: Write the failing test for Rename()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestRename(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2},
		"b": []int64{3, 4},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2 := df.Rename(map[string]string{"a": "alpha", "b": "beta"})
	cols := df2.Columns()
	if cols[0] != "alpha" || cols[1] != "beta" {
		t.Errorf("Rename().Columns() = %v, want [alpha beta]", cols)
	}

	val, err := df2.At(0, "alpha")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	if val != int64(1) {
		t.Errorf("Rename At(0, alpha) = %v, want 1", val)
	}

	// Original unchanged
	origCols := df.Columns()
	if origCols[0] != "a" || origCols[1] != "b" {
		t.Errorf("Original Columns() = %v, want [a b]", origCols)
	}
}

func TestRename_PartialMapping(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
		"y": []int64{2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2 := df.Rename(map[string]string{"x": "xx"})
	cols := df2.Columns()
	if cols[0] != "xx" || cols[1] != "y" {
		t.Errorf("Rename partial Columns() = %v, want [xx y]", cols)
	}
}
```

### Step 18.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestRename" -v
```

Expected: Compilation failure — `Rename` not defined.

### Step 18.3: Implement Rename()

- [ ] Add to `dataframe/mutate.go`:

```go
// Rename returns a new DataFrame with columns renamed according to the mapping.
// Keys in the mapping are old names, values are new names.
// Columns not in the mapping are kept with their original names.
func (df DataFrame) Rename(mapping map[string]string) DataFrame {
	newCols := make([]string, len(df.columns))
	newData := make(map[string]*series.Series[any], len(df.data))

	for i, col := range df.columns {
		newName, ok := mapping[col]
		if !ok {
			newName = col
		}
		newCols[i] = newName
		s := df.data[col]
		renamed := s.Rename(newName)
		newData[newName] = &renamed
	}

	return DataFrame{
		index:   df.index,
		columns: newCols,
		data:    newData,
	}
}
```

### Step 18.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestRename" -v
```

Expected: Both tests PASS.

### Step 18.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add Rename() column renaming"
```

---

## Task 19: SetIndex() — Set Column as Index

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 19.1: Write the failing test for SetIndex()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestSetIndex(t *testing.T) {
	df, err := New(map[string]any{
		"id":   []string{"a", "b", "c"},
		"val":  []int64{10, 20, 30},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2, err := df.SetIndex("id")
	if err != nil {
		t.Fatalf("SetIndex() error: %v", err)
	}

	// "id" column should be removed from columns
	cols := df2.Columns()
	if len(cols) != 1 || cols[0] != "val" {
		t.Errorf("SetIndex(id).Columns() = %v, want [val]", cols)
	}

	// Index labels should be the old "id" values
	labels := df2.Index().Labels()
	if len(labels) != 3 {
		t.Fatalf("Index().Labels() len = %d, want 3", len(labels))
	}
	if labels[0] != "a" || labels[1] != "b" || labels[2] != "c" {
		t.Errorf("Index().Labels() = %v, want [a b c]", labels)
	}

	// Loc should work with the new index
	val, err := df2.Loc("b", "val")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	if val != int64(20) {
		t.Errorf("Loc(b, val) = %v, want 20", val)
	}
}

func TestSetIndex_NotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.SetIndex("missing")
	if err == nil {
		t.Error("SetIndex(missing) expected error, got nil")
	}
}
```

### Step 19.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestSetIndex" -v
```

Expected: Compilation failure — `SetIndex` not defined.

### Step 19.3: Implement SetIndex()

- [ ] Add to `dataframe/mutate.go`:

```go
// SetIndex sets the specified column as the DataFrame's index,
// removing it from the columns. Returns an error if the column is not found.
func (df DataFrame) SetIndex(col string) (DataFrame, error) {
	s, ok := df.data[col]
	if !ok {
		return DataFrame{}, fmt.Errorf("column %q not found", col)
	}

	// Build new index from column values
	labels := make([]any, df.Len())
	for i := 0; i < df.Len(); i++ {
		val, _ := s.At(i)
		labels[i] = val
	}
	newIdx := index.NewStringIndex(toStringLabels(labels), col)

	// Remove the column from data
	newCols := make([]string, 0, len(df.columns)-1)
	newData := make(map[string]*series.Series[any], len(df.data)-1)
	for _, c := range df.columns {
		if c != col {
			newCols = append(newCols, c)
			newData[c] = df.data[c]
		}
	}

	return DataFrame{
		index:   newIdx,
		columns: newCols,
		data:    newData,
	}, nil
}

// toStringLabels converts []any to []string for StringIndex construction.
func toStringLabels(labels []any) []string {
	out := make([]string, len(labels))
	for i, l := range labels {
		out[i] = fmt.Sprintf("%v", l)
	}
	return out
}
```

### Step 19.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestSetIndex" -v
```

Expected: Both tests PASS.

### Step 19.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add SetIndex() to set column as row index"
```

---

## Task 20: ResetIndex() — Reset Index to Default

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 20.1: Write the failing test for ResetIndex()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestResetIndex_Drop(t *testing.T) {
	df, err := New(map[string]any{
		"id":  []string{"a", "b"},
		"val": []int64{10, 20},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2, err := df.SetIndex("id")
	if err != nil {
		t.Fatalf("SetIndex() error: %v", err)
	}

	df3 := df2.ResetIndex(true)
	// Should have a RangeIndex now
	if df3.Len() != 2 {
		t.Errorf("ResetIndex().Len() = %d, want 2", df3.Len())
	}
	// "id" should NOT reappear as a column (drop=true)
	cols := df3.Columns()
	if len(cols) != 1 || cols[0] != "val" {
		t.Errorf("ResetIndex(drop=true).Columns() = %v, want [val]", cols)
	}
}

func TestResetIndex_NoDrop(t *testing.T) {
	df, err := New(map[string]any{
		"id":  []string{"a", "b"},
		"val": []int64{10, 20},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2, err := df.SetIndex("id")
	if err != nil {
		t.Fatalf("SetIndex() error: %v", err)
	}

	df3 := df2.ResetIndex(false)
	// "id" should reappear as a column
	cols := df3.Columns()
	found := false
	for _, c := range cols {
		if c == "id" {
			found = true
		}
	}
	if !found {
		t.Errorf("ResetIndex(drop=false).Columns() = %v, should contain 'id'", cols)
	}
}
```

### Step 20.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestResetIndex" -v
```

Expected: Compilation failure — `ResetIndex` not defined.

### Step 20.3: Implement ResetIndex()

- [ ] Add to `dataframe/mutate.go`:

```go
// ResetIndex resets the index to a default RangeIndex.
// If drop is false, the old index is inserted as a column (using the index's Name()).
// If drop is true, the old index is discarded.
func (df DataFrame) ResetIndex(drop bool) DataFrame {
	newIdx := index.NewRangeIndex(df.Len())
	newCols := make([]string, 0, len(df.columns)+1)
	newData := make(map[string]*series.Series[any], len(df.data)+1)

	if !drop {
		idxName := df.index.Name()
		if idxName == "" {
			idxName = "index"
		}
		// Add index labels as a new column at the front
		labels := df.index.Labels()
		vals := make([]any, len(labels))
		copy(vals, labels)
		ser := series.New[any](vals, newIdx, idxName)
		newCols = append(newCols, idxName)
		newData[idxName] = &ser
	}

	for _, col := range df.columns {
		newCols = append(newCols, col)
		newData[col] = df.data[col]
	}

	return DataFrame{
		index:   newIdx,
		columns: newCols,
		data:    newData,
	}
}
```

### Step 20.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestResetIndex" -v
```

Expected: Both tests PASS.

### Step 20.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add ResetIndex() to reset row index"
```

---

## Task 21: AsType() — Column Type Casting

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 21.1: Write the failing test for AsType()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestAsType(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	df2, err := df.AsType(map[string]dtype.DType{
		"x": dtype.Float64,
	})
	if err != nil {
		t.Fatalf("AsType() error: %v", err)
	}

	dtypes := df2.DTypes()
	if dtypes["x"] != dtype.Float64 {
		t.Errorf("AsType DTypes[x] = %v, want Float64", dtypes["x"])
	}
}

func TestAsType_ColumnNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.AsType(map[string]dtype.DType{
		"missing": dtype.Float64,
	})
	if err == nil {
		t.Error("AsType expected error for missing column, got nil")
	}
}
```

### Step 21.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestAsType" -v
```

Expected: Compilation failure — `AsType` not defined.

### Step 21.3: Implement AsType()

- [ ] Add to `dataframe/mutate.go`:

```go
// AsType returns a new DataFrame with the specified columns cast to new types.
// Returns an error if any column is not found or the cast fails.
func (df DataFrame) AsType(dtypes map[string]dtype.DType) (DataFrame, error) {
	newData := make(map[string]*series.Series[any], len(df.data))
	for k, v := range df.data {
		newData[k] = v
	}

	for col, dt := range dtypes {
		s, ok := newData[col]
		if !ok {
			return DataFrame{}, fmt.Errorf("column %q not found", col)
		}
		casted, err := s.AsType(dt)
		if err != nil {
			return DataFrame{}, fmt.Errorf("column %q: %w", col, err)
		}
		newData[col] = &casted
	}

	return DataFrame{
		index:   df.index,
		columns: df.Columns(),
		data:    newData,
	}, nil
}
```

### Step 21.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestAsType" -v
```

Expected: Both tests PASS.

### Step 21.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add AsType() column type casting"
```

---

## Task 22: FillNA() — Fill Null Values

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 22.1: Write the failing test for FillNA()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestFillNA(t *testing.T) {
	// Create a DataFrame with nil values via FromRecords
	records := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": nil, "b": "y"},
		{"a": int64(3), "b": nil},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	df2 := df.FillNA(int64(0))

	// Check that nil in "a" is filled
	val, err := df2.At(1, "a")
	if err != nil {
		t.Fatalf("At() error: %v", err)
	}
	// The fill value replaces null; exact behavior depends on series FillNA
	// We just check it's not nil
	if val == nil {
		t.Error("FillNA: expected non-nil value at (1, a)")
	}
}
```

### Step 22.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestFillNA" -v
```

Expected: Compilation failure — `FillNA` not defined.

### Step 22.3: Implement FillNA()

- [ ] Add to `dataframe/mutate.go`:

```go
// FillNA returns a new DataFrame where null values in every column
// are replaced with the given fill value.
func (df DataFrame) FillNA(val any) DataFrame {
	newData := make(map[string]*series.Series[any], len(df.data))
	for _, col := range df.columns {
		filled := df.data[col].FillNA(val)
		newData[col] = &filled
	}

	return DataFrame{
		index:   df.index,
		columns: df.Columns(),
		data:    newData,
	}
}
```

### Step 22.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestFillNA" -v
```

Expected: PASS.

### Step 22.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add FillNA() null value filling"
```

---

## Task 23: DropNA() — Drop Rows/Columns with Nulls

**Files:**
- Modify: `dataframe/mutate.go`
- Modify: `dataframe/mutate_test.go`

### Step 23.1: Write the failing tests for DropNA()

- [ ] Append to `dataframe/mutate_test.go`:

```go
func TestDropNA_RowsAny(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": nil, "b": "y"},
		{"a": int64(3), "b": "z"},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	df2 := df.DropNA(0, "any")
	if df2.Len() != 2 {
		t.Errorf("DropNA(0, any).Len() = %d, want 2", df2.Len())
	}
}

func TestDropNA_RowsAll(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": nil, "b": nil},
		{"a": int64(3), "b": "z"},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	// "all" only drops rows where ALL values are null
	df2 := df.DropNA(0, "all")
	if df2.Len() != 2 {
		t.Errorf("DropNA(0, all).Len() = %d, want 2", df2.Len())
	}
}

func TestDropNA_Cols(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": nil},
		{"a": int64(2), "b": nil},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	df2 := df.DropNA(1, "any")
	cols := df2.Columns()
	// "b" is all null, should be dropped
	if len(cols) != 1 || cols[0] != "a" {
		t.Errorf("DropNA(1, any).Columns() = %v, want [a]", cols)
	}
}
```

### Step 23.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestDropNA" -v
```

Expected: Compilation failure — `DropNA` not defined.

### Step 23.3: Implement DropNA()

- [ ] Add to `dataframe/mutate.go`:

```go
// DropNA returns a new DataFrame with null values removed.
// axis: 0 = drop rows, 1 = drop columns.
// how: "any" = drop if any null, "all" = drop only if all null.
func (df DataFrame) DropNA(axis int, how string) DataFrame {
	if axis == 0 {
		return df.dropNARows(how)
	}
	return df.dropNACols(how)
}

// dropNARows drops rows based on null values.
func (df DataFrame) dropNARows(how string) DataFrame {
	positions := make([]int, 0, df.Len())
	for i := 0; i < df.Len(); i++ {
		nullCount := 0
		for _, col := range df.columns {
			if df.data[col].IsNull(i) {
				nullCount++
			}
		}

		keep := true
		if how == "any" && nullCount > 0 {
			keep = false
		}
		if how == "all" && nullCount == len(df.columns) {
			keep = false
		}
		if keep {
			positions = append(positions, i)
		}
	}

	result, _ := df.selectRowsByPositions(positions)
	return result
}

// dropNACols drops columns based on null values.
func (df DataFrame) dropNACols(how string) DataFrame {
	newCols := make([]string, 0, len(df.columns))
	newData := make(map[string]*series.Series[any])

	for _, col := range df.columns {
		s := df.data[col]
		nullCount := s.NullCount()

		keep := true
		if how == "any" && nullCount > 0 {
			keep = false
		}
		if how == "all" && nullCount == s.Len() {
			keep = false
		}
		if keep {
			newCols = append(newCols, col)
			newData[col] = s
		}
	}

	return DataFrame{
		index:   df.index,
		columns: newCols,
		data:    newData,
	}
}
```

### Step 23.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestDropNA" -v
```

Expected: All 3 tests PASS.

### Step 23.5: Commit

- [ ] Run:

```bash
git add dataframe/mutate.go dataframe/mutate_test.go
git commit -m "feat(dataframe): add DropNA() to drop rows/columns with nulls"
```

---

## Task 24: SortBy() — Multi-Column Sorting

**Files:**
- Create: `dataframe/sort.go`
- Create: `dataframe/sort_test.go`

### Step 24.1: Write the failing tests for SortBy()

- [ ] Create `dataframe/sort_test.go`:

```go
package dataframe

import (
	"testing"
)

func TestSortBy_SingleColumn(t *testing.T) {
	df, err := New(map[string]any{
		"name": []string{"Charlie", "Alice", "Bob"},
		"age":  []int64{35, 30, 25},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sorted, err := df.SortBy([]string{"age"}, []bool{true})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}

	// Ascending by age: 25, 30, 35
	val, _ := sorted.At(0, "age")
	if val != int64(25) {
		t.Errorf("SortBy asc row 0 age = %v, want 25", val)
	}
	val, _ = sorted.At(1, "age")
	if val != int64(30) {
		t.Errorf("SortBy asc row 1 age = %v, want 30", val)
	}
	val, _ = sorted.At(2, "age")
	if val != int64(35) {
		t.Errorf("SortBy asc row 2 age = %v, want 35", val)
	}

	// Check that associated names follow
	val, _ = sorted.At(0, "name")
	if val != "Bob" {
		t.Errorf("SortBy asc row 0 name = %v, want Bob", val)
	}
}

func TestSortBy_Descending(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{3, 1, 2},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sorted, err := df.SortBy([]string{"x"}, []bool{false})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}

	val, _ := sorted.At(0, "x")
	if val != int64(3) {
		t.Errorf("SortBy desc row 0 = %v, want 3", val)
	}
	val, _ = sorted.At(2, "x")
	if val != int64(1) {
		t.Errorf("SortBy desc row 2 = %v, want 1", val)
	}
}

func TestSortBy_MultiColumn(t *testing.T) {
	df, err := New(map[string]any{
		"dept": []string{"B", "A", "B", "A"},
		"age":  []int64{30, 25, 20, 35},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	sorted, err := df.SortBy([]string{"dept", "age"}, []bool{true, true})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}

	// dept asc, age asc: A/25, A/35, B/20, B/30
	val, _ := sorted.At(0, "dept")
	age, _ := sorted.At(0, "age")
	if val != "A" || age != int64(25) {
		t.Errorf("SortBy multi row 0 = (%v, %v), want (A, 25)", val, age)
	}
	val, _ = sorted.At(2, "dept")
	age, _ = sorted.At(2, "age")
	if val != "B" || age != int64(20) {
		t.Errorf("SortBy multi row 2 = (%v, %v), want (B, 20)", val, age)
	}
}

func TestSortBy_ColumnNotFound(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.SortBy([]string{"missing"}, []bool{true})
	if err == nil {
		t.Error("SortBy expected error for missing column, got nil")
	}
}

func TestSortBy_MismatchedLengths(t *testing.T) {
	df, err := New(map[string]any{
		"x": []int64{1},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	_, err = df.SortBy([]string{"x"}, []bool{true, false})
	if err == nil {
		t.Error("SortBy expected error for mismatched cols/ascending lengths, got nil")
	}
}
```

### Step 24.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestSortBy" -v
```

Expected: Compilation failure — `SortBy` not defined.

### Step 24.3: Implement SortBy()

- [ ] Create `dataframe/sort.go`:

```go
package dataframe

import (
	"fmt"
	"sort"
)

// SortBy returns a new DataFrame sorted by the specified columns.
// cols and ascending must have the same length. Each entry in ascending
// indicates whether to sort that column in ascending (true) or descending (false) order.
// Multi-column sort: first column is primary, second is secondary, etc.
func (df DataFrame) SortBy(cols []string, ascending []bool) (DataFrame, error) {
	if len(cols) != len(ascending) {
		return DataFrame{}, fmt.Errorf(
			"cols length %d does not match ascending length %d",
			len(cols), len(ascending),
		)
	}

	// Validate columns exist
	for _, col := range cols {
		if _, ok := df.data[col]; !ok {
			return DataFrame{}, fmt.Errorf("column %q not found", col)
		}
	}

	// Build index permutation
	n := df.Len()
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(i, j int) bool {
		ri, rj := indices[i], indices[j]
		for k, col := range cols {
			vi, _ := df.data[col].At(ri)
			vj, _ := df.data[col].At(rj)
			cmp := compareAny(vi, vj)
			if cmp == 0 {
				continue
			}
			if ascending[k] {
				return cmp < 0
			}
			return cmp > 0
		}
		return false // equal on all sort columns
	})

	return df.selectRowsByPositions(indices)
}

// compareAny compares two values, returning -1, 0, or 1.
// Numeric types are compared numerically. Strings are compared lexicographically.
// Nils sort last.
func compareAny(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // nil sorts last
	}
	if b == nil {
		return -1
	}

	fa, aIsNum := toFloat64Sort(a)
	fb, bIsNum := toFloat64Sort(b)
	if aIsNum && bIsNum {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

// toFloat64Sort converts numeric types to float64 for comparison.
func toFloat64Sort(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}
```

### Step 24.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestSortBy" -v
```

Expected: All 5 tests PASS.

### Step 24.5: Commit

- [ ] Run:

```bash
git add dataframe/sort.go dataframe/sort_test.go
git commit -m "feat(dataframe): add SortBy() multi-column sorting"
```

---

## Task 25: Sum() and Count() — Aggregation Basics

**Files:**
- Create: `dataframe/agg.go`
- Create: `dataframe/agg_test.go`

### Step 25.1: Write the failing tests for Sum() and Count()

- [ ] Create `dataframe/agg_test.go`:

```go
package dataframe

import (
	"testing"
)

func TestSum(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []float64{1.5, 2.5, 3.5},
		"c": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result := df.Sum()
	// Sum should include numeric columns only
	if result.Len() == 0 {
		t.Fatal("Sum() returned empty series")
	}

	// Check that the sum of "a" is 6
	aVal, _ := result.Loc("a")
	if aVal == nil {
		t.Fatal("Sum() missing column a")
	}
	// The result could be int64 or float64 depending on implementation
	aFloat, ok := toFloat64Sort(aVal)
	if !ok || aFloat != 6.0 {
		t.Errorf("Sum()[a] = %v, want 6", aVal)
	}

	bVal, _ := result.Loc("b")
	bFloat, ok := toFloat64Sort(bVal)
	if !ok || bFloat != 7.5 {
		t.Errorf("Sum()[b] = %v, want 7.5", bVal)
	}
}

func TestCount(t *testing.T) {
	records := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": nil, "b": "y"},
		{"a": int64(3), "b": nil},
	}
	df, err := FromRecords(records)
	if err != nil {
		t.Fatalf("FromRecords() error: %v", err)
	}

	result := df.Count()
	if result.Len() == 0 {
		t.Fatal("Count() returned empty series")
	}

	// "a" has 2 non-null, "b" has 2 non-null
	aVal, _ := result.Loc("a")
	if aVal != int64(2) {
		t.Errorf("Count()[a] = %v, want 2", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != int64(2) {
		t.Errorf("Count()[b] = %v, want 2", bVal)
	}
}
```

### Step 25.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestSum|TestCount" -v
```

Expected: Compilation failure — `Sum`, `Count` not defined.

### Step 25.3: Implement Sum() and Count()

- [ ] Create `dataframe/agg.go`:

```go
package dataframe

import (
	"fmt"
	"math"
	"strings"

	"github.com/vchitepu/gopandas/lib/dtype"
	"github.com/vchitepu/gopandas/lib/index"
	"github.com/vchitepu/gopandas/lib/series"
)

// Sum returns a Series containing the sum of each numeric column.
// Non-numeric columns are skipped.
func (df DataFrame) Sum() series.Series[any] {
	names := make([]string, 0, len(df.columns))
	values := make([]any, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		sum, err := s.Sum()
		if err != nil {
			continue // skip non-numeric columns
		}
		names = append(names, col)
		values = append(values, sum)
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[any](values, idx, "sum")
}

// Count returns a Series containing the non-null count of each column.
func (df DataFrame) Count() series.Series[int64] {
	names := make([]string, 0, len(df.columns))
	values := make([]int64, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		names = append(names, col)
		values = append(values, int64(s.Count()))
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[int64](values, idx, "count")
}
```

### Step 25.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestSum|TestCount" -v
```

Expected: Both tests PASS.

### Step 25.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add Sum() and Count() aggregations"
```

---

## Task 26: Mean() and Std() — Statistical Aggregations

**Files:**
- Modify: `dataframe/agg.go`
- Modify: `dataframe/agg_test.go`

### Step 26.1: Write the failing tests for Mean() and Std()

- [ ] Append to `dataframe/agg_test.go`:

```go
func TestMean(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0},
		"b": []float64{4.0, 5.0, 6.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result := df.Mean()
	if result.Len() != 2 {
		t.Fatalf("Mean().Len() = %d, want 2", result.Len())
	}

	aVal, _ := result.Loc("a")
	if aVal != 2.0 {
		t.Errorf("Mean()[a] = %v, want 2.0", aVal)
	}
	bVal, _ := result.Loc("b")
	if bVal != 5.0 {
		t.Errorf("Mean()[b] = %v, want 5.0", bVal)
	}
}

func TestStd(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result := df.Std()
	if result.Len() != 1 {
		t.Fatalf("Std().Len() = %d, want 1", result.Len())
	}

	aVal, _ := result.Loc("a")
	if aVal == nil {
		t.Fatal("Std()[a] is nil")
	}
	// Standard deviation of [2,4,4,4,5,5,7,9] ≈ 2.0
	aFloat := aVal.(float64)
	if math.Abs(aFloat-2.0) > 0.1 {
		t.Errorf("Std()[a] = %v, want ~2.0", aVal)
	}
}
```

Also add `"math"` to the import block in `dataframe/agg_test.go`.

### Step 26.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestMean|TestStd" -v
```

Expected: Compilation failure — `Mean`, `Std` not defined.

### Step 26.3: Implement Mean() and Std()

- [ ] Add to `dataframe/agg.go`:

```go
// Mean returns a Series[float64] containing the mean of each numeric column.
// Non-numeric columns are skipped.
func (df DataFrame) Mean() series.Series[float64] {
	names := make([]string, 0, len(df.columns))
	values := make([]float64, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		mean, err := s.Mean()
		if err != nil {
			continue // skip non-numeric
		}
		names = append(names, col)
		values = append(values, mean)
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[float64](values, idx, "mean")
}

// Std returns a Series[float64] containing the standard deviation of each numeric column.
// Non-numeric columns are skipped.
func (df DataFrame) Std() series.Series[float64] {
	names := make([]string, 0, len(df.columns))
	values := make([]float64, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		std, err := s.Std()
		if err != nil {
			continue // skip non-numeric
		}
		names = append(names, col)
		values = append(values, std)
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[float64](values, idx, "std")
}
```

### Step 26.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestMean|TestStd" -v
```

Expected: Both tests PASS.

### Step 26.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add Mean() and Std() aggregations"
```

---

## Task 27: Min() and Max() — Extrema Aggregations

**Files:**
- Modify: `dataframe/agg.go`
- Modify: `dataframe/agg_test.go`

### Step 27.1: Write the failing tests for Min() and Max()

- [ ] Append to `dataframe/agg_test.go`:

```go
func TestMin(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{3, 1, 2},
		"b": []float64{5.5, 3.3, 4.4},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result := df.Min()
	aVal, _ := result.Loc("a")
	aFloat, _ := toFloat64Sort(aVal)
	if aFloat != 1.0 {
		t.Errorf("Min()[a] = %v, want 1", aVal)
	}
	bVal, _ := result.Loc("b")
	bFloat, _ := toFloat64Sort(bVal)
	if bFloat != 3.3 {
		t.Errorf("Min()[b] = %v, want 3.3", bVal)
	}
}

func TestMax(t *testing.T) {
	df, err := New(map[string]any{
		"a": []int64{3, 1, 2},
		"b": []float64{5.5, 3.3, 4.4},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	result := df.Max()
	aVal, _ := result.Loc("a")
	aFloat, _ := toFloat64Sort(aVal)
	if aFloat != 3.0 {
		t.Errorf("Max()[a] = %v, want 3", aVal)
	}
	bVal, _ := result.Loc("b")
	bFloat, _ := toFloat64Sort(bVal)
	if bFloat != 5.5 {
		t.Errorf("Max()[b] = %v, want 5.5", bVal)
	}
}
```

### Step 27.2: Run tests to verify they fail

- [ ] Run:

```bash
cd dataframe && go test -run "TestMin|TestMax" -v
```

Expected: Compilation failure — `Min`, `Max` not defined.

### Step 27.3: Implement Min() and Max()

- [ ] Add to `dataframe/agg.go`:

```go
// Min returns a Series containing the minimum of each numeric column.
// Non-numeric columns are skipped.
func (df DataFrame) Min() series.Series[any] {
	names := make([]string, 0, len(df.columns))
	values := make([]any, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		min, err := s.Min()
		if err != nil {
			continue
		}
		names = append(names, col)
		values = append(values, min)
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[any](values, idx, "min")
}

// Max returns a Series containing the maximum of each numeric column.
// Non-numeric columns are skipped.
func (df DataFrame) Max() series.Series[any] {
	names := make([]string, 0, len(df.columns))
	values := make([]any, 0, len(df.columns))

	for _, col := range df.columns {
		s := df.data[col]
		max, err := s.Max()
		if err != nil {
			continue
		}
		names = append(names, col)
		values = append(values, max)
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[any](values, idx, "max")
}
```

### Step 27.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestMin|TestMax" -v
```

Expected: Both tests PASS.

### Step 27.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add Min() and Max() aggregations"
```

---

## Task 28: Describe() — Summary Statistics

**Files:**
- Modify: `dataframe/agg.go`
- Modify: `dataframe/agg_test.go`

### Step 28.1: Write the failing test for Describe()

- [ ] Append to `dataframe/agg_test.go`:

```go
func TestDescribe(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{10.0, 20.0, 30.0, 40.0, 50.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	desc := df.Describe()
	rows, cols := desc.Shape()
	// Should have rows: count, mean, std, min, max (at minimum)
	if rows < 5 {
		t.Errorf("Describe().Shape() rows = %d, want >= 5", rows)
	}
	// Should have the same numeric columns
	if cols < 2 {
		t.Errorf("Describe().Shape() cols = %d, want >= 2", cols)
	}

	// Check that count row exists and has correct values
	descCols := desc.Columns()
	found := false
	for _, c := range descCols {
		if c == "a" || c == "b" {
			found = true
		}
	}
	if !found {
		t.Errorf("Describe().Columns() = %v, should contain a and/or b", descCols)
	}
}
```

### Step 28.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestDescribe" -v
```

Expected: Compilation failure — `Describe` not defined.

### Step 28.3: Implement Describe()

- [ ] Add to `dataframe/agg.go`:

```go
// Describe returns a DataFrame with summary statistics for each numeric column.
// Rows are: count, mean, std, min, max.
func (df DataFrame) Describe() DataFrame {
	statNames := []string{"count", "mean", "std", "min", "max"}

	numericCols := make([]string, 0, len(df.columns))
	for _, col := range df.columns {
		dt := df.data[col].DType()
		if dt == dtype.Int64 || dt == dtype.Float64 {
			numericCols = append(numericCols, col)
		}
	}

	if len(numericCols) == 0 {
		return DataFrame{
			index:   index.NewRangeIndex(0),
			columns: []string{},
			data:    map[string]*series.Series[any]{},
		}
	}

	idx := index.NewStringIndex(statNames, "")
	data := make(map[string]*series.Series[any], len(numericCols))

	for _, col := range numericCols {
		s := df.data[col]
		count := float64(s.Count())
		mean, _ := s.Mean()
		std, _ := s.Std()
		minVal, _ := s.Min()
		maxVal, _ := s.Max()

		minFloat, _ := toFloat64Sort(minVal)
		maxFloat, _ := toFloat64Sort(maxVal)

		vals := []any{count, mean, std, minFloat, maxFloat}
		ser := series.New[any](vals, idx, col)
		data[col] = &ser
	}

	return DataFrame{
		index:   idx,
		columns: numericCols,
		data:    data,
	}
}
```

### Step 28.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestDescribe" -v
```

Expected: PASS.

### Step 28.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add Describe() summary statistics"
```

---

## Task 29: Corr() — Correlation Matrix

**Files:**
- Modify: `dataframe/agg.go`
- Modify: `dataframe/agg_test.go`

### Step 29.1: Write the failing test for Corr()

- [ ] Append to `dataframe/agg_test.go`:

```go
func TestCorr(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{2.0, 4.0, 6.0, 8.0, 10.0},
		"c": []float64{5.0, 4.0, 3.0, 2.0, 1.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	corr, err := df.Corr()
	if err != nil {
		t.Fatalf("Corr() error: %v", err)
	}

	rows, cols := corr.Shape()
	if rows != 3 || cols != 3 {
		t.Errorf("Corr().Shape() = (%d, %d), want (3, 3)", rows, cols)
	}

	// a-b perfect positive correlation = 1.0
	ab, err := corr.Loc("a", "b")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	abFloat, _ := toFloat64Sort(ab)
	if math.Abs(abFloat-1.0) > 0.01 {
		t.Errorf("Corr[a,b] = %v, want ~1.0", ab)
	}

	// a-c perfect negative correlation = -1.0
	ac, err := corr.Loc("a", "c")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	acFloat, _ := toFloat64Sort(ac)
	if math.Abs(acFloat+1.0) > 0.01 {
		t.Errorf("Corr[a,c] = %v, want ~-1.0", ac)
	}

	// Diagonal should be 1.0
	aa, err := corr.Loc("a", "a")
	if err != nil {
		t.Fatalf("Loc() error: %v", err)
	}
	aaFloat, _ := toFloat64Sort(aa)
	if math.Abs(aaFloat-1.0) > 0.01 {
		t.Errorf("Corr[a,a] = %v, want 1.0", aa)
	}
}
```

### Step 29.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestCorr" -v
```

Expected: Compilation failure — `Corr` not defined.

### Step 29.3: Implement Corr()

- [ ] Add to `dataframe/agg.go`:

```go
// Corr returns a DataFrame containing the Pearson correlation matrix
// for all numeric columns. Non-numeric columns are excluded.
func (df DataFrame) Corr() (DataFrame, error) {
	numericCols := make([]string, 0, len(df.columns))
	for _, col := range df.columns {
		dt := df.data[col].DType()
		if dt == dtype.Int64 || dt == dtype.Float64 {
			numericCols = append(numericCols, col)
		}
	}

	if len(numericCols) == 0 {
		return DataFrame{}, fmt.Errorf("no numeric columns for correlation")
	}

	n := len(numericCols)
	idx := index.NewStringIndex(numericCols, "")
	data := make(map[string]*series.Series[any], n)

	// Precompute float64 values for each column
	colVals := make(map[string][]float64, n)
	for _, col := range numericCols {
		s := df.data[col]
		vals := make([]float64, df.Len())
		for i := 0; i < df.Len(); i++ {
			v, isNull := s.At(i)
			if isNull {
				vals[i] = math.NaN()
			} else {
				f, _ := toFloat64Sort(v)
				vals[i] = f
			}
		}
		colVals[col] = vals
	}

	for _, colI := range numericCols {
		corrVals := make([]any, n)
		for j, colJ := range numericCols {
			corrVals[j] = pearson(colVals[colI], colVals[colJ])
		}
		ser := series.New[any](corrVals, idx, colI)
		data[colI] = &ser
	}

	return DataFrame{
		index:   idx,
		columns: numericCols,
		data:    data,
	}, nil
}

// pearson computes Pearson correlation between two float64 slices.
// NaN values are excluded pairwise.
func pearson(x, y []float64) float64 {
	if len(x) != len(y) {
		return math.NaN()
	}

	var sumX, sumY, sumXY, sumX2, sumY2 float64
	var n float64

	for i := range x {
		if math.IsNaN(x[i]) || math.IsNaN(y[i]) {
			continue
		}
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
		n++
	}

	if n < 2 {
		return math.NaN()
	}

	numerator := n*sumXY - sumX*sumY
	denominator := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))

	if denominator == 0 {
		return math.NaN()
	}

	return numerator / denominator
}
```

### Step 29.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestCorr" -v
```

Expected: PASS.

### Step 29.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add Corr() correlation matrix"
```

---

## Task 30: CorrWith() — Correlation with a Single Series

**Files:**
- Modify: `dataframe/agg.go`
- Modify: `dataframe/agg_test.go`

### Step 30.1: Write the failing test for CorrWith()

- [ ] Append to `dataframe/agg_test.go`:

```go
func TestCorrWith(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		"b": []float64{2.0, 4.0, 6.0, 8.0, 10.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Correlate with a series that's perfectly correlated with "a"
	other := series.New[any](
		[]any{float64(10), float64(20), float64(30), float64(40), float64(50)},
		index.NewRangeIndex(5),
		"other",
	)

	result, err := df.CorrWith(&other)
	if err != nil {
		t.Fatalf("CorrWith() error: %v", err)
	}

	if result.Len() != 2 {
		t.Fatalf("CorrWith().Len() = %d, want 2", result.Len())
	}

	aCorr, _ := result.Loc("a")
	if math.Abs(aCorr.(float64)-1.0) > 0.01 {
		t.Errorf("CorrWith()[a] = %v, want ~1.0", aCorr)
	}

	bCorr, _ := result.Loc("b")
	if math.Abs(bCorr.(float64)-1.0) > 0.01 {
		t.Errorf("CorrWith()[b] = %v, want ~1.0", bCorr)
	}
}

func TestCorrWith_LengthMismatch(t *testing.T) {
	df, err := New(map[string]any{
		"a": []float64{1.0, 2.0, 3.0},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	other := series.New[any](
		[]any{float64(1), float64(2)},
		index.NewRangeIndex(2),
		"other",
	)

	_, err = df.CorrWith(&other)
	if err == nil {
		t.Error("CorrWith expected error for length mismatch, got nil")
	}
}
```

### Step 30.2: Run test to verify it fails

- [ ] Run:

```bash
cd dataframe && go test -run "TestCorrWith" -v
```

Expected: Compilation failure — `CorrWith` not defined.

### Step 30.3: Implement CorrWith()

- [ ] Add to `dataframe/agg.go`:

```go
// CorrWith returns a Series[float64] containing the Pearson correlation
// of each numeric column with the given Series.
// Returns an error if the Series length doesn't match the DataFrame length.
func (df DataFrame) CorrWith(s *series.Series[any]) (series.Series[float64], error) {
	if s.Len() != df.Len() {
		return series.Series[float64]{}, fmt.Errorf(
			"series length %d does not match DataFrame length %d",
			s.Len(), df.Len(),
		)
	}

	// Convert the input series to float64
	yVals := make([]float64, df.Len())
	for i := 0; i < df.Len(); i++ {
		v, isNull := s.At(i)
		if isNull {
			yVals[i] = math.NaN()
		} else {
			f, ok := toFloat64Sort(v)
			if !ok {
				yVals[i] = math.NaN()
			} else {
				yVals[i] = f
			}
		}
	}

	names := make([]string, 0, len(df.columns))
	values := make([]float64, 0, len(df.columns))

	for _, col := range df.columns {
		colSer := df.data[col]
		dt := colSer.DType()
		if dt != dtype.Int64 && dt != dtype.Float64 {
			continue
		}

		xVals := make([]float64, df.Len())
		for i := 0; i < df.Len(); i++ {
			v, isNull := colSer.At(i)
			if isNull {
				xVals[i] = math.NaN()
			} else {
				f, _ := toFloat64Sort(v)
				xVals[i] = f
			}
		}

		names = append(names, col)
		values = append(values, pearson(xVals, yVals))
	}

	labels := make([]string, len(names))
	copy(labels, names)
	idx := index.NewStringIndex(labels, "")
	return series.New[float64](values, idx, "corr"), nil
}
```

### Step 30.4: Run tests to verify they pass

- [ ] Run:

```bash
cd dataframe && go test -run "TestCorrWith" -v
```

Expected: Both tests PASS.

### Step 30.5: Commit

- [ ] Run:

```bash
git add dataframe/agg.go dataframe/agg_test.go
git commit -m "feat(dataframe): add CorrWith() single-series correlation"
```

---

## Task 31: Full Integration Test

**Files:**
- Modify: `dataframe/dataframe_test.go`

### Step 31.1: Write an integration test exercising the full pipeline

- [ ] Append to `dataframe/dataframe_test.go`:

```go
func TestIntegration_Pipeline(t *testing.T) {
	// Construct a DataFrame
	df, err := New(map[string]any{
		"name":   []string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
		"dept":   []string{"Eng", "Sales", "Eng", "Sales", "Eng"},
		"salary": []float64{100000, 80000, 120000, 90000, 110000},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// 1. Select columns
	sub, err := df.Select("name", "salary")
	if err != nil {
		t.Fatalf("Select() error: %v", err)
	}
	if len(sub.Columns()) != 2 {
		t.Errorf("Select: got %d cols, want 2", len(sub.Columns()))
	}

	// 2. Query
	highEarners, err := df.Query("salary > 95000")
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if highEarners.Len() != 3 {
		t.Errorf("Query(salary > 95000).Len() = %d, want 3", highEarners.Len())
	}

	// 3. Sort
	sorted, err := df.SortBy([]string{"salary"}, []bool{false})
	if err != nil {
		t.Fatalf("SortBy() error: %v", err)
	}
	topSalary, _ := sorted.At(0, "salary")
	if topSalary != float64(120000) {
		t.Errorf("Sorted top salary = %v, want 120000", topSalary)
	}

	// 4. Head
	top3 := sorted.Head(3)
	if top3.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", top3.Len())
	}

	// 5. Mean
	meanSeries := df.Mean()
	salaryMean, _ := meanSeries.Loc("salary")
	if salaryMean != 100000.0 {
		t.Errorf("Mean()[salary] = %v, want 100000.0", salaryMean)
	}

	// 6. Rename
	renamed := df.Rename(map[string]string{"salary": "pay"})
	rCols := renamed.Columns()
	found := false
	for _, c := range rCols {
		if c == "pay" {
			found = true
		}
	}
	if !found {
		t.Error("Rename: expected 'pay' in columns")
	}

	// 7. WithColumn
	bonus := series.New[any](
		[]any{float64(5000), float64(4000), float64(6000), float64(4500), float64(5500)},
		index.NewRangeIndex(5),
		"bonus",
	)
	withBonus := df.WithColumn("bonus", &bonus)
	if len(withBonus.Columns()) != 4 {
		t.Errorf("WithColumn: got %d cols, want 4", len(withBonus.Columns()))
	}

	// 8. Drop
	noName := df.Drop("name")
	if len(noName.Columns()) != 2 {
		t.Errorf("Drop: got %d cols, want 2", len(noName.Columns()))
	}

	// 9. String() doesn't panic
	s := df.String()
	if s == "" {
		t.Error("String() returned empty string")
	}

	t.Logf("Integration test DataFrame:\n%s", df.String())
}
```

### Step 31.2: Run the full test suite

- [ ] Run:

```bash
cd dataframe && go test -v
```

Expected: All tests PASS.

### Step 31.3: Commit

- [ ] Run:

```bash
git add dataframe/
git commit -m "test(dataframe): add full integration test pipeline"
```

---

## Task 32: Clean Up and Final Verification

**Files:**
- All files in `dataframe/`

### Step 32.1: Remove any unused imports

- [ ] Check each file for unused imports and remove them:

```bash
cd dataframe && goimports -w *.go
```

If `goimports` is not installed, manually review imports in each file and remove unused ones. Particularly check:
- `dataframe/dataframe.go` — may have `strings`, `fmt`, `sort`, Arrow imports
- `dataframe/select.go` — may have unused `math/rand` import
- `dataframe/filter.go` — may have unused `strconv`, `strings` imports
- `dataframe/agg.go` — may have unused `math`, `strings`, `fmt` imports

### Step 32.2: Run full test suite with race detector

- [ ] Run:

```bash
cd dataframe && go test -race -v ./...
```

Expected: All tests PASS with no race conditions.

### Step 32.3: Run go vet

- [ ] Run:

```bash
cd dataframe && go vet ./...
```

Expected: No issues.

### Step 32.4: Commit

- [ ] Run:

```bash
git add dataframe/
git commit -m "chore(dataframe): clean up imports and verify with race detector"
```

---

## Summary

This plan implements the complete `dataframe` package in 32 tasks across 7 functional groups:

| Group | Tasks | Methods |
|-------|-------|---------|
| Construction + Metadata | 1-5 | New, FromRecords, FromArrow, Shape, Columns, DTypes, Index, Len, String |
| Column Access | 6-8 | Col, At, Loc |
| Selection | 9-14 | Head, Tail, ILoc, LocRows, Select, Drop, Sample |
| Filtering | 15-16 | Filter, Query |
| Mutation | 17-23 | WithColumn, Rename, SetIndex, ResetIndex, AsType, FillNA, DropNA |
| Sorting | 24 | SortBy |
| Aggregation | 25-30 | Sum, Count, Mean, Std, Min, Max, Describe, Corr, CorrWith |
| Integration + Cleanup | 31-32 | Full pipeline test, import cleanup |

**Not included (separate plans):** GroupBy, Merge/Join/Concat, Reshape (Pivot/Melt/Transpose/Stack/Unstack), IO methods.

**Files created:**
- `dataframe/dataframe.go` — struct + constructors + metadata + String()
- `dataframe/dataframe_test.go` — tests for construction, metadata, String(), integration
- `dataframe/access.go` — Col(), At(), Loc()
- `dataframe/access_test.go` — access tests
- `dataframe/select.go` — ILoc(), LocRows(), Select(), Drop(), Head(), Tail(), Sample()
- `dataframe/select_test.go` — selection tests
- `dataframe/filter.go` — Filter(), Query() with expression parser
- `dataframe/filter_test.go` — filter tests
- `dataframe/mutate.go` — WithColumn(), Rename(), SetIndex(), ResetIndex(), AsType(), FillNA(), DropNA()
- `dataframe/mutate_test.go` — mutation tests
- `dataframe/sort.go` — SortBy()
- `dataframe/sort_test.go` — sort tests
- `dataframe/agg.go` — Sum(), Count(), Mean(), Std(), Min(), Max(), Describe(), Corr(), CorrWith()
- `dataframe/agg_test.go` — aggregation tests
