# gopandas Design Spec

**Date:** 2026-04-07  
**Status:** Approved

---

## Overview

`gopandas` is a Go-native data analysis and manipulation library modeled after Python's [pandas](https://pandas.pydata.org/). It provides a `DataFrame` and `Series` abstraction backed by Apache Arrow columnar storage, plus a CLI for processing data files directly from the terminal.

The goal is a conceptual 1:1 mapping of pandas' core public API, using Go-idiomatic naming (exported, CamelCase). Scope for this version is the core subset: DataFrame, Series, Index, GroupBy, IO (CSV/JSON/Parquet), and a CLI tool.

---

## Architecture

```
gopandas/
├── dtype/         # Column dtype enum + value types
├── series/        # Series[T] — single typed column with Index
├── dataframe/     # DataFrame — ordered map of named Series + shared Index
├── index/         # Index types: RangeIndex, StringIndex, MultiIndex
├── groupby/       # GroupBy — split-apply-combine engine
├── io/            # Readers/writers: CSV, JSON, Parquet
├── ops/           # Core operations: filter, sort, merge, reshape, pivot
├── stats/         # Aggregations: mean, std, sum, count, min, max, corr
├── arrow/         # Internal Arrow adapter (wraps apache/arrow-go)
└── cmd/           # CLI binary (cobra)
```

**Go version:** 1.21+ (generics required)  
**Key dependency:** `github.com/apache/arrow-go/v18` for columnar array storage

---

## Numpy / pandas Dependency Gap

pandas depends on NumPy for its underlying array model. In Go, this gap is filled by Apache Arrow Go:

| pandas/numpy concept | gopandas equivalent |
|---|---|
| `np.ndarray` | `arrow.Array` (apache/arrow-go) |
| `pd.NA` / `np.nan` | Arrow null bitmap |
| `dtype('int64')` | `dtype.Int64` |
| `dtype('float64')` | `dtype.Float64` |
| `dtype('object')` / string | `dtype.String` |
| `dtype('bool')` | `dtype.Bool` |
| `dtype('datetime64[ns]')` | `dtype.Timestamp` (Arrow TimestampArray) |
| `pd.Categorical` | `dtype.Dictionary` (Arrow DictionaryArray) |

Arrow provides: typed nullable arrays, zero-copy slicing, Parquet interop, and active Go support. No C dependencies in the Go Arrow implementation.

---

## Data Model

### dtype

```go
package dtype

type DType int

const (
    Invalid DType = iota
    Int64
    Float64
    String
    Bool
    Timestamp
    Dictionary // categorical
)
```

### Index

```go
package index

// Index is the row label abstraction
type Index interface {
    Len() int
    Labels() []any
    Loc(label any) (int, bool)  // label -> positional offset
    Slice(start, end int) Index
    Name() string
}

// Concrete types
type RangeIndex struct { ... }   // 0..n-1, default
type StringIndex struct { ... }  // []string labels
type Int64Index struct { ... }   // []int64 labels
type MultiIndex struct { ... }   // hierarchical: [][]any
```

### Series[T]

A single named column with an Index. Backed by an Arrow array.

```go
package series

type Series[T any] struct {
    name  string
    index index.Index
    arr   arrow.Array   // underlying Arrow array
}

// Construction
func New[T any](values []T, idx index.Index, name string) Series[T]
func FromArrow(arr arrow.Array, idx index.Index, name string) Series[any]

// Metadata
func (s Series[T]) Len() int
func (s Series[T]) Name() string
func (s Series[T]) DType() dtype.DType
func (s Series[T]) Index() index.Index

// Access
func (s Series[T]) At(i int) (T, bool)       // positional, bool = isNull
func (s Series[T]) Loc(label any) (T, bool)  // label-based
func (s Series[T]) Values() []T
func (s Series[T]) IsNull(i int) bool

// Slicing / selection
func (s Series[T]) Head(n int) Series[T]
func (s Series[T]) Tail(n int) Series[T]
func (s Series[T]) ILoc(start, end int) Series[T]
func (s Series[T]) Filter(mask []bool) Series[T]

// Transformation (returns new Series, immutable)
func (s Series[T]) Map(fn func(T) T) Series[T]
func (s Series[T]) Apply(fn func(T) any) Series[any]
func (s Series[T]) Sort(ascending bool) Series[T]
func (s Series[T]) DropNA() Series[T]
func (s Series[T]) FillNA(val T) Series[T]
func (s Series[T]) Unique() Series[T]
func (s Series[T]) ValueCounts() Series[int64]
func (s Series[T]) Rename(name string) Series[T]
func (s Series[T]) AsType(d dtype.DType) (Series[any], error)

// Statistics
func (s Series[T]) Sum() (T, error)
func (s Series[T]) Mean() (float64, error)
func (s Series[T]) Std() (float64, error)
func (s Series[T]) Var() (float64, error)
func (s Series[T]) Min() (T, error)
func (s Series[T]) Max() (T, error)
func (s Series[T]) Median() (float64, error)
func (s Series[T]) Quantile(q float64) (float64, error)
func (s Series[T]) Count() int       // non-null count
func (s Series[T]) NullCount() int
func (s Series[T]) Describe() map[string]float64  // returns stats map; DataFrame-returning version lives in stats package
```

### DataFrame

An ordered collection of named Series sharing a common Index.

```go
package dataframe

type DataFrame struct {
    index   index.Index
    columns []string
    data    map[string]series.Series[any]
}

// Construction
func New(cols map[string]any) (DataFrame, error)
func FromRecords(records []map[string]any) (DataFrame, error)
func FromArrow(rec arrow.Record) DataFrame

// IO construction (in io package, returns DataFrame)
// dataframe.FromCSV, dataframe.FromJSON, dataframe.FromParquet

// Metadata
func (df DataFrame) Shape() (rows, cols int)
func (df DataFrame) Columns() []string
func (df DataFrame) DTypes() map[string]dtype.DType
func (df DataFrame) Index() index.Index
func (df DataFrame) Len() int

// Access
func (df DataFrame) Col(name string) (series.Series[any], error)
func (df DataFrame) At(row int, col string) (any, error)        // iloc-style row
func (df DataFrame) Loc(label any, col string) (any, error)     // label-based row

// Selection
func (df DataFrame) ILoc(rowStart, rowEnd, colStart, colEnd int) (DataFrame, error)   // positional slice
func (df DataFrame) LocRows(labels []any) (DataFrame, error)    // label-based row selection
func (df DataFrame) Select(cols ...string) (DataFrame, error)
func (df DataFrame) Drop(cols ...string) DataFrame
func (df DataFrame) Filter(mask series.Series[bool]) (DataFrame, error)
func (df DataFrame) Query(expr string) (DataFrame, error)       // simple expression: "col > 10"
func (df DataFrame) Head(n int) DataFrame
func (df DataFrame) Tail(n int) DataFrame
func (df DataFrame) Sample(n int, seed int64) (DataFrame, error)

// Mutation (returns new DataFrame)
func (df DataFrame) WithColumn(name string, s series.Series[any]) DataFrame
func (df DataFrame) Rename(mapping map[string]string) DataFrame
func (df DataFrame) SetIndex(col string) (DataFrame, error)
func (df DataFrame) ResetIndex(drop bool) DataFrame
func (df DataFrame) AsType(dtypes map[string]dtype.DType) (DataFrame, error)
func (df DataFrame) FillNA(val any) DataFrame
func (df DataFrame) DropNA(axis int, how string) DataFrame    // axis: 0=rows, 1=cols; how: "any"/"all"

// Sort
func (df DataFrame) SortBy(cols []string, ascending []bool) (DataFrame, error)

// Aggregation
func (df DataFrame) Describe() DataFrame
func (df DataFrame) Sum() series.Series[any]
func (df DataFrame) Mean() series.Series[float64]
func (df DataFrame) Std() series.Series[float64]
func (df DataFrame) Min() series.Series[any]
func (df DataFrame) Max() series.Series[any]
func (df DataFrame) Count() series.Series[int64]
func (df DataFrame) Corr() (DataFrame, error)
func (df DataFrame) CorrWith(s series.Series[any]) (series.Series[float64], error)

// GroupBy
func (df DataFrame) GroupBy(cols ...string) groupby.GroupBy

// Join / merge
func (df DataFrame) Merge(other DataFrame, on []string, how ops.JoinType) (DataFrame, error)
func (df DataFrame) Join(other DataFrame, how ops.JoinType, lsuffix, rsuffix string) (DataFrame, error)
func (df DataFrame) Concat(others ...DataFrame) (DataFrame, error)

// Reshape
func (df DataFrame) Pivot(index, columns, values string) (DataFrame, error)
func (df DataFrame) PivotTable(index, columns, values []string, aggFunc ops.AggFunc) (DataFrame, error)
func (df DataFrame) Melt(idVars, valueVars []string, varName, valueName string) DataFrame
func (df DataFrame) Transpose() DataFrame
func (df DataFrame) Stack() (DataFrame, error)
func (df DataFrame) Unstack() (DataFrame, error)

// IO
func (df DataFrame) ToCSV(w io.Writer, opts ...io.CSVOption) error
func (df DataFrame) ToJSON(w io.Writer, orient io.JSONOrient) error
func (df DataFrame) ToParquet(w io.Writer) error
func (df DataFrame) String() string   // tabular display
```

### GroupBy

```go
package groupby

type GroupBy struct {
    df    dataframe.DataFrame
    keys  []string
    groups map[string][]int   // group key -> row indices; multi-column keys are joined as "val1|val2"
}

func (gb GroupBy) Sum() dataframe.DataFrame
func (gb GroupBy) Mean() dataframe.DataFrame
func (gb GroupBy) Count() dataframe.DataFrame
func (gb GroupBy) Min() dataframe.DataFrame
func (gb GroupBy) Max() dataframe.DataFrame
func (gb GroupBy) Std() dataframe.DataFrame
func (gb GroupBy) First() dataframe.DataFrame
func (gb GroupBy) Last() dataframe.DataFrame
func (gb GroupBy) Agg(fns map[string]ops.AggFunc) (dataframe.DataFrame, error)
func (gb GroupBy) Apply(fn func(dataframe.DataFrame) dataframe.DataFrame) dataframe.DataFrame
func (gb GroupBy) Transform(fn func(series.Series[any]) series.Series[any]) dataframe.DataFrame
func (gb GroupBy) Size() series.Series[int64]
func (gb GroupBy) NGroups() int
func (gb GroupBy) Groups() map[string][]int
```

---

## IO Layer

All IO lives in the `io` package (or `iocsv`, `iojson`, `ioparquet` sub-packages to avoid name collision with stdlib `io`).

### CSV

```go
type CSVOption func(*csvConfig)

func WithSep(sep rune) CSVOption
func WithHeader(has bool) CSVOption
func WithIndexCol(col string) CSVOption
func WithUseCols(cols []string) CSVOption
func WithNAValues(vals []string) CSVOption
func WithNRows(n int) CSVOption
func WithSkipRows(n int) CSVOption
func WithDTypeOverride(col string, d dtype.DType) CSVOption

func FromCSV(r io.Reader, opts ...CSVOption) (dataframe.DataFrame, error)
func (df dataframe.DataFrame) ToCSV(w io.Writer, opts ...CSVOption) error
```

### JSON

```go
type JSONOrient int

const (
    OrientRecords JSONOrient = iota  // [{col: val, ...}, ...]  (default)
    OrientColumns                    // {col: [val, ...], ...}
    OrientIndex                      // {idx: {col: val, ...}, ...}
)

func FromJSON(r io.Reader, orient JSONOrient) (dataframe.DataFrame, error)
func (df dataframe.DataFrame) ToJSON(w io.Writer, orient JSONOrient) error
```

### Parquet

```go
func FromParquet(r io.ReaderAt, size int64) (dataframe.DataFrame, error)
func (df dataframe.DataFrame) ToParquet(w io.Writer) error
```

---

## CLI Design

Binary: `gopandas` (built from `cmd/gopandas/main.go` using [cobra](https://github.com/spf13/cobra)).

### Subcommands

```
gopandas read <file> [flags]
  --head int           Print first N rows (default 5)
  --tail int           Print last N rows
  --describe           Print descriptive statistics
  --shape              Print (rows, cols)
  --dtypes             Print column names and dtypes
  --select col1,col2   Select specific columns
  --filter "expr"      Filter rows by expression (e.g. "price > 100")
  --groupby col        Group by column
  --agg sum|mean|count Aggregation to apply (used with --groupby)
  --sort col           Sort by column
  --sort-desc          Sort descending (used with --sort)
  --output file        Write result to file (infers format from extension)
  --format csv|json|parquet  Output format (overrides extension)

gopandas convert <input> <output> [flags]
  --from csv|json|parquet   Input format (default: infer from extension)
  --to   csv|json|parquet   Output format (default: infer from extension)
  --select col1,col2        Select columns during conversion
```

### Output Formatting

Default terminal output is a tabular text representation (like pandas `print(df)`). Columns are right-aligned for numeric, left-aligned for string. Long DataFrames truncate the middle rows with `...` indicators.

---

## Error Handling

- All fallible public functions return `(T, error)` — no silent failures.
- Errors are descriptive: `column "foo" not found`, `shape mismatch: expected 100 rows, got 50`, etc.
- Panics are reserved for internal programming errors (invariant violations that should never occur in correct usage).
- Type coercion errors surface as errors, not panics.
- `Query()` / expression parser returns a parse error on invalid expressions rather than empty results.

---

## Testing Strategy

- Unit tests per package (`series/series_test.go`, `dataframe/dataframe_test.go`, etc.)
- Table-driven tests for all operations with golden CSV fixtures in `testdata/`
- Tests validate output against known-correct results (pre-computed with Python pandas)
- Fuzz targets for CSV parser and Query expression parser
- Integration tests in `cmd/` that invoke the CLI binary against fixture files and compare stdout
- Benchmark tests for GroupBy, Merge, and large CSV reads

---

## Explicitly Out of Scope (v1)

The following pandas features are deferred to future versions:

- **REPL** / interactive mode
- **Time series** resampling (`resample`, `DatetimeIndex` frequency math)
- **Rolling / expanding / EWM** windows
- **Sparse arrays**
- **Excel IO** (`.xlsx`)
- **SQL IO** (`read_sql`, `to_sql`)
- **HDF5 IO**
- **Styler** (HTML display formatting)
- **Period / PeriodIndex**
- **IntervalIndex**
- **Extension types** (custom dtypes)
- **Parallel execution** (no goroutine-based parallelism in v1)

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/apache/arrow-go/v18` | Columnar array storage (numpy replacement) |
| `github.com/spf13/cobra` | CLI framework |
| `gonum.org/v1/gonum/stat` | Statistical functions (std, variance, correlation) |

---

## Module Name

`github.com/vinaychitepu/gopandas`
