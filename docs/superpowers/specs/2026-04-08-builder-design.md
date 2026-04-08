# Builder / Fluent API Design

**Date:** 2026-04-08  
**Status:** Approved

## Problem

Most `DataFrame` methods return `(DataFrame, error)`. Go does not allow calling a method on a multi-return value, so chaining like `df.Select(...).Query(...)` is a compile error. Users must either use intermediate variables or accept boilerplate error checks between every step.

## Goal

Enable a fluent chain like:

```go
result, err := df.Build().
    Select("name", "age", "salary").
    Query("age >= 30").
    SortBy([]string{"salary"}, []bool{false}).
    Head(10).
    Result()
```

## Approach

Option A: `Builder` struct inside the `dataframe` package.

## Architecture

### Type

```go
// dataframe/builder.go
type Builder struct {
    df  DataFrame
    err error
}
```

### Entry Point

```go
func (df DataFrame) Build() *Builder {
    return &Builder{df: df}
}
```

### Short-circuit Pattern

Every method checks `b.err != nil` before applying its operation. If an error is already stored, the method is a no-op and returns `b`. This is the standard Go error-accumulator pattern.

### Terminal Method

```go
func (b *Builder) Result() (DataFrame, error) {
    return b.df, b.err
}
```

## Methods

### Transform methods (wrap `(DataFrame, error)` returns)

| Builder method | Delegates to |
|---|---|
| `Select(cols ...string)` | `df.Select` |
| `Query(expr string)` | `df.Query` |
| `Filter(mask series.Series[bool])` | `df.Filter` |
| `SortBy(cols []string, ascending []bool)` | `df.SortBy` |
| `ILoc(rowStart, rowEnd, colStart, colEnd int)` | `df.ILoc` |
| `LocRows(labels []any)` | `df.LocRows` |
| `Sample(n int, seed int64)` | `df.Sample` |
| `WithColumn(name string, s *series.Series[any])` | `df.WithColumn` |
| `AsType(dtypes map[string]dtype.DType)` | `df.AsType` |
| `SetIndex(col string)` | `df.SetIndex` |
| `DropNA(axis int, how string)` | `df.DropNA` |

### Transform methods (wrap plain `DataFrame` returns)

| Builder method | Delegates to |
|---|---|
| `Drop(cols ...string)` | `df.Drop` |
| `Head(n int)` | `df.Head` |
| `Tail(n int)` | `df.Tail` |
| `Rename(mapping map[string]string)` | `df.Rename` |
| `ResetIndex(drop bool)` | `df.ResetIndex` |
| `FillNA(val any)` | `df.FillNA` |

### Aggregation methods (return `DataFrame`, fit in chain)

| Builder method | Delegates to |
|---|---|
| `Describe()` | `df.Describe` |
| `Corr()` | `df.Corr` |

**Excluded:** `Sum`, `Mean`, `Std`, `Min`, `Max`, `Count` — these return `series.Series[any]`, not `DataFrame`, so they cannot be part of a `DataFrame` chain.

## Error Handling

- The first error encountered is stored in `b.err`.
- All subsequent method calls are no-ops.
- `Result()` always returns `(b.df, b.err)` — the last successful `DataFrame` state and the first error.
- No errors are silently dropped.

Example:
```go
result, err := df.Build().
    Select("name", "salary").   // error: column not found
    Query("salary > 50000").    // skipped
    Result()
// err = "column not found: salary"
```

## Files

| File | Purpose |
|---|---|
| `dataframe/builder.go` | `Builder` struct, `Build()`, all chained methods, `Result()` |
| `dataframe/builder_test.go` | Unit tests |

## Tests

1. **Happy path** — full chain of `Select → Query → SortBy → Head` produces correct result
2. **Error short-circuit** — error on step 2 skips steps 3+, `Result()` returns that error
3. **No-op chain** — `df.Build().Result()` returns `df` unchanged with `nil` error
4. **Aggregation terminal** — `df.Build().Select(...).Describe().Result()` produces describe DataFrame
5. **Plain-return methods** — `Drop`, `Head`, `Tail`, `FillNA`, `Rename` work correctly in chain
