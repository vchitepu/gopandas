# gopandas

`gopandas` is a Go-native data analysis library inspired by pandas, plus a CLI for common data file workflows.

It includes:
- `DataFrame` and `Series`
- GroupBy + aggregations
- Core reshape/merge operations
- CSV/JSON/Parquet/XLSX I/O
- `gopandas` CLI (`read`, `convert`)

## Install

As a library:

```bash
go get github.com/vchitepu/gopandas
```

Build the CLI from source:

```bash
go build -o gopandas ./cmd/gopandas
```

Install the CLI binary with `go install`:

```bash
go install github.com/vchitepu/gopandas/cmd/gopandas@latest
```

## Library Usage

### 1) Create a DataFrame from records

```go
package main

import (
	"fmt"

	"github.com/vchitepu/gopandas/lib/dataframe"
)

func main() {
	records := []map[string]any{
		{"name": "Alice", "age": 30, "city": "New York", "salary": 75000.50},
		{"name": "Bob", "age": 25, "city": "San Francisco", "salary": 82000.00},
		{"name": "Charlie", "age": 35, "city": "Chicago", "salary": 68000.75},
	}

	df, err := dataframe.FromRecords(records)
	if err != nil {
		panic(err)
	}

	fmt.Println(df.String())
}
```

### 2) Select, filter, and sort (common pandas-style workflow)

```go
selected, err := df.Select("name", "age", "salary")
if err != nil {
	panic(err)
}

filtered, err := selected.Query("age >= 30")
if err != nil {
	panic(err)
}

sorted, err := filtered.SortBy([]string{"salary"}, []bool{false}) // descending
if err != nil {
	panic(err)
}

fmt.Println(sorted.Head(5).String())
```

### 3) GroupBy + aggregation

```go
package main

import (
	"fmt"

	"github.com/vchitepu/gopandas/lib/groupby"
)

func exampleGroupBy(df dataframe.DataFrame) {
	gb := groupby.NewGroupBy(df, "city")
	result, err := gb.Mean()
	if err != nil {
		panic(err)
	}
	fmt.Println(result.String())
}
```

### 4) Read and write CSV/JSON files

```go
package main

import (
	"os"

	csvio "github.com/vchitepu/gopandas/lib/dataio/csv"
	jsonio "github.com/vchitepu/gopandas/lib/dataio/json"
)

func exampleIO() {
	in, err := os.Open("testdata/employees.csv")
	if err != nil {
		panic(err)
	}
	defer in.Close()

	df, err := csvio.FromCSV(in)
	if err != nil {
		panic(err)
	}

	out, err := os.Create("employees.json")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	if err := jsonio.ToJSON(df, out, jsonio.OrientRecords); err != nil {
		panic(err)
	}
}
```

### 5) Read and write XLSX files (`lib/dataio/excel`)

```go
package main

import (
	"os"

	excelio "github.com/vchitepu/gopandas/lib/dataio/excel"
)

func exampleExcelIO() {
	in, err := os.Open("testdata/employees.xlsx")
	if err != nil {
		panic(err)
	}
	defer in.Close()

	df, err := excelio.FromXLSX(in)
	if err != nil {
		panic(err)
	}

	out, err := os.Create("employees_copy.xlsx")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	if err := excelio.ToXLSX(df, out); err != nil {
		panic(err)
	}
}
```

### 6) Parse CSV date columns as timestamps

```go
in, err := os.Open("testdata/sales.csv")
if err != nil {
	panic(err)
}
defer in.Close()

df, err := csvio.FromCSV(
	in,
	csvio.WithParseDates([]string{"Date"}),
	// optional explicit format (Go layout syntax)
	csvio.WithDateFormats([]string{"01/02/2006"}),
)
if err != nil {
	panic(err)
}

fmt.Println(df.DTypes()) // Date => timestamp
```

### 7) Builder / Fluent API

Use the builder when you want to chain DataFrame operations in one pipeline without handling intermediate errors at each step.

```go
result, err := df.Build().
	Select("name", "age", "salary").
	Query("age >= 30").
	SortBy([]string{"salary"}, []bool{false}). // descending
	Head(5).
	Result()
if err != nil {
	panic(err)
}

fmt.Println(result.String())
```

Builder calls short-circuit on the first error: once any step fails, subsequent chained calls are no-ops, and `Result()` returns that original error.

## CLI Usage

Build once:

```bash
go build -o gopandas ./cmd/gopandas
```

### Read a file

```bash
gopandas read testdata/employees.csv
gopandas read testdata/employees.csv --head 3
gopandas read testdata/employees.csv --shape
gopandas read testdata/employees.csv --dtypes
gopandas read testdata/employees.csv --describe
```

### Parse dates in CSV columns

```bash
gopandas read testdata/sales.csv --parse-dates Date --dtypes
gopandas read testdata/sales.csv --parse-dates Date --date-format 01/02/2006 --dtypes
gopandas read testdata/sales.csv --parse-dates Date --date-format 01/02/2006 --filter "Date > '11/12/2025'"
```

Tip: when filtering date columns, wrap date literals in quotes in the query string.

### Select/filter/sort from CLI

```bash
gopandas read testdata/employees.csv --select name,salary --filter "salary > 80000" --sort salary --sort-desc
```

### Group and aggregate

```bash
gopandas read testdata/employees.csv --groupby city --agg mean
gopandas read testdata/employees.csv --groupby city --agg count
```

### Write transformed output

```bash
gopandas read testdata/employees.csv --select name,age --output employees_subset.csv
gopandas read testdata/employees.csv --filter "age >= 30" --output employees_30_plus.json --format json
```

### Read XLSX from CLI

```bash
gopandas read testdata/employees.xlsx
gopandas read testdata/employees.xlsx --head 10
gopandas read testdata/employees.xlsx --filter "salary > 90000"
```

### Convert between formats

```bash
gopandas convert testdata/employees.csv employees.json
gopandas convert testdata/simple.json simple.csv
gopandas convert testdata/sales.csv sales.parquet --from csv --to parquet
gopandas convert testdata/employees.csv employees_subset.csv --select name,age
gopandas convert testdata/employees.xlsx output.csv
gopandas convert testdata/employees.csv output.xlsx
```

## Planned Features

Planned for future versions:

- REPL / interactive mode
- Time series resampling (`resample`, frequency math)
- Rolling / expanding / EWM windows
- Sparse arrays
- SQL I/O (`read_sql`, `to_sql`)
- HDF5 I/O
- Styler (HTML display formatting)
- `Period` / `PeriodIndex`
- `IntervalIndex`
- Extension types (custom dtypes)
- Parallel execution

## Running Tests

```bash
go test ./...
```
