# Terminal Visualization Component Design

**Date:** 2026-04-12
**Status:** Approved

---

## Overview

Add a terminal visualization component to the `gopandas` CLI that renders rich, styled output directly in the terminal — no image files, no web browser. The component covers three modes:

1. **Enhanced table display** — styled borders, colored headers, numeric alignment
2. **Charts** — horizontal bar chart, histogram (8-level block), line chart (Braille grid)
3. **Summary dashboard** — a single framed panel with shape, column dtypes, statistics table, and data preview

All output uses `charmbracelet/lipgloss` for styling, with automatic dark/light theme detection mirroring the pattern from the `weave` CLI (shine project).

---

## Architecture

### Package Structure

```
cmd/gopandas/viz/
  theme.go       # Theme struct + DarkTheme()/LightTheme() + Detect()
  table.go       # RenderTable(df DataFrame, th Theme, termWidth int) string
  chart.go       # RenderBar/Histogram/Line(df DataFrame, opts ChartOptions, th Theme, termWidth int) string
  summary.go     # RenderSummary(df DataFrame, th Theme, termWidth int) string
  render.go      # VizOptions struct + Render(df DataFrame, opts VizOptions) string (dispatch entrypoint)
```

The `viz` package is internal to the CLI binary. It does not become part of the `lib/` data library, keeping the core library free of terminal rendering dependencies.

### New Dependency

```
github.com/charmbracelet/lipgloss v1.1.0
golang.org/x/term v0.41.0
```

These are the same versions used in the weave/shine project. `golang.org/x/term` is used to detect terminal width and TTY status.

---

## CLI Changes

### New Flags on `read` Command

```
--viz [type]      Visualization mode: bar | histogram | line | table | summary
--x col           Column for x-axis (bar, line) or bin source (histogram)
--y col           Column for y-axis / values (bar, line; defaults to count for histogram)
--bins N          Number of bins for histogram (default: 10)
--theme [mode]    Theme override: dark | light (default: auto-detect)
```

### Flag Semantics

- `--viz` is optional. When absent, the existing plain-text `df.String()` output is used unchanged.
- `--viz` composes freely with the existing pipeline flags (`--filter`, `--select`, `--groupby`, `--sort`). The pipeline runs first; visualization operates on the resulting DataFrame.
- `--x` defaults to the first column if omitted.
- `--y` defaults to the second numeric column if omitted (errors if no numeric column found).
- `--bins` only applies when `--viz histogram`.
- `--theme` defaults to auto-detect via `GOPANDAS_THEME` env var → `COLORFGBG` → `TERM_PROGRAM` heuristics → dark.

### Example Invocations

```bash
# Styled table output
gopandas read data.csv --viz table

# Bar chart: city on x-axis, mean salary on y-axis (after groupby pipeline)
gopandas read data.csv --groupby city --agg mean --viz bar --x city --y salary

# Histogram of salary column with 15 bins
gopandas read data.csv --viz histogram --x salary --bins 15

# Line chart of a time series
gopandas read transactions.csv --parse-dates Date --sort Date --viz line --x Date --y amount

# Full summary dashboard
gopandas read data.csv --viz summary
```

---

## Theme

### `Theme` Struct (`theme.go`)

```go
type Theme struct {
    // Text
    Normal lipgloss.Style
    Bold   lipgloss.Style
    Dim    lipgloss.Style

    // Headings (used for section labels in dashboard)
    SectionHeader lipgloss.Style

    // Table
    TableHeader lipgloss.Style
    TableBorder lipgloss.Color

    // Charts
    BarColor      lipgloss.Color
    AxisColor     lipgloss.Style
    ChartBorder   lipgloss.Color

    // Dashboard outer panel
    PanelBorder   lipgloss.Color
    PanelTitle    lipgloss.Style
}
```

### Dark Theme Colors (matching weave palette)

| Element | Color |
|---|---|
| Table header | `#8BA4D4` (blue-gray, bold) |
| Table border | `#3A3F4B` (dark gray) |
| Bar fill | `#8BA4D4` |
| Axis labels | faint |
| Section header | `#C9A86A` (amber) |
| Panel border | `#7B74A6` (purple-gray) |
| Panel title | bold white |

### Light Theme Colors

| Element | Color |
|---|---|
| Table header | `#3F5F8A` (navy blue, bold) |
| Table border | `#C2C7D0` (light gray) |
| Bar fill | `#3F5F8A` |
| Section header | `#8D6B3F` (warm brown) |
| Panel border | `#7D73A3` |

### Auto-Detection (`Detect(flagValue string) Theme`)

1. Flag override (`--theme dark` / `--theme light`)
2. `GOPANDAS_THEME` env var
3. `COLORFGBG` env var (bg component < 128 → dark)
4. `TERM_PROGRAM=Apple_Terminal` → light
5. Default → dark

---

## Styled Table (`table.go`)

### Behavior

- Box-drawing borders: `┌ ─ ┬ ┐ │ ├ ┼ ┤ └ ┴ ┘`
- Header row: bold, `TableHeader` color
- Numeric columns (int64, float64): right-aligned
- String/timestamp columns: left-aligned
- Column widths: auto-sized to content, then proportionally shrunk if total exceeds terminal width (identical algorithm to weave's `fitTableWidths`)
- Minimum column width: 6 characters (truncates with `…` if needed)
- Long tables: if rows > 50, shows first 25 + `  … (N rows omitted) …` (faint) + last 25
- TTY guard: TTY detection lives in `Render()` (the dispatch entrypoint). If stdout is not a TTY, `Render()` returns plain `df.String()` directly and individual renderers (including `RenderTable`) never need to check TTY themselves.

### Function Signature

```go
func RenderTable(df dataframe.DataFrame, th Theme, termWidth int) string
```

---

## Charts (`chart.go`)

### Common Options

```go
type ChartOptions struct {
    Type      string // "bar" | "histogram" | "line"
    XCol      string
    YCol      string
    Bins      int    // histogram only; default 10
    Title     string // auto-generated if empty
}
```

### Bar Chart

- Horizontal layout: `label ████████████ value`
- Labels left-padded to uniform width (longest label width)
- Bar length proportional to y value; max bar width = `termWidth - labelWidth - gutter (4 chars)`
- Bar rendered using `█` (U+2588 FULL BLOCK) in `BarColor`
- Value at end of bar: right-aligned, faint style
- Negative values: use `░` (U+2591 LIGHT SHADE) extending left from center axis
- Zero-value bars display as a single `▏` mark

Example:
```
New York      ████████████████████  82,000.00
Chicago       █████████████████     68,000.75
San Francisco ████████████████████  75,000.50
```

### Histogram

- 8-level vertical bars using Unicode block elements: `▁▂▃▄▅▆▇█`
- Each bar represents one bin; bin boundaries shown on x-axis
- Auto-bins: equal-width bins from `min(col)` to `max(col)` in N steps
- Y-axis: count of values per bin, drawn on the left with faint labels
- X-axis: bin start values, shown below bars in faint style
- Chart height: default 10 rows (adjusts to terminal height)

### Line Chart

- 2D grid rendered with Braille Unicode characters (`⠀` through `⣿`) for smooth curves
- Grid size: `termWidth - yAxisWidth - 2` columns × `min(20, termHeight/2)` rows
- X must be numeric or timestamp; data is sorted by x before rendering
- Y-axis labels drawn on left, x-axis labels drawn below
- Missing/null values: gap in line (no dot rendered at that position)
- Title drawn above chart

### Chart Title

Auto-generated as: `<filename> | <xcol> vs <ycol>` or `<filename> | distribution of <xcol>` for histogram.

---

## Summary Dashboard (`summary.go`)

### Layout

```
┌─ data.csv ─────────────────────────────────┐
│  150 rows × 5 columns                      │
├─ COLUMNS ──────────────────────────────────┤
│  name      string                          │
│  age       int64                           │
│  salary    float64                         │
│  city      string                          │
│  hired     timestamp                       │
├─ STATISTICS ───────────────────────────────┤
│  ┌────────┬───────┬────────┬──────┬──────┐ │
│  │        │ count │   mean │  std │  max │ │
│  ├────────┼───────┼────────┼──────┼──────┤ │
│  │ age    │   150 │   32.4 │  8.1 │   55 │ │
│  │ salary │   150 │ 74,200 │12,800│ 115k │ │
│  └────────┴───────┴────────┴──────┴──────┘ │
├─ PREVIEW (first 5 rows) ───────────────────┤
│  ┌─────────┬─────┬──────────┬───────────┐  │
│  │ name    │ age │ city     │    salary │  │
│  ├─────────┼─────┼──────────┼───────────┤  │
│  │ Alice   │  30 │ New York │ 75,000.50 │  │
│  │ Bob     │  25 │ SF       │ 82,000.00 │  │
│  │ Charlie │  35 │ Chicago  │ 68,000.75 │  │
│  └─────────┴─────┴──────────┴───────────┘  │
└────────────────────────────────────────────┘
```

### Section Details

- **Outer panel**: full-width box using `PanelBorder` color; title shows filename
- **Row/column count**: bold, on the line immediately below the top border
- **COLUMNS section**: each line shows `  <name>  <dtype>` — name in normal style, dtype in dim style
- **STATISTICS section**: nested styled table showing only numeric columns; columns are count, mean, std, min, max; values formatted with thousands separators
- **PREVIEW section**: first 5 rows rendered using `RenderTable` (without its own outer border since the dashboard provides one)
- Section separator lines: `├─ SECTION NAME ──...─┤` with `SectionHeader` color for the label text

### Function Signature

```go
func RenderSummary(df dataframe.DataFrame, filename string, th Theme, termWidth int) string
```

---

## Dispatch (`render.go`)

```go
type VizOptions struct {
    Type      string // "bar" | "histogram" | "line" | "table" | "summary"
    XCol      string
    YCol      string
    Bins      int
    ThemeMode string // "dark" | "light" | "" (auto)
    Filename  string // for dashboard title
}

// Render dispatches to the correct renderer.
// Returns plain df.String() if opts.Type is empty or stdout is not a TTY.
func Render(df dataframe.DataFrame, opts VizOptions, termWidth int, isTTY bool) string
```

The `read` command computes `termWidth` and `isTTY` from `golang.org/x/term` before calling `viz.Render`.

---

## Error Handling

All errors are returned from `runRead` via `error` and printed by cobra to stderr.

| Situation | Error message |
|---|---|
| Unknown `--viz` type | `unknown viz type "pie" (supported: bar, histogram, line, table, summary)` |
| `--x` column not found | `--x column "foo" not found (available: name, age, city, salary)` |
| `--y` column not found | `--y column "foo" not found (available: name, age, city, salary)` |
| `--y` column not numeric | `--y column "name" is not numeric (dtype: string)` |
| `--x` column not numeric for histogram | `--x column "city" is not numeric; histogram requires a numeric column` |
| `--x` required but not inferrable | `--viz line requires --x (no default column could be inferred)` |
| Not a TTY | Warning to stderr: `warning: --viz ignored (not a terminal)`; output falls back to plain table |

---

## Testing

### Unit Tests (`cmd/gopandas/viz/*_test.go`)

- **`theme_test.go`**: Detect() returns correct theme for each env combination
- **`table_test.go`**: border characters present, numeric right-aligned, string left-aligned, truncation works, TTY=false returns plain output
- **`chart_test.go`**:
  - Bar: longest label sets consistent padding, bar proportions correct for known values
  - Histogram: correct number of bins, 8-level block character selection correct
  - Line: Braille chars appear in output, x-axis label count matches bins
- **`summary_test.go`**: all four sections present, numeric-only columns in stats table, preview shows exactly 5 rows

### CLI Integration Tests (`cmd/gopandas/main_test.go`)

Extend existing integration tests with:
- `gopandas read testdata/sample.csv --viz table` → output contains `│` and `─`
- `gopandas read testdata/sample.csv --groupby city --agg mean --viz bar --x city --y salary` → output contains `█`
- `gopandas read testdata/sample.csv --viz summary` → output contains `COLUMNS`, `STATISTICS`, `PREVIEW`
- `gopandas read testdata/sample.csv --viz bar --x nonexistent` → exits non-zero, stderr contains `not found`

---

## Files Changed / Added

| File | Change |
|---|---|
| `cmd/gopandas/viz/theme.go` | New |
| `cmd/gopandas/viz/table.go` | New |
| `cmd/gopandas/viz/chart.go` | New |
| `cmd/gopandas/viz/summary.go` | New |
| `cmd/gopandas/viz/render.go` | New |
| `cmd/gopandas/viz/theme_test.go` | New |
| `cmd/gopandas/viz/table_test.go` | New |
| `cmd/gopandas/viz/chart_test.go` | New |
| `cmd/gopandas/viz/summary_test.go` | New |
| `cmd/gopandas/read.go` | Add `--viz`, `--x`, `--y`, `--bins`, `--theme` flags; call `viz.Render` |
| `go.mod` / `go.sum` | Add `charmbracelet/lipgloss`, `golang.org/x/term` |
| `README.md` | Document new `--viz` flags with examples |

---

## Explicitly Out of Scope (v1)

- Scatter plot
- Pie chart
- Interactive/TUI mode (arrow keys, zoom)
- Color palette customization beyond dark/light
- Pager support (no `less`-style scrolling)
- Export to image (PNG, SVG)
- Sparklines inline in table cells
