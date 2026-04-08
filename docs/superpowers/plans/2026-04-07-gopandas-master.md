# gopandas Implementation — Master Tracker

> This document tracks the overall state of the gopandas implementation.
> Each section corresponds to a separate plan document. Work through plans
> one at a time, using subagents, in the order shown.

**Spec:** `docs/superpowers/specs/2026-04-07-gopandas-design.md`

---

## Dependency Order

```
Layer 0 (foundation):  dtype, arrow adapter, index
Layer 1 (core):        series
Layer 2 (composite):   dataframe
Layer 3 (operations):  groupby, ops (merge/reshape/filter)
Layer 4 (io):          io (CSV, JSON, Parquet)
Layer 5 (cli):         cmd/gopandas
```

---

## Plan Status

| # | Package | Plan File | Status |
|---|---------|-----------|--------|
| 0a | `dtype` | `plans/2026-04-07-dtype.md` | DONE |
| 0b | `arrowutil` (adapter) | `plans/2026-04-07-arrow.md` | DONE |
| 0c | `index` | `plans/2026-04-07-index.md` | DONE |
| 1 | `series` | `plans/2026-04-07-series.md` | DONE |
| 2 | `dataframe` | `plans/2026-04-07-dataframe.md` | DONE |
| 3a | `groupby` | `plans/2026-04-07-groupby.md` | DONE |
| 3b | `ops` | `plans/2026-04-07-ops.md` | DONE |
| 4 | `io` | `plans/2026-04-07-io.md` | DONE |
| 5 | `cmd/gopandas` | `plans/2026-04-07-cli.md` | DONE |

### Status values
- `NOT STARTED` — plan written, not yet executed
- `IN PROGRESS` — subagent is working on it
- `DONE` — all tasks complete, tests pass, committed
- `BLOCKED` — waiting on dependency

---

## Module Init (do once before any plan)

```bash
cd /Users/vinaychitepu/Code/gopandas
go mod init github.com/vinaychitepu/gopandas
git init
git add go.mod
git commit -m "chore: init go module"
```

---

## Notes

- Each plan is self-contained: a subagent reads only that plan file + the spec.
- After completing each plan, update this file's status table.
- Plans at the same layer can be executed in parallel if desired.
- Layer N+1 depends on Layer N being DONE.
