---
inclusion: fileMatch
fileMatchPattern: "dportfoliotoolthirdparties/**"
---

# DPortfolioToolThirdParties - Architecture & Operations

## Overview

The DPortfolioToolThirdParties pipeline calculates daily and monthly WorkableFlag/NonWorkableReason for loan exposures across portfolios (Mercury, Light, Orbit, Cell, Pearl Portfolio A).

## Components

### 1. DPortfolioToolThirdParties.py (Daily notebook)
- Runs **daily** with `run_date` = previous business date
- Writes daily WorkableFlag data to `derivativeactive.DPortfolioToolThirdParties`
- At the end, checks if the **Materialized View** needs refresh

### 2. Materialized View: `derivativeactive.dportfoliotoolthirdpartiesmonthly`
- Pre-computes monthly WorkableFlag aggregation for all completed months
- Replaces the old `DPortfolioToolThirdPartiesMonthly` notebook + table approach
- Read by external tools that cannot call functions directly

### 3. Function: `LastMonthEOM_WorkableFlagMonthly_NonWorkableReasonMonthly` (DEPRECATED)
- Original table-valued function, now replaced by the Materialized View
- Kept in repo for historical reference only
- Not called by any active notebook

## Why Materialized View?

- External tools can only read **tables/views**, not functions
- A regular view was too slow (~7 min per query) because it computes all months on-the-fly
- The MV stores pre-computed results → instant reads
- Tradeoff: refresh is expensive (full recomputation) but only needed at month-end or re-runs

## MV Refresh Logic (in DPortfolioToolThirdParties.py)

The MV refreshes **only when needed**:

| Scenario | Refresh? | Why |
|---|---|---|
| Daily run, month not closed yet | No | MV not affected |
| Daily run, run_date = last working day | Yes | New month closing |
| Re-run for any day in a closed month | Yes | Source data changed for a month already in MV |
| Re-run for a day in current (open) month | No | MV doesn't include current month |

### Logic:
```python
is_last_working_day = (run_date == CurrentMonthLastWorkingDate from ref_calendar)
mv_has_month = (MV already has data for run_date's month)

if is_last_working_day or mv_has_month:
    REFRESH MATERIALIZED VIEW
```

## Important Notes

- **Re-runs:** If you re-run the daily for a closed month, the MV will refresh on every execution. This is expected but costly (~7 min per refresh). Plan re-runs accordingly.
- **Order doesn't matter:** Whether you re-run Jan 31 first or Jan 15 first, the MV will refresh each time and always reflect the latest source data.
- **Incomplete re-runs:** If you re-run only some days of a month (e.g., only Jan 31 but not Jan 1-30), the MV will refresh with incomplete data. Always re-run the full month if correcting data.
- **DPortfolioToolThirdPartiesMonthly.ipynb:** This notebook is now **deprecated**. The MV replaces its functionality entirely.
- **ref_calendar dependency:** The MV uses `ref_calendar.CurrentMonthLastWorkingDate` to determine month boundaries. Ensure this table is up-to-date.
