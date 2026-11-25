# General Implementation Guide — Databricks Report Pipelines

Standard patterns and conventions for building report pipelines in this repository.

---

## Repository Structure

```
<ReportName>/
  LoadData/                    # Notebooks that load source data into delivery delta tables (if applicable)
  Export/                      # Notebooks that export data to files (if applicable)
  MasterJob/                   # Orchestration notebooks (if multiple steps needed)
  config/
    live/                      # JSON configs for active reports (if using config-driven approach)
    deprecated/                # JSON configs for inactive reports

01-utils/                      # Shared Python utilities (helpers, export functions)
aws_utils/                     # AWS-specific utilities (SSM, S3, credentials)
DatabricksWorkflows/           # Databricks job YAML definitions
Documentation_Instructions/    # Documentation files
```

Not all reports require all folders. Use only what is needed:
- A simple report may only need a single notebook and a YAML
- A complex pipeline may need LoadData, Export, MasterJob, and config folders

---

## YAML Job Definitions

All Databricks job definitions go under `DatabricksWorkflows/`.

Conventions:
- One YAML per job
- File name should match the job name
- All tasks must have `max_retries: 0` (fail immediately, no retry)
- Use `git_source` pointing to the repo and branch
- Use `source: GIT` in notebook tasks
- Use Unity Catalog-enabled clusters (set `existing_cluster_id` to a Unity Catalog cluster, or use serverless compute)

---

## JSON Config Files (Optional)

If your report runs for multiple portfolios or entities with different parameters, use JSON config files under `<ReportName>/config/live/` and `config/deprecated/`.

- One JSON file per portfolio/entity
- Filename must be **lowercase**
- Active configs go in `live/`, inactive in `deprecated/`
- Moving a config from `live/` to `deprecated/` deactivates it without deleting

This approach is optional. If your report has fixed parameters or runs for a single entity, you can pass parameters directly via job parameters.

---

## Notebook Conventions

### Naming
- All notebook filenames should be **lowercase** when they are called dynamically via `dbutils.notebook.run()` (case-sensitive)
- Use descriptive names that match the report or table they produce

### Parameters
Every notebook should accept parameters via `dbutils.widgets`. Only define the parameters your notebook actually needs:

```python
dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
```

### Import Paths
Use absolute path resolution with fallback to support both Git source jobs and Workspace Repos:

```python
import os
import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/<ReportName>/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("<relative_path>/aws_utils")
    sys.path.append("<relative_path>/01-utils")
```

Replace `<ReportName>` with the actual report folder name.

The fallback `<relative_path>` depends on where your notebook is located relative to the repo root:
- Notebook at `ReportName/SubFolder/notebook.py` (2 levels deep) → `../../`
- Notebook at `ReportName/notebook.py` (1 level deep) → `../`
- Notebook at repo root → no prefix needed

Adjust the number of `../` based on your notebook's depth in the folder structure.

---

## Shared Utilities

### When to use
- Before writing a new function, check if a similar one already exists in `01-utils/helpers.py` or `aws_utils/aws_utils.py`
- If your function is specific to one report, keep it in the report notebook
- If your function could be reused by other reports, add it to the appropriate utils module

### Where to put new functions
- **Business logic / data operations** → `01-utils/helpers.py`
- **AWS / infrastructure operations** → `aws_utils/aws_utils.py`
- **Report-specific export utilities** → `01-utils/export_utils.py`


> ⚠️ **Important:** Always check available functions before implementing a new one. Be very careful when altering an existing function — other reports may depend on it.

---

## run_date Logic

Implemented in `01-utils/helpers.py` via `get_validated_eom_run_date()`:

- If `run_date` parameter is **provided**: use the given value (validates format YYYY-MM-DD)
- If `run_date` parameter is **empty**: automatically calculate you run_date logic in your notebook

---

## Portfolio Filter

Implemented in `01-utils/helpers.py` via `build_portfolio_filter()`:

- Accepts comma-separated portfolio names (e.g., `"cosmos,cetus"`)
- Uses **ILIKE** matching — `"cosmos"` matches both `cosmosretail` and `cosmoswholesale`
- Empty value means all portfolios (no filter applied)
- Returns a SQL WHERE clause fragment using `PortfolioName`

Usage:
```python
from helpers import build_portfolio_filter
portfolio_filter, portfolio_param, portfolio_display = build_portfolio_filter(dbutils.widgets.get("portfolio"))
```

In SQL queries:
```python
spark.sql(f"SELECT * FROM table WHERE {portfolio_filter}")
```

If a query has ambiguous `PortfolioName` (multiple tables with that column), qualify it:
```python
f"WHERE {portfolio_filter.replace('PortfolioName', 'alias.PortfolioName')}"
```

---

## Calling Other Notebooks (Optional)

If your pipeline calls child notebooks via `dbutils.notebook.run()`:
- Ensure child notebook filenames are **lowercase**
- Pass parameters explicitly via the `arguments` dict
- Set an appropriate timeout
- The child notebook inherits the working directory of the parent

Example:
```python
dbutils.notebook.run("child_notebook_name", timeout_seconds=3600, arguments={
    "run_date": run_date,
    "env_prefix": env_prefix,
})
```

---

## Exporting Files (Optional)

If your report exports files (Excel, CSV, etc.):
- Use `01-utils/export_utils.py` for common export functions (e.g., `export_to_excel`)
- Create the output directory (lowercase) before writing: `os.makedirs(output_dir, exist_ok=True)`
- Build the output path by combining the SSM base path with the relative path from config
- Use a date-based subfolder (format `YYYYMMDD`) for each run

---

## Email Integration (Optional)

If your report needs email notifications after completion:
- Use `send_report_email()` from helpers.py
- The email job (`Email_report_job`) is looked up by name at runtime via Databricks SDK — no hardcoded job IDs
- Email parameters (recipients, template) come from the JSON config or can be passed directly

---

## Master Orchestrator Pattern (Optional)re

For pipelines with multiple steps (e.g., LoadData → Export):

1. Create a `master_orchestrator.py` notebook that calls child jobs by name via Databricks SDK
2. Optionally use `check_run_eligibility.py` as a gate to control when the pipeline runs (e.g., only on Nth business day). This notebook should be under your ReportName/MasterJob/check_run_eligibility.py file, explicity for each report 

Alternatively, define the DAG directly in a YAML file using `run_job_task` with job IDs for visual DAG representation in the Databricks UI.

---

## Type Safety

Delta tables with ANSI mode (serverless clusters) require explicit type matching:

- Use `CAST(column AS STRING)` or `.withColumn("col", col("col").cast("string"))` before writing
- Use `NULLIF(divisor, 0)` for any division to prevent division by zero
- No implicit type conversions — a DOUBLE cannot be written to a STRING column without explicit CAST
- Always verify your DataFrame schema matches the target table schema before writing

---

## SSM Parameters

Common SSM parameters used across reports:

| SSM Key | Description |
|---------|-------------|
| `/cfn/bucket_prefix` | Environment prefix (d/q/p) |
| `/reports/exports/s3_main_path` | Base export path with `envprefix` placeholder |

If your report needs additional SSM parameters, document them in the report-specific documentation.

---

## Deployment Checklist

1. Create delta tables in target environment with correct schema and partitioning
2. Verify SSM parameters exist in the target environment
3. Create child jobs first, then master job
4. Update `git_branch` in YAML files to target branch
5. Set schedule `pause_status` to `UNPAUSED` when ready
6. Test adhoc before enabling schedule

> ⚠️ **Pull Request Reminder:** If your YAML files use `run_job_task` with hardcoded `job_id` values (e.g., Master DAG calling LoadData/Export), you must mention in the PR description that job IDs need to be updated per environment after deployment. Job IDs differ between dev, QA, and production — they cannot be committed as-is.
