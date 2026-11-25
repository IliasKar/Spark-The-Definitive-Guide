# Databricks notebook source
# MAGIC %md
# MAGIC ## Log Area Completion (generic)
# MAGIC
# MAGIC Final task of any pipeline job. Writes a single 'completed' record for the
# MAGIC given `pipeline_area` into `pipeline.area_runs`.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `pipeline_area` (required): the area to mark as completed (e.g.
# MAGIC   `derivative`, `ptools_core`, `biteam`, `exports`)
# MAGIC - `business_date` (optional): if empty, resolved from task_runs (option B),
# MAGIC   or from `business_date_based_on_area` when that is supplied.
# MAGIC   Required when `skip_task_runs=true` and no `business_date_based_on_area` given.
# MAGIC - `skip_task_runs` (optional, default `false`): if `true`, writes a simple
# MAGIC   completed record without reading task_runs (no derived start/end/duration).
# MAGIC   Use for areas that have no per-table task logging (reports, exports, adhoc).
# MAGIC - `business_date_based_on_area` (optional, default empty): when set and no
# MAGIC   explicit `business_date` is given, the business_date is resolved from
# MAGIC   `pipeline.v_area_latest_status` for that source area, i.e.
# MAGIC   `SELECT business_date FROM pipeline.v_area_latest_status WHERE pipeline_area = <value>`.
# MAGIC   Intended for reports whose own area has no task_runs but that align with
# MAGIC   another area's business_date (e.g. `ptools_thirdparties`).
# MAGIC
# MAGIC **Two modes:**
# MAGIC - Normal (skip_task_runs=false): start/end/duration derived from task_runs
# MAGIC   events (the per-table controller already wrote). For derivative, stg-lng-lnd, etc.
# MAGIC - Adhoc (skip_task_runs=true): simple stamp — just marks the area completed
# MAGIC   with current_timestamp. For reports, exports, or any area without per-table logging.
# MAGIC   The business_date can be sourced from another area via `business_date_based_on_area`.

# COMMAND ----------

dbutils.widgets.text("pipeline_area", "", "Pipeline Area (e.g. derivative, ptools_core, biteam, exports)")
dbutils.widgets.text("business_date", "", "Business Date (YYYY-MM-DD, empty=resolve from task_runs)")
dbutils.widgets.text("skip_task_runs", "false", "Skip task_runs lookup (true/false, default false)")
dbutils.widgets.text("business_date_based_on_area", "", "Source area to resolve business_date from v_area_latest_status (optional)")

# COMMAND ----------

import sys
# Paths from repo root (jobs run with cwd = repo root, like controller)
sys.path.append("./aws_utils")
sys.path.append("./01-utils/pipeline_logging")
# Fallbacks relative to this notebook's own folder
sys.path.append("../../aws_utils")
sys.path.append(".")

import aws_utils

# Get Environment Prefix
env = aws_utils.get_env_prefix()

pipeline_area = dbutils.widgets.get("pipeline_area").strip()
business_date_param = dbutils.widgets.get("business_date").strip() or None
skip_task_runs = dbutils.widgets.get("skip_task_runs").strip().lower() == "true"
business_date_based_on_area = dbutils.widgets.get("business_date_based_on_area").strip() or None

if not pipeline_area:
    raise ValueError("pipeline_area parameter is required (e.g. derivative, ptools_core, biteam, exports)")

print(f"env_prefix: {env}")
print(f"pipeline_area: {pipeline_area}")
print(f"business_date: {business_date_param or '(resolve)'}")
print(f"skip_task_runs: {skip_task_runs}")
print(f"business_date_based_on_area: {business_date_based_on_area or '(none)'}")

# COMMAND ----------

# Write the area completion record.
try:
    from pipeline_log_helpers import log_area_completed
    log_area_completed(
        spark,
        pipeline_area,
        business_date=business_date_param,
        env_prefix=env,
        skip_task_runs=skip_task_runs,
        business_date_based_on_area=business_date_based_on_area,
    )
except Exception as e:
    # Non-blocking: logging must never fail the job.
    print(f"[WARNING] log_area_completed failed for area '{pipeline_area}' (non-blocking): {e}")
