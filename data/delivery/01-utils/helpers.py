from datetime import datetime
from pytz import timezone
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Run date helpers (Spark SQL — pushes all filtering to the engine)
# ---------------------------------------------------------------------------

def is_last_business_day_of_month(spark: SparkSession, env_name: str, input_date: str) -> bool:
    """Check if the given date is the last business day of its month."""
    row = spark.sql(f"""
        SELECT MAX(CalendarDate) AS LastBusinessDate
        FROM {env_name}.reference.ref_calendar
        WHERE CalendarDate BETWEEN DATE_TRUNC('MONTH', DATE('{input_date}'))
                               AND LAST_DAY(DATE('{input_date}'))
          AND IsBusinessDay = 1
    """).first()
    return str(row["LastBusinessDate"]) == input_date if row and row["LastBusinessDate"] else False


def get_last_business_date_of_previous_month(spark: SparkSession, env_name: str, input_date: str) -> str:
    """Get the last business day of the previous month relative to input_date. Returns 'YYYY-MM-DD' string."""
    row = spark.sql(f"""
        SELECT MAX(CalendarDate) AS LastBusinessDate
        FROM {env_name}.reference.ref_calendar
        WHERE CalendarDate < DATE_TRUNC('MONTH', DATE('{input_date}'))
          AND IsBusinessDay = 1
    """).first()
    return str(row["LastBusinessDate"]) if row and row["LastBusinessDate"] else None



def get_validated_eom_run_date(spark: SparkSession, env_name: str, run_date_raw: str) -> str:
    """
    Validate and return an end-of-month business date.
    If run_date_raw is provided, validates format and checks it's a last business day of month.
    If empty, defaults to last business day of previous month.
    Always returns a string 'YYYY-MM-DD'.
    """
    if run_date_raw and run_date_raw.strip():
        try:
            parsed = datetime.strptime(run_date_raw.strip(), '%Y-%m-%d').date()
        except ValueError:
            raise ValueError(f"Invalid run_date format: '{run_date_raw}'. Expected YYYY-MM-DD.")
        run_date = str(parsed)
        if not is_last_business_day_of_month(spark, env_name, run_date):
            raise ValueError(
                f"Adhoc run_date '{run_date}' is not the last business day of the month. "
                "Reports should only run for EOM dates."
            )
    else:
        today = datetime.now(timezone('Europe/Athens')).strftime('%Y-%m-%d')
        run_date = get_last_business_date_of_previous_month(spark, env_name, today)
        if not run_date:
            raise ValueError("Could not determine last business day of previous month.")

    return run_date


# ---------------------------------------------------------------------------
# Portfolio filter (pure Python — no Spark needed)
# ---------------------------------------------------------------------------

def build_portfolio_filter(portfolio_raw: str, portfolio_exact_match: bool = False):
    """
    Build SQL filter clause and display values from a raw portfolio parameter.

    Args:
        portfolio_raw: comma-separated portfolio names (e.g. "domus,monza") or empty for all.
        portfolio_exact_match: if True, uses exact match (=). If False (default), uses ILIKE (%name%).

    Returns:
        tuple: (portfolio_filter, portfolio_param, portfolio_display)
            - portfolio_filter: SQL WHERE clause
            - portfolio_param: comma-separated string or "%" for all
            - portfolio_display: human-readable string for logging
    """
    portfolio_raw = (portfolio_raw or "").strip()
    portfolio_list = [p.strip() for p in portfolio_raw.split(",") if p.strip()] if portfolio_raw else []

    if portfolio_list:
        if portfolio_exact_match:
            conditions = " OR ".join(f"lower(PortfolioName) = lower('{p}')" for p in portfolio_list)
        else:
            conditions = " OR ".join(f"lower(PortfolioName) ILIKE lower('%{p}%')" for p in portfolio_list)
        portfolio_filter = f"({conditions})"
        portfolio_param = ",".join(portfolio_list)
    else:
        portfolio_filter = "1=1"
        portfolio_param = "%"

    portfolio_display = portfolio_param if portfolio_param != "%" else "ALL"
    return portfolio_filter, portfolio_param, portfolio_display


# ---------------------------------------------------------------------------
# Delta table operations (Spark SQL — only option)
# ---------------------------------------------------------------------------

def delete_from_unity_table(
    spark: SparkSession,
    table_path: str,
    run_date: str,
    portfolio_filter: str,
) -> None:
    """Delete rows from a Unity Catalog Delta table for a given business date and portfolio filter."""
    spark.sql(f"""
        DELETE FROM {table_path}
        WHERE ReportingDate = {run_date!r}
        AND {portfolio_filter}
    """)


# ---------------------------------------------------------------------------
# Config loading (JSON files)
# ---------------------------------------------------------------------------

def load_configs(dbutils, config_folder="live", portfolio_filter=None, report_name=None):
    """
    Load portfolio JSON configs from <ReportName>/config/<folder>/.
    config_folder: 'live', 'deprecated', or 'all'.
    portfolio_filter: set of lowercase portfolio names, or None for all.
    report_name: folder name of the report (e.g., 'MegaDataTape'). Auto-detected from notebook path if not provided.
    """
    import os, json
    _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    if report_name:
        _repo_root = _notebook_path.split(f"/{report_name}/")[0]
        config_base = f"/Workspace{_repo_root}/{report_name}/config"
    else:
        # Auto-detect: assume notebook is under <ReportName>/SomeFolder/notebook
        parts = _notebook_path.split("/")
        # Find the report folder (first folder after repo root that contains the notebook)
        _repo_root = "/".join(parts[:-3])  # go up 2 levels from notebook
        report_folder = parts[-3]
        config_base = f"/Workspace{_repo_root}/{report_folder}/config"

    folders = ["live", "deprecated"] if config_folder == "all" else [config_folder]

    configs = []
    for folder in folders:
        config_dir = os.path.join(config_base, folder)
        if not os.path.exists(config_dir):
            print(f"Warning: config folder '{config_dir}' not found, skipping.")
            continue
        for filename in sorted(os.listdir(config_dir)):
            if not filename.endswith(".json"):
                continue
            key = filename.replace(".json", "")
            if portfolio_filter and not any(f in key.lower() for f in portfolio_filter):
                continue
            with open(os.path.join(config_dir, filename)) as f:
                config = json.load(f)
            config["_key"] = key
            config["_folder"] = folder
            configs.append(config)
    return configs


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def archive_existing_files(output_dir):
    """Move existing files in output_dir to a previous_runs/ subfolder.
    Uses the timestamp from the first filename if possible, otherwise today's date."""
    import os
    import shutil
    from datetime import datetime
    from pytz import timezone
    import re as _re

    existing_files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))]
    if not existing_files:
        return 0

    match = _re.search(r'_(\d{8})T\d{6}\.\w+$', existing_files[0])
    export_date = match.group(1) if match else datetime.now(timezone("Europe/Athens")).strftime("%Y%m%d")
    prev_dir = os.path.join(output_dir, "previous_runs", export_date)
    os.makedirs(prev_dir, exist_ok=True)
    for f in existing_files:
        shutil.move(os.path.join(output_dir, f), os.path.join(prev_dir, f))
    print(f"  ↳ Moved {len(existing_files)} existing file(s) to previous_runs/{export_date}/")
    return len(existing_files)


def run_portfolio_export(dbutils, report_args, run_date, export_base_path, env_prefix):
    """Run the export notebook for a single portfolio.
    If output_dir already has files, moves them to previous_runs/ subfolder before exporting."""
    import os

    portfolio_name = report_args["portfolio_name"]
    output_dir = export_base_path + report_args["report_output"] + f"{run_date.replace('-', '')}/"
    os.makedirs(output_dir, exist_ok=True)

    archive_existing_files(output_dir)

    output_file_prefix = report_args["output_file_prefix"]

    print(f"  → {portfolio_name} | output: {output_dir}")
    dbutils.notebook.run(portfolio_name.lower(), 3600, arguments={
        "last_eom_business_date": str(run_date),
        "env_prefix": env_prefix,
        "output_dir": output_dir,
        "output_file_prefix": output_file_prefix,
    })


def send_report_email(report_args, email_args, run_date, export_base_path, email_job_name, log_table, body_template, unc_base_path=""):
    """Trigger the Email job by name via Databricks SDK."""
    import json
    from databricks.sdk import WorkspaceClient

    portfolio_name = report_args["portfolio_name"]
    output_file_prefix = report_args.get("output_file_prefix", portfolio_name)
    email_report_path = unc_base_path.rstrip("\\") + "\\" + report_args.get("report_output", "").replace("/", "\\") + run_date.replace("-", "")

    body_data = json.dumps({
        "report_name": output_file_prefix.rstrip("_"),
        "run_date": run_date,
        "report_location_info": email_report_path,
    })

    notebook_params = {
        "recipients_entra_group": email_args.get("recipients_entra_group", ""),
        "email_subject": f"{output_file_prefix.rstrip('_')} Report",
        "body_data_json": body_data,
        "log_table": log_table,
        "body_template": body_template,
        "recipients_to": email_args.get("recipients_to", ""),
        "recipients_cc": email_args.get("recipients_cc", ""),
    }

    w = WorkspaceClient()
    job = next(w.jobs.list(name=email_job_name))
    run = w.jobs.run_now(job_id=job.job_id, notebook_params=notebook_params)
    print(f"  ✓ Email triggered (run_id: {run.run_id})")
