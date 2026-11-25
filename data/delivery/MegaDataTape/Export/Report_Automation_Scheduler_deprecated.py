# Databricks notebook source
# Runs MegaDataTape exports and triggers email notifications.
# - If report_args is empty: runs all live portfolios from config/live/*.json
# - If report_args is provided: runs only that portfolio (single-portfolio mode)
# - portfolio param: comma-separated filter (only used when report_args is empty)

dbutils.widgets.text("report_args", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("report_name", "")
dbutils.widgets.text("portfolio", "")  # comma-separated filter, empty = all
dbutils.widgets.text("config_folder", "live")  # live, deprecated, or all
dbutils.widgets.text("skip_email", "false")  # true to skip email notifications

import os
import json
import boto3

import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname, connect_to_aws_and_access_ssm, get_scope_key_names, get_aws_credentials
from helpers import get_validated_eom_run_date, load_configs, run_portfolio_export, send_report_email

# COMMAND ----------

env_name = get_env_fullname()
env_prefix = get_env_prefix()

run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
print(f"run_date: {run_date} | env: {env_prefix}")

# ── SSM: export base path ──
secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)
aws_key = aws_keys[0] if aws_keys else None
aws_secret_key = get_aws_credentials(secrets_scope, aws_key)
ssm_client = boto3.client("ssm", region_name="eu-central-1",
                          aws_access_key_id=aws_key,
                          aws_secret_access_key=aws_secret_key)

EXPORT_BASE_PATH = connect_to_aws_and_access_ssm(
    "/reports/exports/s3_main_path", ssm_client, aws_secret_key
).replace("envprefix", env_prefix)

UNC_BASE_PATH = connect_to_aws_and_access_ssm(
    "/reports/exports/unc_main_path", ssm_client, aws_secret_key
)

EMAIL_JOB_NAME = "Email_report_job"
LOG_TABLE = f"`{env_prefix}-cdh-delivery`.report_catalog_email_logs.email_logs"
BODY_TEMPLATE = f"/Volumes/{env_prefix}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html"

# COMMAND ----------

# COMMAND ----------

# Functions moved to 01-utils/helpers.py: load_configs, run_portfolio_export, send_report_email

# COMMAND ----------

report_args_raw = dbutils.widgets.get("report_args").strip()
portfolio_raw = dbutils.widgets.get("portfolio").strip()

if report_args_raw:
    # ── Single portfolio mode (called externally with report_args) ──
    report_args = json.loads(report_args_raw.replace("'", '"'))
    run_portfolio_export(dbutils, report_args, run_date, EXPORT_BASE_PATH, env_prefix)
    print(f"Notebook {report_args['portfolio_name'].lower()} has been executed.")
else:
    # ── All portfolios mode ──
    config_folder = dbutils.widgets.get("config_folder").strip() or "live"
    skip_email = dbutils.widgets.get("skip_email").strip().lower() == "true"
    filter_set = {p.strip().lower() for p in portfolio_raw.split(",") if p.strip()} if portfolio_raw else None
    configs = load_configs(dbutils, config_folder, filter_set, report_name="MegaDataTape")
    print(f"Running {len(configs)} portfolio(s) from '{config_folder}': {[c['_key'] for c in configs]}")

    results = []
    for config in configs:
        portfolio_name = config["report_args"]["portfolio_name"]
        print(f"\n▶ {portfolio_name}...")
        try:
            run_portfolio_export(dbutils, config["report_args"], run_date, EXPORT_BASE_PATH, env_prefix)
            print(f"  ✓ Export done")
        except Exception as e:
            results.append((portfolio_name, "EXPORT_FAILED", str(e)))
            print(f"  ✗ Export failed: {e}")
            continue
        try:
            if skip_email:
                print(f"  ⏭ Email skipped")
                results.append((portfolio_name, "OK", ""))
            else:
                send_report_email(config["report_args"], config.get("email_args", {}), run_date, EXPORT_BASE_PATH, EMAIL_JOB_NAME, LOG_TABLE, BODY_TEMPLATE, UNC_BASE_PATH)
                results.append((portfolio_name, "OK", ""))
        except Exception as e:
            results.append((portfolio_name, "EMAIL_FAILED", str(e)))
            print(f"  ✗ Email failed: {e}")

    failed = [r for r in results if r[1] != "OK"]
    print(f"\n{'='*50}")
    print(f"Total: {len(results)} | OK: {len(results)-len(failed)} | Failed: {len(failed)}")
    for name, status, detail in failed:
        print(f"  {status}: {name} -> {detail}")
    if failed:
        raise Exception(f"{len(failed)} portfolio(s) failed.")
