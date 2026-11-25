# Databricks notebook source
# Reads portfolio configs from MegaDataTape/config/live/*.json and outputs them
# as a JSON array for use by a for_each_task downstream.
#
# Each item in the output contains all parameters needed by the individual
# export notebook (rhodium.py, domus.py, etc.)

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("portfolio", "")  # comma-separated filter, empty = all
dbutils.widgets.text("portfolio_exact_match", "false")
dbutils.widgets.text("config_folder", "live")  # live, deprecated, or all
dbutils.widgets.text("skip_email", "false")

import os
import sys
import json
import boto3

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname, connect_to_aws_and_access_ssm, get_scope_key_names, get_aws_credentials
from helpers import get_validated_eom_run_date, load_configs

# COMMAND ----------

env_name = get_env_fullname()
env_prefix = get_env_prefix()

run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
print(f"run_date: {run_date} | env: {env_prefix}")

# ── SSM: resolve export base path ──
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

# COMMAND ----------

config_folder = dbutils.widgets.get("config_folder").strip() or "live"
portfolio_raw = dbutils.widgets.get("portfolio").strip()
portfolio_exact_match = dbutils.widgets.get("portfolio_exact_match").strip().lower() == "true"
skip_email = dbutils.widgets.get("skip_email").strip().lower() == "true"

if portfolio_exact_match and portfolio_raw:
    filter_set = {p.strip().lower() for p in portfolio_raw.split(",") if p.strip()}
else:
    filter_set = {p.strip().lower() for p in portfolio_raw.split(",") if p.strip()} if portfolio_raw else None

configs = load_configs(dbutils, config_folder, filter_set, report_name="MegaDataTape")

# Apply exact match filtering if needed (load_configs uses substring match by default)
if portfolio_exact_match and filter_set:
    configs = [c for c in configs if c["_key"].lower() in filter_set]

print(f"Loaded {len(configs)} portfolio config(s) from '{config_folder}'")

# COMMAND ----------

# Build the iteration list: each item has everything the export notebook needs
run_date_str = run_date.replace("-", "")
iterations = []

for config in configs:
    report_args = config["report_args"]
    portfolio_name = report_args["portfolio_name"]
    output_file_prefix = report_args["output_file_prefix"]
    report_output = report_args["report_output"]
    output_dir = EXPORT_BASE_PATH + report_output + f"{run_date_str}/"

    iterations.append({
        "notebook_name": portfolio_name.lower(),
        "portfolio_name": portfolio_name,
        "last_eom_business_date": run_date,
        "env_prefix": env_prefix,
        "output_dir": output_dir,
        "output_file_prefix": output_file_prefix,
        "report_output": report_output,
        "zip_folder_prefix": report_args.get("zip_folder_prefix", ""),
        "skip_email": str(skip_email).lower(),
        "email_args": json.dumps(config.get("email_args", {})),
        "export_base_path": EXPORT_BASE_PATH,
        "unc_base_path": UNC_BASE_PATH,
    })

print(f"Portfolios to run: {[item['portfolio_name'] for item in iterations]}")

# COMMAND ----------

# Output as task value for for_each_task
dbutils.jobs.taskValues.set(key="portfolio_configs", value=json.dumps(iterations))
dbutils.notebook.exit(json.dumps(iterations))
