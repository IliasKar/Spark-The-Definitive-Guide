# Databricks notebook source
# Reads portfolio configs from config/live/*.json and outputs them as a JSON array
# for use by a for_each_task downstream.

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("portfolio", "")  # comma-separated filter, empty = all
dbutils.widgets.text("config_folder", "live")

import os
import sys
import json

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/DetailedPortfolio/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname
from helpers import load_configs

# COMMAND ----------

env_name = get_env_fullname()
env_prefix = get_env_prefix()

run_date_raw = dbutils.widgets.get("run_date").strip()
if run_date_raw:
    run_date = run_date_raw
else:
    run_date = str(spark.sql(f"SELECT MAX(BusinessDate) FROM {env_name}.derivative.dexposure").collect()[0][0])
config_folder = dbutils.widgets.get("config_folder").strip() or "live"
portfolio_raw = dbutils.widgets.get("portfolio").strip()
filter_set = {p.strip().lower() for p in portfolio_raw.split(",") if p.strip()} if portfolio_raw else None

# COMMAND ----------

configs = load_configs(dbutils, config_folder, filter_set, report_name="DetailedPortfolio")
print(f"Loaded {len(configs)} portfolio config(s) from '{config_folder}'")

# Build the iteration list: each item has everything the report notebook needs
iterations = []
for config in configs:
    report_args = config["report_args"].copy()
    portfolio_name = report_args["portfolio_name"]
    iterations.append({
        "report_args": json.dumps(report_args),
        "report_name": f"{portfolio_name}_DetailedPortfolio",
        "run_date": run_date,
        "env_prefix": env_prefix,
    })

print(f"Portfolios to run: {[item['report_name'] for item in iterations]}")

# COMMAND ----------

# Output as task value for for_each_task
dbutils.jobs.taskValues.set(key="portfolio_configs", value=json.dumps(iterations))
dbutils.notebook.exit(json.dumps(iterations))
