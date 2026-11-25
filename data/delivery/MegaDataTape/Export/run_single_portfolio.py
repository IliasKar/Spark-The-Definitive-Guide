# Databricks notebook source
# Wrapper for for_each_task: runs a single portfolio export notebook
# and optionally sends the email notification.
#
# Called by the for_each_task with parameters from load_portfolio_configs.

dbutils.widgets.text("notebook_name", "")
dbutils.widgets.text("portfolio_name", "")
dbutils.widgets.text("last_eom_business_date", "")
dbutils.widgets.text("env_prefix", "")
dbutils.widgets.text("output_dir", "")
dbutils.widgets.text("output_file_prefix", "")
dbutils.widgets.text("report_output", "")
dbutils.widgets.text("zip_folder_prefix", "")
dbutils.widgets.text("skip_email", "false")
dbutils.widgets.text("email_args", "{}")
dbutils.widgets.text("export_base_path", "")
dbutils.widgets.text("unc_base_path", "")

import os
import sys
import json

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../01-utils")

from helpers import archive_existing_files, send_report_email

# COMMAND ----------

notebook_name = dbutils.widgets.get("notebook_name")
portfolio_name = dbutils.widgets.get("portfolio_name")
last_eom_business_date = dbutils.widgets.get("last_eom_business_date")
env_prefix = dbutils.widgets.get("env_prefix")
output_dir = dbutils.widgets.get("output_dir")
output_file_prefix = dbutils.widgets.get("output_file_prefix")
report_output = dbutils.widgets.get("report_output")
zip_folder_prefix = dbutils.widgets.get("zip_folder_prefix")
skip_email = dbutils.widgets.get("skip_email").lower() == "true"
email_args = json.loads(dbutils.widgets.get("email_args"))
export_base_path = dbutils.widgets.get("export_base_path")
unc_base_path = dbutils.widgets.get("unc_base_path")

print(f"▶ {portfolio_name} | notebook: {notebook_name}")
print(f"  output_dir: {output_dir}")
print(f"  run_date: {last_eom_business_date}")

# COMMAND ----------

# Archive existing files before export
os.makedirs(output_dir, exist_ok=True)
archive_existing_files(output_dir)

# COMMAND ----------

# Run the portfolio export notebook
dbutils.notebook.run(notebook_name, 3600, arguments={
    "last_eom_business_date": last_eom_business_date,
    "env_prefix": env_prefix,
    "output_dir": output_dir,
    "output_file_prefix": output_file_prefix,
})
print(f"  ✓ Export done: {portfolio_name}")

# COMMAND ----------

# ZIP exported files if zip_folder_prefix is configured
if zip_folder_prefix:
    import zipfile
    import re
    import tempfile
    import shutil

    run_date_str = re.sub(r"-", "", last_eom_business_date)
    zip_filename = f"{zip_folder_prefix}_{run_date_str}.zip"
    # Place zip one level up (e.g., .../galaxyii/ instead of .../galaxyii/20260731/)
    parent_dir = os.path.dirname(output_dir.rstrip("/"))
    zip_final_path = os.path.join(parent_dir, zip_filename)

    # Zip only the actual export files in the date folder (skip subfolders like previous_runs)
    exported_files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))]

    # Write zip to local temp dir first (Volumes don't support random-access writes)
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_zip_path = os.path.join(tmp_dir, zip_filename)
        with zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in exported_files:
                zf.write(os.path.join(output_dir, f), arcname=f)
        # Replace only the zip with this exact name (leaves other zips in the parent untouched)
        if os.path.exists(zip_final_path):
            os.remove(zip_final_path)
        shutil.copy2(tmp_zip_path, zip_final_path)

    # Original files are intentionally kept in the date folder, exactly like
    # non-zipped portfolios (archiving / previous_runs behaves as before).
    print(f"  ✓ Zipped {len(exported_files)} file(s) into: {zip_filename} (originals kept in {run_date_str}/)")
else:
    print(f"  ⏭ No zip_folder_prefix configured, skipping ZIP")

# COMMAND ----------

# Send email notification
if skip_email:
    print(f"  ⏭ Email skipped")
else:
    EMAIL_JOB_NAME = "Email_report_job"
    LOG_TABLE = f"`{env_prefix}-cdh-delivery`.report_catalog_email_logs.email_logs"
    BODY_TEMPLATE = f"/Volumes/{env_prefix}-cdh/reference/reportscatalog/email_templates/report_success_template_email.html"

    report_args = {
        "portfolio_name": portfolio_name,
        "output_file_prefix": output_file_prefix,
        "report_output": report_output,
    }
    send_report_email(report_args, email_args, last_eom_business_date, export_base_path, EMAIL_JOB_NAME, LOG_TABLE, BODY_TEMPLATE, unc_base_path)
    print(f"  ✓ Email sent: {portfolio_name}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {portfolio_name}")
