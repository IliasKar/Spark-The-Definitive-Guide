# Databricks notebook source
# Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix & Job Id of the Validator Job: ExternalFilesValidatorJob
env = aws_utils.get_env_prefix()
job_id = aws_utils.get_databricks_job_id("/cfn/databricks/job/LandingToReferenceIncremental/id")

# COMMAND ----------

validator_script = dbutils.widgets.get("validator_script")
print(validator_script)

# COMMAND ----------

import requests
import traceback

job_id = "938903286365237" # Job ID of: Validator Job - External Files
arguments = {"job_id": job_id}
response = requests.post(
    f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
    json=arguments,
    headers={"Authorization": "Bearer dapi8ec412165d5d95f2697bb02be096a24a"}
)
if response.status_code == 200:
    msg = f"Job with ID {job_id} called successfully!"
    print(msg)
else:
    msg = f"Failed to call job with ID {job_id}."
    print(msg)
    print(response.text)

# COMMAND ----------

# Returning a message from Notebook B
dbutils.notebook.exit(f"{msg}")
