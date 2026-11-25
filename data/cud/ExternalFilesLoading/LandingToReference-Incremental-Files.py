# Databricks notebook source
import file_loading
from crawler import update_crawler_with_delta_targets
from pyspark.sql import SparkSession

# Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix

env = aws_utils.get_env_prefix()

# COMMAND ----------

relative_root_path = dbutils.widgets.get("relative_root_path")
incoming_folder = dbutils.widgets.get("incoming_folder")
output_folder = dbutils.widgets.get("output_folder")
separator  = dbutils.widgets.get("separator")

dbutils.widgets.text("unity", "True")
unity = dbutils.widgets.get("unity")

dbutils.widgets.text("incremental", "True")
incremental = dbutils.widgets.get("incremental")

# COMMAND ----------

arg_keys = {
        "relative_root_path": '/'.join(relative_root_path.split('/')[:-1]),
        "file_name": relative_root_path.split("/")[-1],
        "incoming_folder": incoming_folder,
        "output_folder": output_folder,
        "separator": separator,
        "unity": unity,
        "incremental": incremental,
    }

relative_path = relative_root_path.split("/")[0]
validator_notebook = "./CallValidatorJob"
try:
    validator_script_sql = spark.sql(f"""SELECT ValidatorScript
                                       FROM `{env}-cdh`.`workflow`.ExternalFilesConfiguration
                                       WHERE UPPER(RelativePath) = UPPER('{relative_path}') 
                                       AND UPPER(Flow) = UPPER('Incremental')""").collect()[0]
except:
    raise ValueError("No records found in ExternalFilesConfiguration for the given RelativePath and Flow. Check these values and try again!")
validator_script = validator_script_sql["ValidatorScript"]
result = dbutils.notebook.run(validator_notebook, timeout_seconds=300, arguments={"validator_script": validator_script})
print("Result from Validator Notebook:", result)

file_loading.file_to_landing_to_reference(spark,arg_keys)
# add 

# COMMAND ----------

if unity == 'False':
    file_name = relative_root_path[0].split("/")[-1]

    crawler_name = f"{env}-ec1-cepal-crawler-reference"
    new_delta_targets = [
        f"s3://{env}-ec1-cepal-reference-zone/{output_folder}/{file_name}/",
    ]

    update_crawler_with_delta_targets(crawler_name, new_delta_targets)
