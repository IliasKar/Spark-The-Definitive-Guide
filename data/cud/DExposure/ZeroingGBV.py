# Databricks notebook source
import logging
from typing import Any

from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    trim,
    when,
    months_between,
    dateadd,
    datediff,
    regexp_replace,
    min,
)

import os
#env = os.environ["S3PREFIX"] 



import delta_table_creator, transform, user_defined_functions 

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

#Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix

env = aws_utils.get_env_prefix()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Creation

# COMMAND ----------

schema = (
    "ExposureKey	string,"
    "GBVTriggerEventFlag	boolean,"
    "GBVTriggerEventDate	date,"
    "GBVTriggerEventType	string,"
    "GBVCappedAmount	double,"
    "GBVCappedDate	date,"
    "GBVTailExposure	double,"
    "GBVFunctionResult	double,"
    "BusinessDate date"
)

partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "zeroinggbvexposure"
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"

#================== Create Delta Table ==================

logging.info("Creating Delta Table...")
if spark.catalog.tableExists(derivative_table_path) is False:
    create_delta_table(
        spark,
        derivative_table_path,
        partition_columns,
        schema,  # type: ignore
    )
else:
    logging.info("Table already exists.")

# ================== Delete From Delta Table ==================
transform.delete_from_unity_table(
    spark,
    derivative_table_path,
    run_date,  # type: ignore
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Exposures which are at least defined as Capped

# COMMAND ----------

ZeroGBV = spark.sql(f""" select
 
    ExposureKey,
    GBVTriggerEventFlag,
    GBVTriggerEventDate,
    GBVTriggerEventType,
    GBVCappedAmount,
    GBVCappedDate,
    TailExposure as  GBVTailExposure,
    GBVFunctionResult,
    BusinessDate

FROM `{env}-cdh`.derivative.DExposure
WHERE BusinessDate = {run_date!r} and GBVCappedDate is not null
"""
)

# COMMAND ----------

result_df = ZeroGBV

if unity == "True":
    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else: 
    result_df.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
