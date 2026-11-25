# Databricks notebook source
import logging
import delta_table_creator, extract, transform
import boto3
import os
from pyspark.sql.functions import (
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
)

from delta import DeltaTable  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql.functions import trim

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-01-31'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-08-27')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"]
# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
# table_name = dbutils.widgets.get("table_name") 
table_name = 'DBPAmicableReport'
unity = dbutils.widgets.get("unity") 

# """
# Takes the s3 parameter value from ssm

# ssm_client = boto3.client("ssm", region_name="eu-central-1") # Initialize the SSM client

# response = ssm_client.get_parameter(
#     Name='/cfn/a360/matching/conf_s3_path',
#     WithDecryption=False
# )

# s3_path = response['Parameter']['Value']
# """

# COMMAND ----------

# DBTITLE 1,SCHEMA INITIALIZATION
schema = (
            "DBPAmicableReportID string"
            ",SourceSystem string"
            ",PortfolioKey string"
            ",PortfolioName string"
            ",IngestionTimestamp timestamp"
            ",BatchId string"
            ",DBPAmicableReportKey string"
            ",BP string"
            ",Month string"
            ",Segment string"
            ",Strategy string"
            ",KeysEnteringInAmicable double"
            ",BalanceEnteringInAmicable double"
            ",GBVEnteringInAmicable double"
            ",RestructuredAmount double"
            ",BusinessDate date"
        )

# COMMAND ----------

# DBTITLE 1,CREATE - DELETE DELTA TABLE
partition_columns = ["BusinessDate"]
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
 
# ================== Create Delta Table ==================
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
# MAGIC ##### Create wanted Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### biteam_bp_amicable_report

# COMMAND ----------

biteam_bp_amicable_report = spark.sql(
f'''
SELECT 
    BP
  , PortfolioId
  , Month
  , Segment
  , Strategy
  , Keys_Entering_in_Amicable
  , Balance_Entering_In_Amicable
  , GBV_Entering_In_Amicable
  , Restructured_Amount
  , cast({run_date!r} as date) as BusinessDate
FROM  `{env}-cdh`.reference.biteam_bp_amicable_report
'''
)

biteam_bp_amicable_report.createOrReplaceTempView("biteam_bp_amicable_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
  PortfolioName,
  PortfolioId,
  unit_code_profits
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# DBTITLE 1,JOINS
# ================== Joins ==================
biteam_bp_amicable_report_df = (
    biteam_bp_amicable_report.join(
        portfolio,
        biteam_bp_amicable_report.PortfolioId == portfolio.PortfolioId,
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF DATAFRAME
# ================== biteam_bp_amicable_report_df ==================
biteam_bp_amicable_report_df_primary_key_fields = [
    biteam_bp_amicable_report.BP,
    biteam_bp_amicable_report.PortfolioId,
    biteam_bp_amicable_report.Month,
    biteam_bp_amicable_report.Segment,
    biteam_bp_amicable_report.Strategy,
    lit("biteam"),
]

biteam_bp_amicable_report_df_key_fields = [
    biteam_bp_amicable_report.BP,
    biteam_bp_amicable_report.PortfolioId,
    biteam_bp_amicable_report.Month,
    biteam_bp_amicable_report.Segment,
    biteam_bp_amicable_report.Strategy,
]

biteam_bp_amicable_report_df = biteam_bp_amicable_report_df.withColumn(
        "DBPAmicableReportID",
        sha2(
            concat_ws(
                ";", *biteam_bp_amicable_report_df_primary_key_fields
            ),
            256,
        ),
    ).withColumn(
        "DBPAmicableReportKey",
            concat_ws(
                ";", *biteam_bp_amicable_report_df_key_fields
            ),
        )

# COMMAND ----------

final_df = biteam_bp_amicable_report_df.select(
            biteam_bp_amicable_report_df.DBPAmicableReportID.cast("string").alias("DBPAmicableReportID"),
            lit("biteam").cast("string").alias("SourceSystem"),
            portfolio.unit_code_profits.cast("string").alias("PortfolioKey"),
            portfolio.PortfolioName.cast("string").alias("PortfolioName"),
            lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
            lit(None).cast("String").alias("BatchId"),
            biteam_bp_amicable_report_df.DBPAmicableReportKey.cast("string").alias("DBPAmicableReportKey"),
            biteam_bp_amicable_report.BP.cast("string").alias("BP"),
            biteam_bp_amicable_report.Month.cast("string").alias("Month"),
            biteam_bp_amicable_report.Segment.cast("string").alias("Segment"),
            biteam_bp_amicable_report.Strategy.cast("string").alias("Strategy"),
            biteam_bp_amicable_report.Keys_Entering_in_Amicable.cast("double").alias("KeysEnteringInAmicable"),
            biteam_bp_amicable_report.Balance_Entering_In_Amicable.cast("double").alias("BalanceEnteringInAmicable"),
            biteam_bp_amicable_report.GBV_Entering_In_Amicable.cast("double").alias("GBVEnteringInAmicable"),
            biteam_bp_amicable_report.Restructured_Amount.cast("double").alias("RestructuredAmount"),
            biteam_bp_amicable_report.BusinessDate.cast("date").alias("BusinessDate"),
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

# DBTITLE 1,WRITE TO DELTA TABLE
if unity == "True":
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
