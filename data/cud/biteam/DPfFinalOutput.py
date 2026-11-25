# Databricks notebook source
import logging
import delta_table_creator, extract, transform
import boto3
import os
from pyspark.sql.functions import (
    coalesce,
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

# dbutils.widgets.text("run_date", '2024-01-31')
# dbutils.widgets.text("table_name", 'DPortfolioToolCore')
# dbutils.widgets.text("hive_unity_indicator", 'hive')
# dbutils.widgets.text("utp_date", '2024-01-17')

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
table_name = 'DPfFinalOutput'
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
            "DPfFinalOutputId string,"
            "SourceSystem string,"
            "PortfolioKey string,"
            "PortfolioName string,"
            "IngestionTimestamp timestamp,"
            "BatchId string,"
            "DPfFinalOutputKey string,"
            "ExposureKey string,"
            "PartyTaxRegistrationNumber string,"
            "PrimaryPartyCDI string,"
            "AgreementKey string,"
            "PayerFlagEntity string,"
            "PayerFlagExposure string,"
            "PayerFlagAgreement string,"
            "ReferencedBusinessDate date"
            "BusinessDate date"
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
# MAGIC ##### biteam_pf_final_output

# COMMAND ----------

biteam_pf_final_output = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT Portfolio_Id, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.payers_flag_algorithm
  WHERE BusinessDate <= {run_date!r}
  GROUP BY Portfolio_Id
),
biteam_pf_final_output AS (
SELECT 
  Account_Number_with_Prefix,
  PartyTaxRegistrationNumber,
  CDI,
  AgreementNumber,
  payerflag.Portfolio_Id,
  Payer_Flag_Entity,
  Payer_Flag_Account,
  Payer_flag_Aggrement,
  BusinessDate,
  row_number() OVER(PARTITION BY Account_Number_with_Prefix ORDER BY Ref_Date DESC, Upload_date DESC) rn --keep the latest record for each account number
FROM  `{env}-ds`.aaas.payers_flag_algorithm payerflag
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON payerflag.Portfolio_Id = maxdate.Portfolio_Id
AND payerflag.BusinessDate = maxdate.MaxBusinessDate
)
SELECT * FROM biteam_pf_final_output WHERE rn = 1;
'''
)

biteam_pf_final_output.createOrReplaceTempView("biteam_pf_final_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
  PortfolioName,
  unit_code_profits,
  PortfolioId
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# DBTITLE 1,JOINS
# ================== Joins ==================
biteam_pf_final_output_df = (
    biteam_pf_final_output.join(
        portfolio,
        biteam_pf_final_output["Portfolio_Id"] == portfolio["PortfolioId"],
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF DATAFRAME
# ================== biteam_pf_final_output_df ==================
biteam_pf_final_output_df_primary_key_fields = [
    biteam_pf_final_output.Account_Number_with_Prefix,
    lit("biteam"),
]

biteam_pf_final_output_df = (
    biteam_pf_final_output_df.withColumn(
        "DPfFinalOutputID",
        sha2(
            concat_ws(
                ";", *biteam_pf_final_output_df_primary_key_fields
            ),
            256,
        ),
    )
)

# COMMAND ----------

final_df = (
            biteam_pf_final_output_df.select(
                biteam_pf_final_output_df.DPfFinalOutputID.cast("string"),
                lit("biteam").cast("string").alias("SourceSystem"),
                portfolio.unit_code_profits.cast("string").alias("PortfolioKey"),
                portfolio.PortfolioName.cast("string").alias("PortfolioName"),
                lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
                lit(None).cast("String").alias("BatchId"),
                biteam_pf_final_output.Account_Number_with_Prefix.cast("string").alias("DPfFinalOutputKey"),
                biteam_pf_final_output.Account_Number_with_Prefix.cast("string").alias("ExposureKey"),
                biteam_pf_final_output.PartyTaxRegistrationNumber.cast("string").alias("PartyTaxRegistrationNumber"),
                biteam_pf_final_output.CDI.cast("string").alias("PrimaryPartyCDI"),
                biteam_pf_final_output.AgreementNumber.alias("AgreementKey"),
                biteam_pf_final_output.Payer_Flag_Entity.cast("string").alias("PayerFlagEntity"),
                biteam_pf_final_output.Payer_Flag_Account.cast("string").alias("PayerFlagExposure"),
                biteam_pf_final_output.Payer_flag_Aggrement.cast("string").alias("PayerFlagAgreement"),
                biteam_pf_final_output.BusinessDate.cast("date").alias("ReferencedBusinessDate"),
                lit(run_date).cast("date").alias("BusinessDate")
            )
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
