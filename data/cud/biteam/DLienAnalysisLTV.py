# Databricks notebook source
# MAGIC %md
# MAGIC import logging
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

# import boto3
import os
import logging

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-02-28'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-02-28')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# DBTITLE 1,ENVIRONMENT VARIABLES
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
unity = dbutils.widgets.get("unity") 

table_name = 'DLienAnalysisLTV'

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

# MAGIC %md 
# MAGIC ##### Initialize the schema retrieved from Redis

# COMMAND ----------

schema = (
    "LienAnalysisLTVId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",LienAnalysisLTVKey string"
    ",PortfolioOriginKey int"
    ",AgreementKey string"
    ",BaseLTV double"
    ",LTV1stAuction double"
    ",LTV2ndAuction double"
    ",LTV3rdAuction double"
    ",LTV4thAuction double"
    ",ExpectedLTV double"
    ",RunDate date"
    ",ReferencedBusinessDate date"
    ",BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

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
# MAGIC ##### lien_analysis_ltv

# COMMAND ----------

lien_analysis_ltv = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.lien_analysis
  WHERE Granularity = 'Agreement'
  AND BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
),
lien_analysis_ltv AS (
SELECT 
    AgreementKey,
    lien.PortfolioId,
    BaseLTV,
    LTVFirstAuction,
    LTVSecondAuction,
    LTVThirdAuction,
    LTVFourthAuction,
    ExpectedLTV,
    businessdate,
    IngestionTimestamp,
    row_number() OVER(PARTITION BY AgreementKey ORDER BY IngestionTimestamp DESC) rn --keep the latest record for each agreement
FROM `{env}-ds`.aaas.lien_analysis lien
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON lien.PortfolioId = maxdate.PortfolioId
AND lien.BusinessDate = maxdate.MaxBusinessDate
WHERE lien.Granularity = 'Agreement'
)
SELECT * FROM lien_analysis_ltv WHERE rn = 1;
'''
)

lien_analysis_ltv.createOrReplaceTempView("lien_analysis_ltv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
    PortfolioName,
    PortfolioId,
    PortfolioKey,
    Businessdate
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Define some subqueries from the final query as temporary views for better readability and maintenance

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Final query with resulted DataFrame
# MAGIC

# COMMAND ----------

final_df = spark.sql(f'''
select sha2(concat_ws(";", lien_analysis_ltv.AgreementKey, "biteam"),256) LienAnalysisLTVId
    , 'biteam' SourceSystem
    , portfolio.PortfolioKey
    , portfolio.PortfolioName
    , CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp
    , cast(NUll as string) BatchId
    , sha2(concat_ws(";", lien_analysis_ltv.AgreementKey),256) LienAnalysisLTVKey
    , cast(lien_analysis_ltv.PortfolioId as int) PortfolioOriginKey
    , lien_analysis_ltv.AgreementKey AgreementKey
    , cast(lien_analysis_ltv.BaseLTV as double) BaseLTV
    , cast(lien_analysis_ltv.LTVFirstAuction as double) LTV1stAuction
    , cast(lien_analysis_ltv.LTVSecondAuction as double) LTV2ndAuction
    , cast(lien_analysis_ltv.LTVThirdAuction as double) LTV3rdAuction
    , cast(lien_analysis_ltv.LTVFourthAuction as double) LTV4thAuction
    , cast(lien_analysis_ltv.ExpectedLTV as double) ExpectedLTV
    , cast(lien_analysis_ltv.IngestionTimestamp as date) RunDate
    , lien_analysis_ltv.businessdate ReferencedBusinessDate
    , cast({run_date!r} as date) BusinessDate
from lien_analysis_ltv 
left join portfolio
on portfolio.portfolioid = lien_analysis_ltv.`PortfolioId` 
''')


# COMMAND ----------

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

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
