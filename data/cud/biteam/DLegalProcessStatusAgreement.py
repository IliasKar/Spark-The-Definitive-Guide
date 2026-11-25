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

# dbutils.widgets.text("run_date", '2026-07-02')
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

table_name = 'DLegalProcessStatusAgreement'

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
    "LegalProcessStatusAgreementId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",LegalProcessStatusAgreementKey string"
    ",OriginSourceSystem string"
    ",AgreementKey string"
    ",LegalProcessStatusLevel1Code bigint"
    ",LegalProcessStatusLevel1 string"
    ",LegalProcessStatusLevel2Code bigint"
    ",LegalProcessStatusLevel2 string"
    ",OperationalLegalStatusLevel3Code bigint"
    ",OperationalLegalStatusLevel3 string"
    ",LegalProcessStatusLastChangeDate timestamp"
    ",CourtType string"
    ",CourtDate timestamp"
    ",RunDate date"
    ",HasSpecialActivityFlag boolean"
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
# MAGIC ##### lps_contract

# COMMAND ----------

lps_contract = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.lps
  WHERE Granularity = 'Agreement'
  AND BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
)
SELECT 
    lps.SourceSystem,
    lps.PortfolioId,
    lps.AgreementKey AS AgreementNumber,
    lps.LegalProcessStatusLevel1,
    lps.LegalProcessStatusLevel1Description,
    lps.LegalProcessStatusLevel2,
    lps.LegalProcessStatusLevel2Description,
    lps.OperationalLegalStatusLevel3,
    lps.OperationalLegalStatusLevel3Description,
    lps.LPSLastChangeDate,
    lps.CourtType,
    lps.CourtDate,
    lps.IngestionTimestamp,
    lps.SpecialActivityFlag,
    lps.BusinessDate

FROM `{env}-ds`.aaas.lps lps
JOIN portfolio_max_businessdate maxdate
ON lps.PortfolioId = maxdate.PortfolioId
AND lps.BusinessDate = maxdate.MaxBusinessDate
WHERE lps.Granularity = 'Agreement'
QUALIFY ROW_NUMBER() OVER(PARTITION BY lps.AgreementKey ORDER BY lps.IngestionTimestamp DESC) = 1
'''
)

lps_contract.createOrReplaceTempView("lps_contract")


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

# Add Column OriginSourceSystem

lps_contract = spark.sql('''
                select lps_contract.*
                    , case when lps_contract.SourceSystem = 12465 then 'qca'
                        when lps_contract.SourceSystem = 14 then 'arotron'
                        end as OriginSourceSystem
                from lps_contract 
                ''')

lps_contract.createOrReplaceTempView("lps_contract")



# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Final query with resulted DataFrame
# MAGIC

# COMMAND ----------

final_df = spark.sql(f'''select 
    sha2(concat_ws(";", lps_contract.AgreementNumber, lps_contract.OriginSourceSystem, "biteam"),256) LegalProcessStatusAgreementId,
    'biteam' SourceSystem,
    portfolio.PortfolioKey,
    portfolio.PortfolioName,
    CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp,
    cast(NUll as string) BatchId,
    sha2(concat_ws(";", lps_contract.AgreementNumber, lps_contract.OriginSourceSystem),256) LegalProcessStatusAgreementKey,
    lps_contract.OriginSourceSystem,
    lps_contract.AgreementNumber AgreementKey,
    lps_contract.LegalProcessStatusLevel1 LegalProcessStatusLevel1Code,
    lps_contract.LegalProcessStatusLevel1Description LegalProcessStatusLevel1,
    lps_contract.LegalProcessStatusLevel2 LegalProcessStatusLevel2Code,
    lps_contract.LegalProcessStatusLevel2Description LegalProcessStatusLevel2,
    lps_contract.OperationalLegalStatusLevel3 OperationalLegalStatusLevel3Code,
    lps_contract.OperationalLegalStatusLevel3Description OperationalLegalStatusLevel3,
    lps_contract.LPSLastChangeDate LegalProcessStatusLastChangeDate,
    lps_contract.CourtType,
    lps_contract.CourtDate,
    cast(lps_contract.IngestionTimestamp as date) RunDate,
    lps_contract.SpecialActivityFlag HasSpecialActivityFlag,
    lps_contract.businessdate ReferencedBusinessDate,
    cast({run_date!r} as date) BusinessDate
from lps_contract
left join portfolio
    on portfolio.portfolioid = lps_contract.PortfolioId
''')


# COMMAND ----------

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

display(derivative_table_path)

# COMMAND ----------

target_schema = spark.table(derivative_table_path).schema
for field in target_schema:
    if field.name in final_df.columns:
        final_df = final_df.withColumn(field.name, final_df[field.name].cast(field.dataType))

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
