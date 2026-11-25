# Databricks notebook source
# MAGIC %md
# MAGIC import logging
# MAGIC #####Import Libaries and Functions
# MAGIC

# COMMAND ----------

import os
import logging

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"]
# # Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix() 

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
unity = dbutils.widgets.get("unity") 

table_name = 'DLienAnalysisRealEstateExposure'

# COMMAND ----------

# DBTITLE 1,SCHEMA INITIALIZATION
schema = (
    "LienAnalysisRealEstateExposureId string"
    ",SourceSystem string"
    ",PortfolioKey int"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",AgreementKey string"
    ",ExposureKey string"
    ",RealEstateCepalKey string"
    ",AllocREVBase double"
    ",CREVBase double"
    ",CREVBaseTLC double"
    ",RunDate date"
    ",ReferencedBusinessDate date"
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

portfolio = spark.sql(
f'''
SELECT 
    PortfolioName,
    PortfolioId,
    PortfolioKey,
    Businessdate,
    unit_code_profits
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

lien_analysis_property_account = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.lien_analysis
  WHERE Granularity = 'RealEstateExposure'
  AND BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
),
lien_analysis_property_account AS (
SELECT 
    lien.PortfolioId,
    AgreementKey,
    ExposureKey,
    RealEstateCepalKey,
    AllocatedREVBase,
    CREVBaseGBV,
    CREVBaseTLC,
    IngestionTimestamp,
    BusinessDate,
    row_number() OVER(PARTITION BY AgreementKey, ExposureKey, RealEstateCepalKey ORDER BY IngestionTimestamp DESC) rn --keep the latest record for each agreement,exposure, realestatecepalkey
FROM `{env}-ds`.aaas.lien_analysis lien
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON lien.PortfolioId = maxdate.PortfolioId
AND lien.BusinessDate = maxdate.MaxBusinessDate
WHERE lien.Granularity = 'RealEstateExposure'
)
SELECT * FROM lien_analysis_property_account WHERE rn = 1;
'''
)

lien_analysis_property_account.createOrReplaceTempView("lien_analysis_property_account")

# COMMAND ----------

final_df = spark.sql(f'''
select 
    sha2(concat_ws(";", lien.AgreementKey, lien.ExposureKey, lien.RealEstateCepalKey, "biteam"),256) LienAnalysisRealEstateExposureId
    ,'biteam' SourceSystem
    ,portfolio.unit_code_profits PortfolioKey
    ,portfolio.PortfolioName
    ,CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp
    ,cast(NUll as string) BatchId
    ,lien.AgreementKey AgreementKey
    ,lien.ExposureKey ExposureKey
    ,lien.RealEstateCepalKey RealEstateCepalKey
    ,lien.AllocatedREVBase AllocREVBase
    ,lien.CREVBaseGBV CREVBase
    ,lien.CREVBaseTLC CREVBaseTLC
    ,cast(lien.IngestionTimestamp as date) RunDate
    ,lien.businessdate ReferencedBusinessDate
    ,cast({run_date!r} as date) BusinessDate
from Lien_Analysis_Property_Account lien
left join portfolio 
on portfolio.portfolioid = lien.PortfolioId
''')

final_df.createOrReplaceTempView("final_df")

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
