# Databricks notebook source
import logging
import os

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

table_name = 'DBPCepalRecoveriesAgreement'

# COMMAND ----------

schema = (
    "BPCepalRecoveriesAgreementId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",BPCepalRecoveriesAgreementKey string"
    ",BusinessPlan string"
    ",AgreementKey string"
    ",Month string"
    ",NetRecoveries double"
    ",GrossRecoveries double"
    ",BaseDate date"
    ",BusinessDate date"
)

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

portfolio = spark.sql(f'''
    SELECT 
        PortfolioName
    ,   PortfolioId
    ,   PortfolioKey
    ,   unit_code_profits
    ,   Businessdate
    FROM  `{env}-cdh`.primary.portfolio
    WHERE BusinessDate = {run_date!r}
''')

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

df_bp_recoveries_contract = spark.sql(f'''
    SELECT
        BP
    ,   PortfolioId
    ,   AgreementNumber
    ,   Month
    ,   Net_Recoveries
    ,   Gross_Recoveries
    ,   cast({run_date!r} as date) as BusinessDate
    ,   Base_Date
    ,   Loading_Date
    FROM  `{env}-ds`.reference.bp_recoveries_contract
''')

df_bp_recoveries_contract.createOrReplaceTempView("df_bp_recoveries_contract")

# COMMAND ----------

final_df = spark.sql(f'''
    select 
        sha2(concat_ws(";", RC.BP, portfolio.unit_code_profits, RC.AgreementNumber, RC.Month, "biteam"),256)    as BPCepalRecoveriesAgreementId
    ,   'biteam'                                                                                                as SourceSystem
    ,   cast(portfolio.unit_code_profits as string)                                                             as PortfolioKey
    ,   portfolio.PortfolioName
    ,   CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP)                             as IngestionTimestamp
    ,   cast(NUll as string)                                                                                    as BatchId
    ,   concat(portfolio.unit_code_profits,';',RC.BP,';',RC.AgreementNumber,';',RC.Month)                       as BPCepalRecoveriesAgreementKey
    ,   RC.BP                                                                                                   as BusinessPlan
    ,   RC.AgreementNumber                                                                                      as AgreementKey
    ,   RC.Month
    ,   RC.Net_Recoveries                                                                                       as NetRecoveries
    ,   RC.Gross_Recoveries                                                                                     as GrossRecoveries
    ,   cast(RC.Base_Date as date)                                                                              as BaseDate
    ,   RC.BusinessDate
    from df_bp_recoveries_contract RC
    left join portfolio 
    on portfolio.portfolioid = RC.PortfolioId 
''')

final_df.createOrReplaceTempView("final_df")

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
