# Databricks notebook source
# DBTITLE 1,Import libraries
import logging
import os

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-08-27')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# DBTITLE 1,Set up Variables
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

table_name = 'DBPCepalRecoveriesInitial'

# COMMAND ----------

# DBTITLE 1,Schema
schema = (
    "BPCepalRecoveriesInitialId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",BPCepalRecoveriesInitialKey string"
    ",CDI	string"
    ",DebtorKey	int"
    ",GroupCode	string"
    ",Strategy	string"
    ",ModelSegment	string"
    ",RecoveryDate	date"
    ",RecoveryAmount	decimal(19,2)"
    ",InitialBusinessPlan	boolean"
    ",BusinessPlanYear int"
    ",BusinessDate date"
)

# COMMAND ----------

# DBTITLE 1,Create and Delete Table
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
# MAGIC #####portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
    SELECT 
        PortfolioName
    ,   PortfolioId
    ,   PortfolioKey
    ,   unit_code_profits
    ,   Businessdate
    FROM  `{env}-cdh`.primary.portfolio
    WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Hermes_Investor_BP

# COMMAND ----------

df_hermes = spark.sql(f''' 
    select
        PortfolioId
    ,   `Borrower Id`           as DebtorKey
    ,   `Group ID`              as GroupCode
    ,   Strategy
    ,   `Recovery Date`         as RecoveryDate
    ,   `Recovery Amount`       as RecoveryAmount
    ,   `Initial Business Plan` as InitialBusinessPlan
    ,   year(`Effective Date`)  as BusinessPlanYear
    ,   cast({run_date!r} as date) as BusinessDate
    from `{env}-cdh`.reference.biteam_hermes_investor_bp
''')

df_hermes.createOrReplaceTempView("df_hermes")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Using BorrowerId to assemble CDI

# COMMAND ----------

from pyspark.sql.functions import col, lpad,concat, lit

prefix = "HRB"
total_length = 13

df_with_new_column = df_hermes.withColumn(
    "Suffix",
    lpad(col("DebtorKey").cast("string"), total_length - len(prefix), "0")
)


df_hermes_cdi = df_with_new_column.withColumn(
    "CDI",
    concat(lit(prefix), col("Suffix"))
)

df_hermes_cdi.createOrReplaceTempView("df_hermes_cdi")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Neptune_Investor_BP

# COMMAND ----------

df_neptune = spark.sql(f''' 
    select
        PortfolioId
    ,   CDI
    ,   GroupID                    as GroupCode
    ,   Model_Segment              as ModelSegment
    ,   `Recovery Date`          as RecoveryDate
    ,   `Recovery Amount`          as RecoveryAmount
    ,   `Initial Business Plan`  as InitialBusinessPlan
    ,   year(`Effective Date`)  as BusinessPlanYear
    ,   cast({run_date!r} as date) as BusinessDate
    from `{env}-cdh`.reference.biteam_neptune_investor_bp
''')

df_neptune.createOrReplaceTempView("df_neptune")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Union of the two Tables

# COMMAND ----------

df_hermes_neptune = spark.sql(f''' 
    select
        PortfolioId
    ,   CDI
    ,   DebtorKey
    ,   GroupCode
    ,   Strategy
    ,   null as ModelSegment
    ,   RecoveryDate
    ,   RecoveryAmount
    ,   InitialBusinessPlan
    ,   BusinessPlanYear
    ,   BusinessDate
    from df_hermes_cdi  

    union 

    select 
        PortfolioId
    ,   CDI
    ,   null as DebtorKey
    ,   GroupCode
    ,   null as Strategy
    ,   ModelSegment
    ,   RecoveryDate
    ,   RecoveryAmount
    ,   InitialBusinessPlan
    ,   BusinessPlanYear
    ,   BusinessDate
    from df_neptune
''')

df_hermes_neptune.createOrReplaceTempView("df_hermes_neptune")

# COMMAND ----------

final_df= spark.sql(f''' 
    select 
        sha2(concat_ws(";", hn.CDI, hn.RecoveryDate, hn.Strategy, "biteam"),256) BPCepalRecoveriesInitialId
    ,   'biteam' SourceSystem
    ,   cast(portfolio.unit_code_profits as string) as PortfolioKey
    ,   portfolio.PortfolioName
    ,   CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp
    ,   cast(NUll as string) BatchId
    ,   concat(hn.CDI,';',hn.RecoveryDate,';',coalesce(hn.Strategy,'')) BPCepalRecoveriesInitialKey
    ,   hn.CDI
    ,   hn.DebtorKey
    ,   hn.GroupCode
    ,   hn.Strategy
    ,   hn.ModelSegment
    ,   hn.RecoveryDate
    ,   hn.RecoveryAmount
    ,   hn.InitialBusinessPlan
    ,   hn.BusinessPlanYear
    ,   hn.BusinessDate            
    from df_hermes_neptune hn
    left join portfolio 
    on portfolio.portfolioid = hn.PortfolioId 
''')


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
