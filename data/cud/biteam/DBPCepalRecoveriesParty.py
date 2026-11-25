# Databricks notebook source
import logging
# DBTITLE 1,Import libraries
import os

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# DBTITLE 1,Set up Variables
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

table_name = 'DBPCepalRecoveriesParty'

# COMMAND ----------

# DBTITLE 1,Schema
schema = (
    "BPCepalRecoveriesPartyId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BPCepalRecoveriesPartyKey string"
    ",BatchId string"
    ",BusinessPlan string"
    ",KeyType string"
    ",CDI string"
    ",AgreementKey string"
    ",Month string"
    ",NetRecoveries double"
    ",GrossRecoveries double"
    ",BaseDate date"
    ",Segment string"
    ",Strategy string"
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

df_bp_net_recoveries_key_vw = spark.sql(f''' 
    select 
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   Key
    ,   month
    ,   Net_Recoveries
    ,   Base_Date
    ,   Loading_Date
    ,   cast({run_date!r} as date) as BusinessDate
    from `{env}-cdh`.reference.biteam_bp_net_recoveries_key
''')

df_bp_net_recoveries_key_vw.createOrReplaceTempView("df_bp_net_recoveries_key_vw")

# COMMAND ----------

df_bp_gross_recoveries_key_vw = spark.sql(f''' 
    select 
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   Key
    ,   month
    ,   Segment
    ,   Strategy
    ,   Gross_Recoveries
    ,   Base_Date
    ,   Loading_Date 
    ,   cast({run_date!r} as date) as BusinessDate
    from `{env}-cdh`.reference.biteam_bp_gross_recoveries_key
''')

df_bp_gross_recoveries_key_vw.createOrReplaceTempView("df_bp_gross_recoveries_key_vw")

# COMMAND ----------

df_bp_net_recoveries_union = spark.sql(f'''
                                       
    select
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   Key    as CDI
    ,   null   as AgreementKey
    ,   month
    ,   Net_Recoveries
    ,   Base_Date
    ,   Loading_Date 
    ,   BusinessDate
    from df_bp_net_recoveries_key_vw
    where Key_Type='CDI'

    union

    select
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   null   as CDI
    ,   Key    as AgreementKey
    ,   month
    ,   Net_Recoveries
    ,   Base_Date
    ,   Loading_Date 
    ,   BusinessDate
    from df_bp_net_recoveries_key_vw
    where Key_Type='Contract'

''')

df_bp_net_recoveries_union.createOrReplaceTempView("df_bp_net_recoveries_union")                                      

# COMMAND ----------

df_bp_gross_recoveries_union = spark.sql(f'''
                                       
    select
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   Key    as CDI
    ,   null   as AgreementKey
    ,   month
    ,   Segment
    ,   Strategy
    ,   Gross_Recoveries
    ,   Base_Date
    ,   Loading_Date 
    ,   BusinessDate
    from df_bp_gross_recoveries_key_vw
    where Key_Type='CDI'

    union

    select
        BP
    ,   PortfolioId
    ,   Key_Type
    ,   null   as CDI
    ,   Key    as AgreementKey
    ,   month
    ,   Segment
    ,   Strategy
    ,   Gross_Recoveries
    ,   Base_Date
    ,   Loading_Date 
    ,   BusinessDate
    from df_bp_gross_recoveries_key_vw
    where Key_Type='Contract'

''')

df_bp_gross_recoveries_union.createOrReplaceTempView("df_bp_gross_recoveries_union")                                      

# COMMAND ----------

df_net_gross_recoveries_key = spark.sql(f'''
    select 
   coalesce (net.BP, gross.BP) AS BP
    ,   coalesce(net.PortfolioId,gross.PortfolioId) as Portfolio
    ,   coalesce(net.Key_Type,gross.Key_Type) as Key_Type
    ,   coalesce(net.CDI,gross.CDI) as CDI
    ,   coalesce(net.AgreementKey,gross.AgreementKey) as AgreementKey
    ,   coalesce(net.Base_Date, gross.Base_Date) as Base_Date
    ,   coalesce(net.month, gross.month) as month
    ,   net.Net_Recoveries
    ,   gross.Gross_Recoveries
    ,   gross.Segment
    ,   gross.Strategy
    ,   portfolio.unit_code_profits
    ,   portfolio.PortfolioName
    ,   sha2(concat_ws(";", coalesce (net.BP, gross.BP), coalesce(net.CDI,gross.CDI), coalesce(net.month,gross.month), portfolio.unit_code_profits, "biteam", gross.Segment, gross.Strategy),256) BPCepalRecoveriesPartyId
    ,   concat(coalesce (net.BP, gross.BP),';',coalesce(net.CDI,gross.CDI),';',coalesce(net.month,gross.month),';',portfolio.unit_code_profits, ';', gross.Segment, ';', gross.Strategy) as BPCepalRecoveriesPartyKey
    ,portfolio.BusinessDate
    from df_bp_net_recoveries_union net 
    full outer join  df_bp_gross_recoveries_union gross
    on      net.BP = gross.BP
        and net.PortfolioId = gross.PortfolioId
        and net.CDI = gross.CDI
        and net.month=gross.month
    left join portfolio 
    on      portfolio.portfolioid = coalesce(net.PortfolioId,gross.PortfolioId)
    where coalesce(net.Key_Type, gross.Key_Type)='CDI'

    union

    select 
   coalesce (net.BP, gross.BP) AS BP
    ,   coalesce(net.PortfolioId,gross.PortfolioId) as Portfolio
    ,   coalesce(net.Key_Type,gross.Key_Type) as Key_Type
    ,   coalesce(net.CDI,gross.CDI) as CDI
    ,   coalesce(net.AgreementKey,gross.AgreementKey) as AgreementKey
    ,   coalesce(net.Base_Date, gross.Base_Date) as Base_Date
    ,   coalesce(net.month, gross.month) as month
    ,   net.Net_Recoveries
    ,   gross.Gross_Recoveries
    ,   gross.Segment
    ,   gross.Strategy
    ,   portfolio.unit_code_profits
    ,   portfolio.PortfolioName
    ,   sha2(concat_ws(";", coalesce (net.BP, gross.BP), coalesce(net.CDI,gross.CDI), coalesce(net.month,gross.month), portfolio.unit_code_profits, "biteam", gross.Segment, gross.Strategy),256) BPCepalRecoveriesPartyId
    ,   concat(coalesce (net.BP, gross.BP),';',coalesce(net.CDI,gross.CDI),';',coalesce(net.month,gross.month),';',portfolio.unit_code_profits, ';', gross.Segment, ';', gross.Strategy) as BPCepalRecoveriesPartyKey
    ,portfolio.BusinessDate
    from df_bp_net_recoveries_union net 
    full outer join df_bp_gross_recoveries_union gross
    on      net.BP = gross.BP
        and net.PortfolioId = gross.PortfolioId
        and net.AgreementKey = gross.AgreementKey
        and net.month=gross.month
    left join portfolio 
    on      portfolio.portfolioid = coalesce(net.PortfolioId,gross.PortfolioId)
    where coalesce(net.Key_Type, gross.Key_Type)='Contract'
''')

df_net_gross_recoveries_key.createOrReplaceTempView("df_net_gross_recoveries_key")

# COMMAND ----------

final_df= spark.sql(f'''    
    select 
        BPCepalRecoveriesPartyId
    ,   'biteam' SourceSystem
    ,   cast(unit_code_profits as string)   as PortfolioKey
    ,   PortfolioName
    ,   CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp
    ,   cast(NUll as string) BatchId
    ,   BPCepalRecoveriesPartyKey
    ,   BP                                  as BusinessPlan
    ,   Key_Type                            as KeyType
    ,   CDI
    ,   AgreementKey
    ,   Month
    ,   Segment
    ,   Strategy
    ,   Net_Recoveries                      as NetRecoveries
    ,   Gross_Recoveries                    as GrossRecoveries
    ,   Base_Date                           as BaseDate
    ,   BusinessDate
    from df_net_gross_recoveries_key
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
