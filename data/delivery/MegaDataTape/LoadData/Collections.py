# Databricks notebook source

# COMMAND ----------

# run_date parameter
# If provided, must be a valid date (YYYY-MM-DD) and a last business day of month.
# If not provided, defaults to last business day of previous month.
dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
dbutils.widgets.text("portfolio_exact_match", "false", "Exact Match")

# COMMAND ----------

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
)

import os
import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname
from helpers import get_validated_eom_run_date, build_portfolio_filter, delete_from_unity_table

# COMMAND ----------

env = get_env_prefix()
env_name = get_env_fullname()

# COMMAND ----------

partition_columns = ["ReportingDate"]
target_area = f"`{env}-cdh-delivery`.`mega-datatape`"

# COMMAND ----------

run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
portfolio_filter, portfolio_param, portfolio_display = build_portfolio_filter(dbutils.widgets.get("portfolio"), portfolio_exact_match=dbutils.widgets.get("portfolio_exact_match").strip().lower() == "true")

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
print(f"Executing: {notebook_name} | run_date: {run_date} | portfolio: {portfolio_display}")

# COMMAND ----------

dexposure = spark.sql(f"""
                      SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                      """)

dexposure.createOrReplaceTempView("dexposure")

dparty = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} and sourcesystem='profits'
                   AND {portfolio_filter}
                      """)

dparty.createOrReplaceTempView("dparty")

collection = spark.sql(f"""
                       SELECT *
                       FROM `{env}-cdh`.derivative.get_collection_data(DATE({run_date!r}))
                       WHERE {portfolio_filter}
                                          """)
 
collection.createOrReplaceTempView("collection")

restructuring = spark.sql(f"""
                          select * 
                          from `{env}-cdh`.primary.restructuring
                          where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                          """)

restructuring.createOrReplaceTempView("restructuring")

exposure = spark.sql(f"""
                    SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.primary.get_all_exposures(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                    """)

exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

Collections = spark.sql(f"""
                    SELECT distinct
                        cast({run_date!r} as date) as ReportingDate,
                        col.PortfolioName,
                        col.DCollectionKey as CollectionKey,
                        col.ActualDate,
                        col.Amount,
                        -- pay.PaymentTypeGroup,
                        COL.PaymentTypeGroup, -- EK 20260309
                        col.PaymentTypeEng PaymentType,  
                        dex.ExposureKey,
                        dex.CDI,
                        cast((case when col.PortfolioName like 'Hermes%' then dp.GroupCode
                        when col.PortfolioName = 'Cetus' then  dp.GroupCodePortfolio end) as string) as GroupID,
                        concat(res.NewAgreementKey,' | ',res.restructuringdate) AS PaymentAgreementID,
                        case when col.portfolioname='EUCLID' and dex.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
                        when col.portfolioname='EUCLID' and dex.SubServicer is null then 'Cepal'
                        else null end as SubServicerFlag,
                        dex.IsHighBalanceFlag as HighBalanceFlag
                    FROM collection col
                    INNER JOIN dexposure dex
                    ON dex.ExposureKey = col.ExposureKey
                    LEFT JOIN exposure ex
                    ON  ex.exposureKey=dex.ExposureKey and ex.DiasNumber=col.DiasNumber    
                    LEFT JOIN (select distinct dp.PartyKey, dp.groupcode, dp.GroupCodePortfolio
                                from dparty dp) as dp
                    ON dp.PartyKey = coalesce(ex.PartyKey, dex.PartyKey)      
                    -- LEFT JOIN PaymentTypeGroupMapping pay        -- EK 20260309
                    -- ON col.ProfitsPaymentTypeCode = pay.ProfitsPaymentTypeCode 
                    left join restructuring res
                    on res.NewExposurekey = dex.Exposurekey    
                """)

target_table_path_Collections = f"{target_area}.collections"

if spark.catalog.tableExists(target_table_path_Collections):
    delete_from_unity_table(
    spark,
    target_table_path_Collections,
    run_date,
    portfolio_filter,
)
 
Collections = Collections.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

Collections.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_Collections) # in order to write in delta_lake
 
spark.sql(f"REPAIR TABLE {target_table_path_Collections} SYNC METADATA")