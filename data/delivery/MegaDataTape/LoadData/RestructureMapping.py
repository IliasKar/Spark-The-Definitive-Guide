# Databricks notebook source

# COMMAND ----------

# ##Initialization

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
dbutils.widgets.text("portfolio_exact_match", "false", "Exact Match")

# COMMAND ----------

from pyspark.sql.functions import (
    coalesce, col, concat_ws, current_timestamp, from_utc_timestamp,
    lit, sha2, substring, trim, when, months_between, dateadd, datediff,
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

env = get_env_prefix()
env_name = get_env_fullname()

partition_columns = ["ReportingDate"]
run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
portfolio_filter, portfolio_param, portfolio_display = build_portfolio_filter(dbutils.widgets.get("portfolio"), portfolio_exact_match=dbutils.widgets.get("portfolio_exact_match").strip().lower() == "true")
target_area = f"`{env}-cdh-delivery`.`mega-datatape`"

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
print(f"Executing: {notebook_name} | run_date: {run_date} | portfolio: {portfolio_display}")

# COMMAND ----------

# ##Import Tables

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

drestructuring = spark.sql(f"""
                          select * 
                          from `{env}-cdh`.derivative.DRestructuring
                          where BusinessDate = {run_date!r} and IsRestructureActive = 1
                          """)

drestructuring.createOrReplaceTempView("drestructuring")

product = spark.sql(f"""
                    select * 
                    from `{env}-cdh`.primary.product
                    where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                    """)

product.createOrReplaceTempView("product")

# COMMAND ----------

# ##RestructureMapping

# COMMAND ----------

AggregatedDparty = spark.sql(f"""
    SELECT
        CDI,
        MAX(CASE
            WHEN PortfolioName LIKE 'Hermes%' THEN GroupCode
            WHEN PortfolioName = 'Cetus' THEN GroupCodePortfolio
        END) AS GroupID
    FROM dparty
    GROUP BY CDI
""")

AggregatedDparty.createOrReplaceTempView("AggregatedDparty")

RestructureMapping = spark.sql(f"""
SELECT distinct
res.BusinessDate as ReportingDate,
res.PortfolioName,
ex.CDI,
agdp.GroupID,
res.NewAgreementKey,
res.OldExposureKey,
res.NewExposureKey,
case when pr.ProductDescription like '%ΥΠΟ ΑΙΡΕΣΗ%' then 'Conditional Woff' else 'Restructure' end as RestructureType,
case when res.portfolioname='EUCLID' and dp.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
when res.portfolioname='EUCLID' and dp.SubServicer is null then 'Cepal'
else null end as SubServicerFlag,
dp.HighBalanceFlag
FROM drestructuring res
INNER JOIN dexposure ex
ON res.NewExposureKey = ex.ExposureKey
LEFT JOIN product pr
ON ex.ProductKey = pr.ProductKey
LEFT JOIN AggregatedDparty agdp
ON ex.CDI = agdp.CDI
LEFT JOIN dparty dp
ON ex.CDI = dp.CDI
WHERE {portfolio_filter.replace('PortfolioName', 'res.PortfolioName')}
""")

target_table_path_RestructureMapping = f"{target_area}.RestructureMapping"

if spark.catalog.tableExists(target_table_path_RestructureMapping):
    delete_from_unity_table(spark, target_table_path_RestructureMapping, run_date, portfolio_filter)

RestructureMapping = RestructureMapping.withColumn("GroupID", col("GroupID").cast("string"))
RestructureMapping = RestructureMapping.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

RestructureMapping.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_RestructureMapping)

spark.sql(f"REPAIR TABLE {target_table_path_RestructureMapping} SYNC METADATA")
