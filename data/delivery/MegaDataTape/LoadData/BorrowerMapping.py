# Databricks notebook source

# COMMAND ----------

# ##Initialization

# COMMAND ----------

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

dparty = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} and sourcesystem='profits'
                   AND {portfolio_filter}
                      """)

dparty.createOrReplaceTempView("dparty")

portfolio = spark.sql(f"""
                       select * 
                       from `{env}-cdh`.primary.portfolio
                       where BusinessDate = {run_date!r} 
                       """)

portfolio.createOrReplaceTempView("portfolio")                       

exposure = spark.sql(f"""
                    SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.primary.get_all_exposures(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                    """)

exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

# ##BorrowerMapping

# COMMAND ----------

BorrowerMapping = spark.sql(f"""
SELECT distinct
cast(ex.RunDate as date) AS ReportingDate,
ex.PortfolioName,
ex.ExposureKey,
ex.CDI,
(case when dp.PortfolioName like 'Hermes%' then dp.GroupCode
when dp.PortfolioName='Cetus' then  dp.GroupCodePortfolio end) as GroupID,
ex.AgreementKey,
case when ex.PartyRelationshipType ='Guarantor' then 'Loan-Guarantor Rltp'
when ex.PartyRelationshipType ='Main Beneficiary' then 'Loan-Main Beneficiary Rltp'
when ex.PartyRelationshipType ='Primary Owner' then 'Loan-Primary Owner Rltp'
when ex.PartyRelationshipType ='Other Beneficiaries' then 'Loan-Other Beneficiaries Rltp'
when ex.PartyRelationshipType ='Co-Owner' then 'Loan-Co-Owner Rltp'
when ex.PartyRelationshipType ='Other' then 'Loan-Other Rltp'
else null end as PartyRelationshipType,
case when ex.isActiveFlag=true then "Y"
else "N" end  AS isActiveFlag,
dp.PartyType,
case when ex.portfolioname='EUCLID' and dp.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
when ex.portfolioname='EUCLID' and dp.SubServicer is null then 'Cepal'
else null end as SubServicerFlag,
dp.HighBalanceFlag as HighBalanceFlag
FROM exposure ex
LEFT JOIN dparty dp
ON ex.CDI = dp.CDI
left join portfolio por
on LEFT(ex.exposurekey,3) = por.prefix
where ex.CDI IS NOT NULL
""")

target_table_path_BorrowerMapping = f"{target_area}.BorrowerMapping"

if spark.catalog.tableExists(target_table_path_BorrowerMapping):
    delete_from_unity_table(
    spark,
    target_table_path_BorrowerMapping,
    run_date,
    portfolio_filter,
)

BorrowerMapping = BorrowerMapping.withColumn("GroupID", col("GroupID").cast("string"))
BorrowerMapping = BorrowerMapping.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

BorrowerMapping.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_BorrowerMapping) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_BorrowerMapping} SYNC METADATA")
