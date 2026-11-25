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

dexposure = spark.sql(f"""
                      SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                      """)

dexposure.createOrReplaceTempView("dexposure")

exposure = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.primary.get_all_exposures(DATE({run_date!r}))
                   where  sourcesystem='profits'
                   AND {portfolio_filter}
                      """)
exposure.createOrReplaceTempView("exposure")

dparty = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} and sourcesystem='profits'
                   AND {portfolio_filter}
                      """)

dparty.createOrReplaceTempView("dparty")

highbalance_by_cdi = spark.sql(f"""
                      SELECT CDI,
                             MAX(HighBalanceFlag) AS IsHighBalanceFlag
                      FROM dparty
                      GROUP BY CDI
                      """)

highbalance_by_cdi.createOrReplaceTempView("highbalance_by_cdi")

interaction = spark.sql(f"""
                        select *,cast({run_date!r} as date) AS ReportingDate
                        from `{env}-cdh`.primary.interaction
                        --where BusinessDate = {run_date!r}
                        where Businessdate = (select max(Businessdate) from `{env}-cdh`.primary.interaction) -- EK 20260309
                        AND IsCallFlag = TRUE --bringing only calls and no other contact 
                        AND {portfolio_filter}
                        """)

interaction.createOrReplaceTempView("interaction")

firmstructure = spark.sql(f"""
                        select *
                        from `{env}-cdh`.primary.firmstructure
                        where BusinessDate = {run_date!r}
                        """)
firmstructure.createOrReplaceTempView("firmstructure")

# COMMAND ----------

# ##ContactHistory 

# COMMAND ----------

ContactHistory = spark.sql(f"""
SELECT DISTINCT
    i.ReportingDate,
    i.PortfolioName,
    i.InteractionKey,
    i.ExposureKey,
    i.CDI,
    CASE 
        WHEN dp.PortfolioName LIKE 'Hermes%' THEN CAST(dp.GroupCode AS STRING)    -- EK 20260309
        WHEN dp.PortfolioName = 'Cetus' THEN CAST(dp.GroupCodePortfolio AS STRING)
    END AS GroupID,
    CAST(i.InteractionDate AS DATE) AS InteractionDate,
    i.ContactType,
    i.InteractionType,
    CASE 
        WHEN i.PortfolioName = 'EUCLID' 
             AND dp.SubServicer = 'EUCLID DV01 - ASSIGN FOR SERVICING' THEN 'DV01'
        WHEN i.PortfolioName = 'EUCLID'
             AND dp.SubServicer IS NULL THEN 'Cepal'
        ELSE NULL
    END AS SubServicerFlag,
    i.IsHighBalanceFlag AS HighBalanceFlag,
    i.PartyTaxRegistrationNumber,
    dp.GroupName,
    dp.LegalNameGR,
    i.IsFtFAppointmentFlag,
    i.IsCallFlag,
    i.IsRPCFlag,
    i.DebtorStatus,
    i.DebtorStatusComments,
    i.CurrentStrategy,
    i.CategoryOfAction,
    i.SubCategoryOfAction,
    i.Comments,
    i.ResultComments,
    fs.EmployeeUsername,
    i.InteractionDate AS DateOfAction
FROM (
    SELECT
        i.ReportingDate,
        i.PortfolioName,
        i.InteractionKey,
        ex.ExposureKey,
        i.CDI,
        i.InteractionDate,
        i.ContactType,
        i.InteractionType,
        hb.IsHighBalanceFlag,
        ex.PartyTaxRegistrationNumber,
        i.IsFtFAppointmentFlag,
        i.IsCallFlag,
        i.IsRPCFlagCalc as IsRPCFlag,   -- EK 20260309
        i.DebtorStatus,
        i.DebtorStatusComments,
        i.CurrentStrategy,
        i.CategoryOfAction,
        i.SubCategoryOfAction,
        i.Comments,
        i.ResultComments,
        i.CreatedBy
    FROM interaction i
    LEFT JOIN exposure ex
      ON ex.CDI = i.CDI
    LEFT JOIN highbalance_by_cdi hb
      ON hb.CDI = i.CDI
    WHERE i.SourceSystem = 'whl'
    
    UNION ALL
    
    SELECT
        i.ReportingDate,
        i.PortfolioName,
        i.InteractionKey,
        coalesce(dex.exposurekey,concat(left(i.CDI,3),i.ExposureKey)),
        i.CDI,
        i.InteractionDate,
        i.ContactType,
        i.InteractionType,
        dex.IsHighBalanceFlag,
        dex.PartyTaxRegistrationNumber,
        i.IsFtFAppointmentFlag,
        i.IsCallFlag,
        i.IsRPCFlagCalc as IsRPCFlag,   -- EK 20260309
        i.DebtorStatus,
        i.DebtorStatusComments,
        i.CurrentStrategy,
        i.CategoryOfAction,
        i.SubCategoryOfAction,
        i.Comments,
        i.ResultComments,
        i.CreatedBy
    FROM interaction i
    LEFT JOIN dexposure dex
      ON dex.ExposureKey = CONCAT(LEFT(i.CDI,3), i.ExposureKey)
    WHERE i.SourceSystem <> 'whl'
) i
LEFT JOIN dparty dp
  ON dp.CDI = i.CDI
LEFT JOIN firmstructure fs
  ON fs.EmployeePartyKey = i.CreatedBy
WHERE CONCAT(YEAR(i.InteractionDate), '-', MONTH(i.InteractionDate)) = 
      CONCAT(YEAR(DATE({run_date!r})), '-', MONTH(DATE({run_date!r})))
""")

# COMMAND ----------

target_table_path_ContactHistory = f"{target_area}.ContactHistory"

if spark.catalog.tableExists(target_table_path_ContactHistory):
    delete_from_unity_table(
    spark,
    target_table_path_ContactHistory,
    run_date,
    portfolio_filter,
)

ContactHistory = ContactHistory.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

ContactHistory.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_ContactHistory) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_ContactHistory} SYNC METADATA")
