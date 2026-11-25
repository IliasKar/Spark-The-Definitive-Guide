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


dunderlaw = spark.sql(f"""
                      select *
                      from `{env}-cdh`.derivative.dunderlaw
                      where BusinessDate = {run_date!r} 
                      """)

dunderlaw.createOrReplaceTempView("dunderlaw")

nacecode = spark.sql(f"""
                     select * 
                     from `{env}-cdh`.reference.nacecode
                      """)

nacecode.createOrReplaceTempView("nacecode")

interaction = spark.sql(f"""
                        select *
                        from `{env}-cdh`.primary.interaction
                        where Businessdate = (select max(Businessdate) from `{env}-cdh`.primary.interaction)
                        --where BusinessDate = {run_date!r}   -- EK 20260309 update for interaction table historical data
                        AND YEAR(InteractionDate) = YEAR(DATE('{run_date}'))
                        AND MONTH(InteractionDate) = MONTH(DATE('{run_date}'))
                        AND {portfolio_filter}
                        """)

interaction.createOrReplaceTempView("interaction")

exposure = spark.sql(f"""
                    SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.primary.get_all_exposures(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                    """)

exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

# #Borrower

# COMMAND ----------

person_cdi = spark.sql(f"""
SELECT CDI,
       MAX(SourceSystem) AS SourceSystem,
       --SUM(CASE WHEN IsRPCflag = true THEN 1 ELSE 0 END) AS RightPersonContacts,
       SUM(CASE WHEN IsRPCflagCalc = true THEN 1 ELSE 0 END) AS RightPersonContacts, -- EK 20260309
       SUM(CASE WHEN IsPositiveFlag = true THEN 1 ELSE 0 END) AS PositiveRightPersonContacts,
       SUM(CASE WHEN IsPromiseFlag = true THEN 1 ELSE 0 END) AS NumberofPromises,
       SUM(CASE WHEN ContactType in ('RIGHT PARTY CONTACT', 'Wrong Person Contact', 'No Contact', 'Right Party Contact', 'Right Person Contact') THEN 1 ELSE 0 END) AS NumberofContactActivities -- EK 20260122
FROM interaction
GROUP BY CDI
""")

person_cdi.createOrReplaceTempView("person_cdi")

Borrower = spark.sql(f"""
SELECT distinct
dpa.BusinessDate as ReportingDate,
exposure.PortfolioName,
dpa.CDI,
(case when dpa.PortfolioName like 'Hermes%' then CAST(dpa.GroupCode AS STRING)            -- EK 20260309                       
    when dpa.PortfolioName='Cetus' then  CAST(dpa.GroupCodePortfolio AS STRING) end) as GroupID,
dpa.PartyType,
case when dpa.l3869flag=true then "Y" 
    else  "N" end L3869Flag, 
dun.CalcLawStatus, 
CASE
    WHEN dpa.PartyType = "Individual" THEN                 ---might need to implement this transformation in dparty
        CASE
            WHEN dpa.Gender = "Male" THEN "M"
            ELSE "F"
        END
    ELSE NULL 
END AS Gender,
dpa.DeceasedFlag,
CASE 
    WHEN dpa.BirthDate <> "1753-01-01" THEN dpa.BirthDate
    ELSE NULL END AS BirthDate,                             ---might need to implement this transformation in dparty
dpa.NationalityENG,  
dpa.MaritalStatusENG,
nc.Nace3lvlDescr,-- dpa.EconomicActivityCode, 
dpa.PartyRegion, 
case 
    when dpa.PartyRegion is NULL then NULL
    else dpa.PartyPostalCode END PartyPostalCode, 
per.RightPersonContacts, 
per.PositiveRightPersonContacts,
per.NumberofPromises, 
per.NumberofContactActivities,
case when dpa.PortfolioName ilike 'Hermes%' or dpa.PortfolioName ilike '%Cetus%' then dpa.RiskDescription else null end as AMLFlag,
case when dpa.portfolioname='EUCLID' and dpa.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
    when dpa.portfolioname='EUCLID' and dpa.SubServicer is null then 'Cepal'
    else null end as SubServicerFlag,
dpa.HighBalanceFlag
FROM dparty dpa
LEFT JOIN dunderlaw dun
ON  dpa.CDI = dun.CDI
and dun.UnderLawType='N3869'
LEFT JOIN nacecode nc
ON dpa.EconomicActivityCode = nc.nacecodeid
INNER JOIN exposure
on dpa.CDI = exposure.CDI
LEFT JOIN person_cdi per
ON per.CDI = dpa.CDI
where exposure.CDI IS NOT NULL
""")

target_table_path_Borrower = f"{target_area}.Borrower"

if spark.catalog.tableExists(target_table_path_Borrower):
    delete_from_unity_table(
    spark,
    target_table_path_Borrower,
    run_date,
    portfolio_filter,
)


Borrower = Borrower.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

Borrower.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_Borrower) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_Borrower} SYNC METADATA")
