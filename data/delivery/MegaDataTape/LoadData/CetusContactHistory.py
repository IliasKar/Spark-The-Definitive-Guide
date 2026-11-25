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

interaction = spark.sql(f'''
SELECT DISTINCT sourcesystem, InteractionTypeCode, IsFtFAppointmentFlag,
    IsCallFlag, IsRPCFlag, InteractionTypeKey
FROM `{env}-cdh`.primary.interaction
where Businessdate = (select max(Businessdate) from `{env}-cdh`.primary.interaction)
AND PortfolioName='Cetus' and sourcesystem='whl'
''')
interaction.createOrReplaceTempView("interaction")

portfolio = spark.sql(f'''
SELECT *
FROM `{env}-cdh`.primary.portfolio
WHERE PortfolioName='Cetus' and businessdate={run_date!r}
''')
portfolio.createOrReplaceTempView("portfolio")

whl_debtorstatus = spark.sql(f'''
SELECT *, SUBSTRING(DebtorStatusPrfCDI,1,3) prfx
FROM `{env}-cdh`.landed.whl_debtorstatus
where businessdate={run_date!r} AND SUBSTRING(DebtorStatusPrfCDI,1,3)='CTS'
''')
whl_debtorstatus.createOrReplaceTempView("whl_debtorstatus")

whl_debtorstatusaction = spark.sql(f'''
SELECT *
FROM `{env}-cdh`.landed.whl_debtorstatusaction
WHERE businessdate={run_date!r}
''')
whl_debtorstatusaction.createOrReplaceTempView("whl_debtorstatusaction")

whl_debtorstatusactionresult = spark.sql(f'''
SELECT *
FROM `{env}-cdh`.landed.whl_debtorstatusactionresult
WHERE businessdate={run_date!r}
''')
whl_debtorstatusactionresult.createOrReplaceTempView("whl_debtorstatusactionresult")

# COMMAND ----------

# ##CetusContactHistory

# COMMAND ----------

DebtorStatus = spark.sql(f'''
SELECT DISTINCT
    {run_date!r} AS ReportingDate,
    p.PortfolioId AS PortfolioId,
    p.PortfolioName AS PortfolioName,
    IFNULL(DS.DebtorStatusPrfCDI,'') AS DebtorStatusDataPrfCDI,
    IFNULL(DS.DebtorStatusPrfAFM,'') AS DebtorStatusDataPrfAFM,
    IFNULL(DS.DebtorStatusPrfGroupID,'') AS DebtorStatusDataPrfGroupID,
    IFNULL(DS.DebtorStatusPrfGroupDescription,'') AS DebtorStatusDataPrfGroupDescription,
    IFNULL(DS.DebtorStatusPrfCustomerName,'') AS DebtorStatusDataPrfsCustomerName,
    IFNULL(DS.DebtorStatusDataStatusValue,'') AS DebtSt_Label,
    IFNULL(SUBSTRING(DS.DebtorStatusStatusComments,0,255),'') AS DebtorStatusDataStatusComments,
    IFNULL(DS.DebtorStatusNextActionValue,'') AS NxtAction_Label,
    IFNULL(SUBSTRING(DS.DebtorStatusNextActionComments,0,255),'') AS DebtorStatusDataNextActionComments,
    IFNULL(DS.DebtorStatusCurrentStrategyValue,'') AS CurStrategy_Label,
    IFNULL(SUBSTRING(DS.DebtorStatusCurrentStrategyComments,0,255),'') AS DebtorStatusDataCurrentStrategyComments,
    IFNULL(DS.DebtorStatusAssignedAgent,'') AS DebtorStatusDataAssignedAgent,
    IFNULL(DSA.SubCategoryDescription,'') AS SubCategoryDescription,
    IFNULL(DSA.CategoryDescription,'') AS CategoryDescription,
    CASE WHEN ACT.IsFtFAppointmentFlag = 'true' THEN 1 ELSE 0 END AS IsFtFAppointment,
    CASE WHEN ACT.IsCallFlag = 'true' THEN 1 ELSE 0 END AS IsCall,
    CASE WHEN ACT.IsRPCFlag = 'true' THEN 1 ELSE 0 END AS IsRPC,
    IFNULL(DSA.DebtorStatusActionCommunicationPhoneNumber,'') AS DebtorStatusDataCommunicationPhoneNumber,
    IFNULL(CAST(DSA.DebtorStatusActionDate AS STRING),'') AS DebtorStatusDataActDate,
    IFNULL(SUBSTRING(DSA.DebtorStatusActionComments,0,255),'') AS DebtorStatusDataActionComments,
    IFNULL(CAST(DSA.DebtorStatusActionDueDate AS STRING), '') AS DebtorStatusDataActionDueDate,
    IFNULL(DSAR.ActionResultDescription,'') AS DebtorStatusDataResultLabel,
    IFNULL(CAST(DSAR.DebtorStatusActionResultDate AS STRING),'') AS DebtorStatusDataResultDate,
    IFNULL(SUBSTRING(DSAR.DebtorStatusActionResultComments,0,255),'') AS DebtorStatusDataResultComments,
    IFNULL(DSA.DebtorStatusActionModifiedByUser,'') AS DebtorStatusDataModifiedByUser,
    IFNULL(CAST(DSA.DebtorStatusActionModifiedDate AS STRING), '') AS DebtorStatusDataModifiedDate,
    CASE WHEN ((ACT.InteractionTypeKey IN ('0067','0068','0069')) AND (DSAR.DebtorStatusActionResultCodeID IN (4,19,20,21,22,23,24,39,40))) THEN 1
         ELSE 0 END AS AppointmentPerformed
FROM whl_debtorstatus DS
INNER JOIN (
    select PortfolioId, whlPortfolioID, PortfolioName
    from portfolio p
    join (select distinct PortfolioID whlPortfolioID, prfx from whl_debtorstatus) s on p.Prefix = s.prfx
) p on p.whlPortfolioID = DS.PortfolioID
LEFT JOIN whl_debtorstatusaction DSA ON DSA.DebtorStatusID = DS.DebtorStatusID
LEFT JOIN interaction ACT ON ACT.InteractionTypeCode = CAST(DSA.DebtorStatusActionSubCatID AS STRING)
LEFT JOIN whl_debtorstatusactionresult DSAR ON DSAR.DebtorStatusActionResultActionID = DSA.DebtorStatusActionID
''')

DebtorStatus.createOrReplaceTempView("DebtorStatus")

# COMMAND ----------

ContactHistory = spark.sql(f'''
SELECT DISTINCT
    CAST(ReportingDate AS DATE) ReportingDate,
    CAST(PortfolioId AS INT) AS PortfolioId,
    CAST(PortfolioName AS STRING) AS PortfolioName,
    CAST(DebtorStatusDataPrfCDI AS STRING) AS DebtorStatusDataPrfCDI,
    CAST(DebtorStatusDataPrfAFM AS STRING) AS DebtorStatusDataPrfAFM,
    CAST(DebtorStatusDataPrfGroupID AS STRING) AS DebtorStatusDataPrfGroupID,
    CAST(DebtorStatusDataPrfGroupDescription AS STRING) AS DebtorStatusDataPrfGroupDescription,
    CAST(DebtorStatusDataPrfsCustomerName AS STRING) AS DebtorStatusDataPrfsCustomerName,
    CAST(DebtSt_Label AS STRING) AS DebtSt_Label,
    CAST(DebtorStatusDataStatusComments AS STRING) AS DebtorStatusDataStatusComments,
    CAST(NxtAction_Label AS STRING) AS NxtAction_Label,
    CAST(DebtorStatusDataNextActionComments AS STRING) AS DebtorStatusDataNextActionComments,
    CAST(CurStrategy_Label AS STRING) AS CurStrategy_Label,
    CAST(DebtorStatusDataCurrentStrategyComments AS STRING) AS DebtorStatusDataCurrentStrategyComments,
    CAST(DebtorStatusDataAssignedAgent AS STRING) AS DebtorStatusDataAssignedAgent,
    CAST(SubCategoryDescription AS STRING) AS SubCategoryDescription,
    CAST(CategoryDescription AS STRING) AS CategoryDescription,
    CAST(IsFtFAppointment AS INT) IsFtFAppointment,
    CAST(IsCall AS INT) IsCall,
    CAST(IsRPC AS INT) IsRPC,
    CAST(DebtorStatusDataCommunicationPhoneNumber AS STRING) DebtorStatusDataCommunicationPhoneNumber,
    CAST(DebtorStatusDataActDate AS STRING) DebtorStatusDataActDate,
    CAST(DebtorStatusDataActionComments AS STRING) AS DebtorStatusDataActionComments,
    CAST(DebtorStatusDataActionDueDate AS STRING) AS DebtorStatusDataActionDueDate,
    CAST(DebtorStatusDataResultLabel AS STRING) AS DebtorStatusDataResultLabel,
    CAST(DebtorStatusDataResultDate AS STRING) AS DebtorStatusDataResultDate,
    CAST(DebtorStatusDataResultComments AS STRING) AS DebtorStatusDataResultComments,
    CAST(DebtorStatusDataModifiedByUser AS STRING) AS DebtorStatusDataModifiedByUser,
    CAST(DebtorStatusDataModifiedDate AS STRING) DebtorStatusDataModifiedDate,
    CAST(AppointmentPerformed AS INT) AppointmentPerformed
FROM DebtorStatus
''')

target_table_path_ContactHistory = f"{target_area}.CetusContactHistory"

if spark.catalog.tableExists(target_table_path_ContactHistory):
    delete_from_unity_table(spark, target_table_path_ContactHistory, run_date, portfolio_filter)

ContactHistory = ContactHistory.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

ContactHistory.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_ContactHistory)

spark.sql(f"REPAIR TABLE {target_table_path_ContactHistory} SYNC METADATA")
