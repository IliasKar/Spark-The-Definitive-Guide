# Databricks notebook source
# MAGIC %md
# MAGIC # 📑 Backstop Report
# MAGIC  
# MAGIC ## 🔗 Links
# MAGIC - **[BRD Backstop Light/Orbit/Cell](https://cepalhellas.atlassian.net/wiki/x/DAHhDQ)**  
# MAGIC   Comprehensive documentation outlining the business requirements for this report.
# MAGIC  
# MAGIC - **[Story Ticket](https://cepalhellas.atlassian.net/browse/DBTC-301)**  
# MAGIC   Details about the story ticket, including tasks and progress.
# MAGIC  
# MAGIC - **[UAT Findings Ticket](https://cepalhellas.atlassian.net/browse/DBTC-947)**  
# MAGIC   A summary of findings from UAT.
# MAGIC  
# MAGIC ---
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/Backstop/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname

env_prefix = get_env_prefix()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters
# MAGIC **report_args:** it is a json which contains all the appropriate parameters to run the report
# MAGIC `
# MAGIC {  
# MAGIC     "portfolio_name": "Orbit",
# MAGIC     "output_file_prefix": "Orbit_Backstop_",
# MAGIC     "report_output": "/Volumes/qa-cdh-delivery/backstop/orbit/"
# MAGIC }
# MAGIC
# MAGIC **run_date:** is the date value to run the report. The run_date is empty when it is called by the orchestrator and that means "today"

# COMMAND ----------

from typing import Any
from delta import DeltaTable  # type: ignore
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
    regexp_replace
)

import os

env = env_prefix

from typing import Optional
import delta  # type: ignore
from pyspark.sql import SparkSession
from io import BytesIO
import pandas as pd
import json
from datetime import datetime
from pytz import timezone

# COMMAND ----------

athens_tz = timezone('Europe/Athens')
try:    
    report_args = dbutils.widgets.get("report_args")
    report_args = report_args.replace("'", '"')
except Exception as e:
    raise ValueError("Missing required input: report_args")
try:    
    report_name = dbutils.widgets.get("report_name")
except Exception as e:
    raise ValueError("Missing required input: report_name")
print(f"Report {report_name} is started")

run_date_raw = dbutils.widgets.get("run_date").strip()
if run_date_raw:
    run_date = run_date_raw
    run_date_str = run_date.replace('-', '')
    referenceDate = run_date
else:
    run_date = datetime.now(athens_tz).strftime('%Y-%m-%d')
    run_date_str = datetime.now(athens_tz).strftime('%Y%m%d')
    referenceDate = spark.sql(f"SELECT CurrentMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate = last_day(add_months(to_date('{run_date}'), -1))").first()["CurrentMonthLastWorkingDate"]
    referenceDate = referenceDate.strftime('%Y-%m-%d')
now_datetime_str = datetime.now(athens_tz).strftime('%Y%m%dT%H%M%S')

# Read JSON
json_data = json.loads(report_args)
# Access the values and store them into variables
if "portfolio_name" not in json_data: raise KeyError("'portfolio_name' key is missing in the report_args JSON data.")
if "output_file_prefix" not in json_data: raise KeyError("'output_file_prefix' key is missing in the report_args JSON data.")
if "report_output" not in json_data: raise KeyError("'report_output' key is missing in the report_args JSON data.")
portfolioName = "".join(json_data["portfolio_name"])
output_file_prefix = "".join(json_data["output_file_prefix"])
report_output = "".join(json_data["report_output"])
# Resolve full export path from SSM if relative (same pattern as MegaDataTape)
if not report_output.startswith("/Volumes"):
    from aws_utils import get_scope_key_names, get_aws_credentials, connect_to_aws_and_access_ssm
    import boto3
    _secrets_scope = "aws-databricks"
    _aws_keys = get_scope_key_names(_secrets_scope)
    _aws_key = _aws_keys[0] if _aws_keys else None
    _aws_secret_key = get_aws_credentials(_secrets_scope, _aws_key)
    _ssm_client = boto3.client("ssm", region_name="eu-central-1",
                               aws_access_key_id=_aws_key,
                               aws_secret_access_key=_aws_secret_key)
    export_base_path = connect_to_aws_and_access_ssm(
        "/reports/exports/s3_main_path", _ssm_client, _aws_secret_key
    ).replace("envprefix", env)
    report_output = export_base_path + report_output
print(f"Report {report_name} is running with the following arguments: report_args = {report_args}, report_name = {report_name}, portfolio_name = {portfolioName}, output_file_prefix = {output_file_prefix}, run_date = {run_date}")
print(f"now is: {now_datetime_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Tables

# COMMAND ----------

# hoist_secure_cdi should be uploaded in the {env}-reportinputfiles catalog
# orbit_ad_hoc_migration_perimeter should be uploaded in the {env}-reportinputfiles catalog
# orbit_secured_borrower should be uploaded in the {env}-reportinputfiles catalog

if portfolioName != 'Orbit':

  BackstopUnsecured = spark.sql(f"""
  SELECT DISTINCT  
  de.exposurekey as DebtId
  ,substring(de.exposurekey, 4) AS OriginalDebtId
  ,de.Portfolioname AS Portfolio
  ,'UNSECURED' AS AssetClass
  ,a.issuedate AS OriginationDate 
  ,de.defaultdate 
  ,de.currency
  ,de.InitialAssignmentAmount AS TotalClaimAmount
  ,de.OutstandingTotalBalance AS RemainingTotalClaim
  ,de.businessdate AS RemainingTotalClaimDate
  FROM `{env}-cdh`.derivative.dexposure de
  LEFT JOIN `{env}-cdh`.primary.agreement a ON de.agreementkey = a.agreementkey
  where 1=1
  AND de.portfolioname = '{portfolioName}'
  AND de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and a.SourceSystem = 'profits'
  AND de.ProductKey not in ('41305;3121','41305;3127','41305;3120')
  AND de.CDI NOT IN (SELECT CDI FROM `{env}-reportinputfiles`.investor.Hoist_SecureCDI)
  AND de.isactiveflag = TRUE
  AND a.issuedate >= '2019-04-25'
  """)

  BackstopUnsecured.createOrReplaceTempView("BackstopUnsecured")

  BackstopSecured = spark.sql(f"""
  SELECT DISTINCT  
  de.exposurekey AS DebtId
  ,substring(de.exposurekey, 4) as OriginalDebtId
  ,de.Portfolioname AS Portfolio
  ,'SECURED' AS AssetClass
  ,a.issuedate AS OriginationDate 
  ,de.defaultdate 
  ,de.currency
  ,de.InitialAssignmentAmount AS TotalClaimAmount
  ,de.OutstandingTotalBalance AS RemainingTotalClaim
  ,de.businessdate AS RemainingTotalClaimDate
  FROM `{env}-cdh`.derivative.dexposure de
  LEFT JOIN `{env}-cdh`.primary.agreement a ON de.agreementkey = a.agreementkey
  WHERE 1=1
  AND de.portfolioname = '{portfolioName}'
  AND de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and a.SourceSystem = 'profits'
  AND de.ProductKey NOT IN ('41305;3121','41305;3127','41305;3120')
  AND de.CDI IN (SELECT CDI FROM `{env}-reportinputfiles`.investor.Hoist_SecureCDI)
  AND de.isactiveflag = TRUE
  """)

  BackstopSecured.createOrReplaceTempView("BackstopSecured")

  FINREPSecured = spark.sql(f"""
  select 
  de.cdi as CustomerID
  ,substring(de.cdi, 4) as OriginalCustomerID
  ,de.exposurekey DebtID
  from `{env}-cdh`.derivative.dexposure de
  inner join `{env}-reportinputfiles`.investor.Hoist_SecureCDI sec on de.CDI = sec.CDI
  where 1=1
  and de.portfolioname = '{portfolioName}'
  and de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and de.ProductKey not in ('41305;3121','41305;3127','41305;3120')
  and de.isactiveflag = true
  """)

  FINREPSecured.createOrReplaceTempView("FINREPSecured")

  BackstopFull = spark.sql(f"""
      select * from BackstopUnsecured
      UNION ALL
      SELECT * FROM BackstopSecured
  """)

  BackstopFull.createOrReplaceTempView("BackstopFull")

else:
  
  DefaultDate = spark.sql(f"""
    SELECT
    CONCAT('ORB', `COD_ACCOUNT`) AS AccountNumber,
    CAST(
      CASE
        WHEN `DEFAULT_DATE_28_2_22` <> '' THEN
          CONCAT(
            RIGHT(`DEFAULT_DATE_28_2_22`, 4), '-',
            CASE
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Ιαν' THEN '01'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Φεβ' THEN '02'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Μαρ' THEN '03'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Απρ' THEN '04'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Μαϊ' THEN '05'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Ιουν' THEN '06'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Ιουλ' THEN '07'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Αυγ' THEN '08'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Σεπ' THEN '09'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Οκτ' THEN '10'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Νοε' THEN '11'
              WHEN SUBSTRING(`DEFAULT_DATE_28_2_22`, 3, LENGTH(`DEFAULT_DATE_28_2_22`) - 6) = 'Δεκ' THEN '12'
            END, '-',
            LEFT(`DEFAULT_DATE_28_2_22`, 2)
          )
      END AS DATE
    ) AS DefaultDate
  from `{env}-reportinputfiles`.investor.Orbit_AdHoc_MigrationPerimeter
  """)
 
  DefaultDate.createOrReplaceTempView("DefaultDate")

  Restructuring = spark.sql(f"""
      SELECT distinct OldExposureKey, NewExposureKey
      from `{env}-cdh`.primary.restructuring
    """)
  
  Restructuring.createOrReplaceTempView("Restructuring")

  ExtraRelations = spark.sql(f"""
    SELECT CONCAT('ORB', `ΑΡΙΘΜΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ`) as AccountNo,  CONCAT('ORB', `ΛΟΓΑΡΙΑΣΜΟΣ_ΟΥΣΙΑΣ`) as RelatedAccountNo
    from `{env}-reportinputfiles`.investor.orbit_extrarelationships
  """)
  
  ExtraRelations.createOrReplaceTempView("ExtraRelations")

  TranDTRelationships = spark.sql(f"""
    SELECT CONCAT('ORB', `Cut-off_view`) as Original_Account --  CONCAT('ORB', `Cutoff_311220`) as Original_Account
    , CONCAT('ORB', `New_Account_No_18_3_22_view`) as New_Account
    from `{env}-reportinputfiles`.investor.orbit_oma_cmsperimeter
    WHERE CAST(`Cut-off_view` AS STRING) <> 'REF!'    -- ek20260708
    union all
    SELECT CONCAT('ORB', `Cut-off_view_31_12_20`) as Original_Account, CONCAT('ORB', `New_Account_No_18_3_22_view`) as New_Account
    from `{env}-reportinputfiles`.investor.orbit_oma_sglperimeter
    WHERE CAST(`Cut-off_view_31_12_20` AS STRING) <> 'REF!'   -- ek20260708
    """)
  
  TranDTRelationships.createOrReplaceTempView("TranDTRelationships")

  TransactionPerimeneter = spark.sql(f"""
    SELECT CONCAT('ORB', Account_No) as AccountNo, 
    CAST(REPLACE(Total_Exposure, ',', '.') AS DECIMAL(19,2)) AS TotalExposure
    from `{env}-reportinputfiles`.investor.orbit_transactionperimeter
  """)

  TransactionPerimeneter.createOrReplaceTempView("TransactionPerimeneter")

  BackstopUnsecured = spark.sql(f"""
  SELECT DISTINCT  
  de.exposurekey AS DebtId
  ,substring(de.exposurekey, 4) as OriginalDebtId 
  -- 'OriginalDebtId' as OriginalDebtId
  ,de.Portfolioname AS Portfolio
  ,CASE
    WHEN de.CDI IN (SELECT CDI FROM `{env}-reportinputfiles`.investor.Hoist_SecureCDI) THEN 'Secured'
    ELSE 'Unsecured'
  END AS AssetClass
  ,a.issuedate AS originationdate
  ,MAX(df.defaultdate) OVER (PARTITION BY de.ExposureKey) as DefaultDate
  ,de.currency
  ,coalesce(MAX(tranp.TotalExposure) OVER (PARTITION BY de.ExposureKey), de.InitialAssignmentAmount) AS TotalClaimAmount
--   ,de.InitialAssignmentAmount AS TotalClaimAmount
  ,de.OutstandingTotalBalance AS RemainingTotalClaim
  ,de.businessdate AS RemainingTotalClaimDate
  FROM `{env}-cdh`.derivative.dexposure de
  LEFT JOIN `{env}-cdh`.primary.agreement a ON de.agreementkey = a.agreementkey
  LEFT JOIN Restructuring res ON de.ExposureKey = res.NewExposureKey
  LEFT JOIN ExtraRelations extra ON COALESCE(res.OldExposureKey,de.exposurekey) = extra.AccountNo
  LEFT JOIN TranDTRelationships tranrel ON COALESCE(extra.RelatedAccountNo,res.OldExposureKey) = tranrel.New_Account
  LEFT JOIN TransactionPerimeneter tranp ON COALESCE(tranrel.Original_Account,extra.RelatedAccountNo, res.OldExposureKey,de.ExposureKey) = tranp.AccountNo
  LEFT JOIN DefaultDate df ON  COALESCE(res.OldExposureKey,de.exposurekey) = df.accountnumber
  WHERE 1=1
  AND de.portfolioname = '{portfolioName}'
  AND de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and a.SourceSystem = 'profits'
  AND de.ProductKey NOT IN ('41305;3121','41305;3127','41305;3120')
  AND de.CDI NOT IN (SELECT CDI FROM `{env}-reportinputfiles`.investor.Hoist_SecureCDI)
  AND de.isactiveflag = TRUE
  AND a.issuedate >= '2019-04-25'
  """)
 
  BackstopUnsecured.createOrReplaceTempView("BackstopUnsecured")

  BackstopSecured = spark.sql(f"""
  SELECT DISTINCT  
  de.exposurekey as DebtId
  ,substring(de.exposurekey, 4) as OriginalDebtId 
  -- 'OriginalDebtId' as OriginalDebtId
  ,de.Portfolioname as Portfolio
  ,case
    when de.CDI IN (select cdi from `{env}-reportinputfiles`.investor.Hoist_SecureCDI) Then 'Secured'
    Else 'Unsecured'
  End as AssetClass
  ,a.issuedate as originationdate
  ,MAX(df.defaultdate) OVER (PARTITION BY de.ExposureKey) as DefaultDate
  ,de.currency
  ,coalesce(MAX(tranp.TotalExposure) OVER (PARTITION BY de.ExposureKey), de.InitialAssignmentAmount) AS TotalClaimAmount
  ,de.OutstandingTotalBalance as RemainingTotalClaim
  ,de.businessdate as RemainingTotalClaimDate
  from `{env}-cdh`.derivative.dexposure de
  inner join `{env}-reportinputfiles`.investor.Hoist_SecureCDI sec on de.cdi = sec.cdi
  left join `{env}-cdh`.primary.agreement a on de.agreementkey = a.agreementkey
  LEFT JOIN Restructuring res ON de.ExposureKey = res.NewExposureKey
  LEFT JOIN ExtraRelations extra ON COALESCE(res.OldExposureKey,de.exposurekey) = extra.AccountNo
  LEFT JOIN TranDTRelationships tranrel ON COALESCE(extra.RelatedAccountNo,res.OldExposureKey) = tranrel.New_Account
  LEFT JOIN TransactionPerimeneter tranp ON COALESCE(tranrel.Original_Account,extra.RelatedAccountNo, res.OldExposureKey,de.ExposureKey) = tranp.AccountNo
  LEFT JOIN DefaultDate df ON  COALESCE(res.OldExposureKey,de.exposurekey) = df.accountnumber  where 1=1
  AND de.portfolioname = '{portfolioName}'
  AND de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and a.SourceSystem = 'profits'
  and de.ProductKey NOT IN ('41305;3121','41305;3127','41305;3120')
  and de.isactiveflag = true
  """)
 
  BackstopSecured.createOrReplaceTempView("BackstopSecured")

  FINREPSecured = spark.sql(f"""
  select 
  sec.cdi as CustomerID
  , cast(substring(sec.cdi,4) as int) as OriginalCustomerID 
  -- 'OriginalCustomerID' as OriginalCustomerID
  ,exposurekey DebtID
  from `{env}-cdh`.derivative.dexposure de
  inner join `{env}-reportinputfiles`.investor.Hoist_SecureCDI sec on de.cdi = sec.cdi
  where 1=1
  and de.portfolioname = '{portfolioName}'
  and de.BusinessDate = '{referenceDate}'
  and de.SourceSystem = 'profits'
  and de.ProductKey not in ('41305;3121','41305;3127','41305;3120')
  and de.isactiveflag = true
  """)

  FINREPSecured.createOrReplaceTempView("FINREPSecured")

  BackstopFull = spark.sql(f"""
      SELECT * FROM BackstopUnsecured
      UNION ALL
      SELECT * FROM BackstopSecured
  """)

  BackstopFull.createOrReplaceTempView("BackstopFull")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Export

# COMMAND ----------

from export_utils import export_data
from helpers import archive_existing_files
from pyspark.sql import functions as F

df_BackstopSecured = BackstopSecured.toPandas()
df_BackstopUnSecured = BackstopUnsecured.toPandas()
df_FINREPSecured = FINREPSecured.toPandas()

output_buffer = BytesIO()
with pd.ExcelWriter(output_buffer) as writer:
    df_BackstopSecured.to_excel(writer, sheet_name=f"Backstop Sec. {portfolioName}", index=False)
    df_BackstopUnSecured.to_excel(writer, sheet_name=f"Backstop Unsec. {portfolioName}", index=False)
    df_FINREPSecured.to_excel(writer, sheet_name=f"FINREP Sec. {portfolioName}", index=False)

output_path = f"{report_output}{portfolioName}/{output_file_prefix}{referenceDate}.xlsx"

# Archive existing files before writing new export
os.makedirs(os.path.dirname(output_path), exist_ok=True)
archive_existing_files(os.path.dirname(output_path))

with open(output_path, "wb") as f:
    f.write(output_buffer.getvalue())
