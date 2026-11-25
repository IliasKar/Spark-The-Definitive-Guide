# Databricks notebook source
# MAGIC %md
# MAGIC # 📑 Dialer Manual Campaign Report
# MAGIC  
# MAGIC ## 🔗 Links
# MAGIC - **[BRD Dialer Manual Campaign](https://cepalhellas.atlassian.net/wiki/spaces/DBTC/pages/291438665/BRD+-+Dialer+manual+campaign+Report+Databricks)**  
# MAGIC   Comprehensive documentation outlining the business requirements for this report.
# MAGIC  
# MAGIC - **[Story Ticket]([DBTC-1134] BI_UAT Dialer Manual Campaign Report - Jira)**  
# MAGIC   Details about the story ticket, including tasks and progress.
# MAGIC  
# MAGIC - **[UAT Findings Ticket]([DBTC-1134] BI_UAT Dialer Manual Campaign Report - Jira)**  
# MAGIC   A summary of findings from UAT.
# MAGIC  
# MAGIC ---
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl
# MAGIC %pip install xlsxwriter
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/DialerManualCampaign/", 1)[0]
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
# MAGIC     "portfolio_name": "",
# MAGIC     "output_file_prefix": "DialerManualCampaign_",
# MAGIC     "report_output": "/Volumes/qa-cdh-delivery/internalreports/dialermanualcampaign/"
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
else:
    run_date = spark.sql(f"SELECT MAX(BusinessDate) FROM `{env}-cdh`.derivative.dexposure").collect()[0][0].strftime("%Y-%m-%d")
run_date_str = run_date.replace('-', '')
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
# MAGIC # df_dialer

# COMMAND ----------

df_dialer = spark.sql(
f'''
WITH 
report_date AS (
  SELECT
    {run_date!r} AS BusinessDate --MAX(BusinessDate) AS BusinessDate
  -- FROM
  --   `{env}-cdh`.derivative.dparty
),
FilteredDparty AS (
  SELECT
    PortfolioName,
    CDI,
    PartyKeyQCCepal AS CustomerID,
    BusinessDate
  FROM
    `{env}-cdh`.derivative.dparty
  WHERE
    BusinessDate
      = (
        SELECT
          BusinessDate
        FROM
          report_date
      )
),
FilteredDexposure AS (
  SELECT
    CDI,
    CaseCode,
    PortfolioName,
    BusinessDate
  FROM
    `{env}-cdh`.derivative.dexposure
  WHERE
    BusinessDate
      = (
        SELECT
          BusinessDate
        FROM
          report_date
      )
),
-- 1) Εξάγουμε όλα τα τηλέφωνα που ικανοποιούν τα κριτήριά μας. Από αυτά, τα work τηλέφωνα σηματοδοτούνται με 1 στο πεδίο is_work.
exploded_phones AS (
  SELECT
    CDI,
    BusinessDate,
    regexp_replace(trim(phone), '[^0-9]', '') AS cleaned_phone,
    CASE
      WHEN lower(coalesce(OriginalTelephoneType, '')) LIKE '%work%' THEN 1
      ELSE 0
    END AS is_work
  FROM
    (
      SELECT
        CDI,
        BusinessDate,
        OriginalTelephoneType,
        PhoneNumber1,
        PhoneNumber2,
        PhoneNumber3
      FROM
        `{env}-cdh`.primary.contactdetails
      WHERE
        length(trim(CDI)) > 3
        AND BusinessDate
          = (
            SELECT
              BusinessDate
            FROM
              report_date
          )
        AND length(DialerActivityTelephoneNumber) = 10 -- κρατάμε μόνο τηλέφωνα με 10 ψηφία (valid)
        AND (
          CollectionSourceTypeCode <> '6096'
          OR CollectionSourceTypeCode IS NULL
        ) -- (εξαιρούμε τα τηλέφωνα που αντιστοιχούν σε External Agences) -- filter from Nikolaos Syrigos query
        AND IsDialerActivityFlag = 1
        AND DoNotCallFlag = 'false'
    ) AS raw
    LATERAL VIEW explode(array(PhoneNumber1, PhoneNumber2, PhoneNumber3)) AS phone
  WHERE
    phone IS NOT NULL
),
-- 2) Βασική εγκυρότητα τηλεφώνων
valid_phones AS (
  SELECT
    BusinessDate,
    CDI,
    cleaned_phone AS phone,
    is_work
  FROM
    exploded_phones
  WHERE
    cleaned_phone IS NOT NULL
    AND cleaned_phone <> ''
    AND cleaned_phone NOT RLIKE '^0+$'
    AND length(cleaned_phone) >= 10
),
-- 3) Στην περίπτωση που για ένα CDI έχουμε ένα phone δύο φορές, στην μία είναι σηματοδοτημένο ως work, και στην άλλη ως non-work, τότε θεωρούμε πως είναι non-work.
dedup_phones AS (
  SELECT
    BusinessDate,
    CDI,
    phone,
    CASE
      WHEN
        SUM(
          CASE
            WHEN is_work = 0 THEN 1
            ELSE 0
          END
        ) > 0
      THEN
        0
      ELSE 1
    END AS is_work_effective
  FROM
    valid_phones
  GROUP BY
    BusinessDate,
    CDI,
    phone
),
-- 4) Κρατάμε μόνο μη-work όταν υπάρχουν, αλλιώς μόνο work
phones_after_policy AS (
  SELECT
    *,
    SUM(
      CASE
        WHEN is_work_effective = 0 THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY BusinessDate, CDI) AS non_work_count -- Υπολογίζουμε το πλήθος των non-work phones του πελάτη
  FROM
    dedup_phones
),
kept_phones AS (
  SELECT
    BusinessDate,
    CDI,
    phone,
    is_work_effective
  FROM
    phones_after_policy
  WHERE
    (
      non_work_count > 0
      AND is_work_effective = 0
    ) -- αν ο πελάτης έχει non-work τηλέφωνα, επιστρέφουμε τα non-work
    OR (
      non_work_count = 0
      AND is_work_effective = 1
    ) -- αν ο πελάτης δεν έχει non-work τηλέφωνα, επιστρέφουμε τα work
),
-- 5) Κατάταξη και pivot σε Phone1..Phone15
ranked_phones AS (
  SELECT
    BusinessDate,
    CDI,
    phone,
    is_work_effective,
    ROW_NUMBER() OVER (PARTITION BY BusinessDate, CDI ORDER BY phone ASC) AS rn
  FROM
    kept_phones
),
FinalContactDetails AS (
  SELECT
    BusinessDate,
    CDI,
    -- Pivot σε Phone1..Phone15
    MAX(
      CASE
        WHEN rn = 1 THEN phone
      END
    ) AS Phone1,
    MAX(
      CASE
        WHEN rn = 2 THEN phone
      END
    ) AS Phone2,
    MAX(
      CASE
        WHEN rn = 3 THEN phone
      END
    ) AS Phone3,
    MAX(
      CASE
        WHEN rn = 4 THEN phone
      END
    ) AS Phone4,
    MAX(
      CASE
        WHEN rn = 5 THEN phone
      END
    ) AS Phone5,
    MAX(
      CASE
        WHEN rn = 6 THEN phone
      END
    ) AS Phone6,
    MAX(
      CASE
        WHEN rn = 7 THEN phone
      END
    ) AS Phone7,
    MAX(
      CASE
        WHEN rn = 8 THEN phone
      END
    ) AS Phone8,
    MAX(
      CASE
        WHEN rn = 9 THEN phone
      END
    ) AS Phone9,
    MAX(
      CASE
        WHEN rn = 10 THEN phone
      END
    ) AS Phone10,
    MAX(
      CASE
        WHEN rn = 11 THEN phone
      END
    ) AS Phone11,
    MAX(
      CASE
        WHEN rn = 12 THEN phone
      END
    ) AS Phone12,
    MAX(
      CASE
        WHEN rn = 13 THEN phone
      END
    ) AS Phone13,
    MAX(
      CASE
        WHEN rn = 14 THEN phone
      END
    ) AS Phone14,
    MAX(
      CASE
        WHEN rn = 15 THEN phone
      END
    ) AS Phone15,
    -- Flag: υπάρχει κάποιο work ανάμεσα στα πρώτα 15;
    CASE
      WHEN
        MAX(
          CASE
            WHEN
              rn BETWEEN 1 AND 15
              AND is_work_effective = 1
            THEN
              1
            ELSE 0
          END
        ) = 1
      THEN
        1
      ELSE 0
    END AS AnyWorkPhone,
    -- Flag: είναι ΟΛΑ work ανάμεσα στα πρώτα 15;
    -- Αν οποιοδήποτε από τα (έως) 15 έχει is_work_effective = 0, τότε 0. Αλλιώς 1.
    CASE
      WHEN
        MIN(
          CASE
            WHEN rn BETWEEN 1 AND 15 THEN is_work_effective
          END
        ) = 1
      THEN
        1
      ELSE 0
    END AS AllWorkPhones
  FROM
    ranked_phones
  WHERE
    rn <= 15
  GROUP BY
    BusinessDate,
    CDI
),
portfolios AS (
  SELECT DISTINCT
    PortfolioName
  FROM
    `{env}-cdh`.primary.portfolio
  WHERE
    RetailWholesale = 'Retail'
)
SELECT DISTINCT 
  d.PortfolioName,
  d.CDI,
  COALESCE(e.CaseCode, NULL) AS CaseCode,
  d.CustomerID,
  c.Phone1,
  c.Phone2,
  c.Phone3,
  c.Phone4,
  c.Phone5,
  c.Phone6,
  c.Phone7,
  c.Phone8,
  c.Phone9,
  c.Phone10,
  c.Phone11,
  c.Phone12,
  c.Phone13,
  c.Phone14,
  c.Phone15,
  --c.AnyWorkPhone,
  c.AllWorkPhones,
  d.BusinessDate
FROM
  FilteredDparty d
    INNER JOIN portfolios p
      ON p.PortfolioName = d.PortfolioName
    LEFT JOIN FilteredDexposure e
      ON d.CDI = e.CDI
      AND d.BusinessDate = e.BusinessDate
    LEFT JOIN FinalContactDetails c
      ON d.CDI = c.CDI
      AND d.BusinessDate = c.BusinessDate
WHERE
  length(trim(d.CDI)) > 3
  AND d.BusinessDate
    = (
      SELECT
        BusinessDate
      FROM
        report_date
    )
  AND c.Phone1 IS NOT NULL
ORDER BY
  d.CDI
'''
)
df_dialer.createOrReplaceTempView("df_dialer") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Export

# COMMAND ----------

from export_utils import export_data
from helpers import archive_existing_files
from pyspark.sql import functions as F

df_dialer_exp = (
        spark.table("df_dialer")
            .filter(F.col("BusinessDate") == run_date))


os.makedirs(report_output, exist_ok=True)

# Archive existing files before writing new export
archive_existing_files(report_output)

# Get distinct portfolio names
portfolio_list = [
    row["PortfolioName"]
    for row in df_dialer_exp.select("PortfolioName").distinct().collect()
]

for portfolio in portfolio_list:
    pd_dialer_portfolio = (
        df_dialer_exp
            .filter(F.col("PortfolioName") == portfolio)).toPandas()

    print(f"Exporting portfolio: {portfolio} | rows: {len(pd_dialer_portfolio)}")

    output_buffer = BytesIO()
    with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
        pd_dialer_portfolio.to_excel(writer, sheet_name="Dialer Manual Campaign", index=False)

    output_path = f"{report_output}{output_file_prefix}{portfolio}_{run_date}.xlsx"

    with open(output_path, "wb") as f:
        f.write(output_buffer.getvalue())

    print(f"Written: {output_path}")

