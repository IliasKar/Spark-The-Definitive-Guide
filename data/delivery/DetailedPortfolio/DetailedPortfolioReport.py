# Databricks notebook source

# MAGIC %md
# MAGIC # 📑 Detailed Portfolio Report
# MAGIC  
# MAGIC ## 🔗 Links
# MAGIC - **[BRD Detailed Portfolio Mercury/Light/Orbit/Cell/Pearl](https://cepalhellas.atlassian.net/wiki/spaces/DBTC/pages/edit-v2/889389057?draftShareId=c8874f39-ec3f-49b2-be8c-f76e17c32ea8#%3Anote%3A-10.-Glossary-%26-Terminology)**  
# MAGIC   Comprehensive documentation outlining the business requirements for this report.
# MAGIC  
# MAGIC - **[Story Ticket](https://cepalhellas.atlassian.net/browse/DBTC-255)**  
# MAGIC   Details about the story ticket, including tasks and progress.
# MAGIC  
# MAGIC - **[UAT Findings Ticket](https://cepalhellas.atlassian.net/browse/DBTC-1349)**  
# MAGIC   A summary of findings from UAT.
# MAGIC  
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/DetailedPortfolio/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname

env_prefix =  get_env_prefix()

# COMMAND ----------

# dbutils.widgets.text("report_args", "")
# dbutils.widgets.text("run_date", "")
# dbutils.widgets.text("report_name", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Parameters
# MAGIC **report_args:** it is a json which contains all the appropriate parameters to run the report
# MAGIC `
# MAGIC {  
# MAGIC     "portfolio_name": "Mercury",
# MAGIC     "output_file_prefix": "Mercury_DetailedPortfolio_",
# MAGIC     "report_output": "/Volumes/qa-cdh-delivery/internalreports/detailedportfolio/mercury/"
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
# MAGIC ## Import Tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Calendar

# COMMAND ----------

calendar = spark.sql(
f'''
SELECT cast({run_date!r} as date) as month_end_business_date
-- SELECT DISTINCT CurrentMonthLastWorkingDate as month_end_business_date
-- FROM `{env}-cdh`.reference.ref_calendar
-- WHERE cast(CalendarDate as date) >= DATEADD(YEAR, -2, current_date()) 
-- AND cast(CalendarDate as date) <= current_date()
-- AND CalendarDate = {run_date!r}
'''
)
calendar.createOrReplaceTempView("calendar")

# COMMAND ----------

decommissionDate = '2026-06-04' # VP 07/09/2026

# COMMAND ----------

# MAGIC %md
# MAGIC # Portfolios

# COMMAND ----------

portfolios = spark.sql(
f'''
  SELECT 
    distinct portfolioname, prefix
  FROM 
    `{env}-cdh`.primary.portfolio
  WHERE  PortfolioName = {portfolioName!r}
'''
)
portfolios.createOrReplaceTempView('portfolios')

# COMMAND ----------

# MAGIC %md
# MAGIC # GuarantorDistinctLoanKeys

# COMMAND ----------

GuarantorDistinctLoanKeys = spark.sql(
f'''
SELECT
Exposurekey as loancode , BusinessDate
FROM `{env}-cdh`.primary.exposure
WHERE portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
AND PartyRelationshipType = 'Guarantor'
GROUP BY ExposureKey, BusinessDate
HAVING COUNT(*) > 0
'''
)
GuarantorDistinctLoanKeys.createOrReplaceTempView("GuarantorDistinctLoanKeys")

# COMMAND ----------

# MAGIC %md
# MAGIC # FX_Rates

# COMMAND ----------

fx_rates = spark.sql(
f'''
SELECT
OriginalCurrency, Date,
max(filled_fxrate) AS filled_fxrate
FROM `{env}-bi`.common.fx_rates
GROUP BY OriginalCurrency, Date
'''
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exposure

# COMMAND ----------

exposure = spark.sql(
f'''
SELECT DISTINCT PortfolioName, regexp_replace(ExposureKey, '[^0-9]', '') as ExposureKey, regexp_replace(MasterExposureKey, '[^0-9]', '') as  MasterExposureKey, SlaveFlag 
from `{env}-cdh`.primary.exposure 
WHERE SourceSystem = 'qca' 
AND SlaveFlag = TRUE 
'''
)
exposure.createOrReplaceTempView("exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dexposure

# COMMAND ----------

dexposure = spark.sql(
f'''
SELECT  
    PortfolioKey,
    sourcesystem,
    PortfolioName,
    ExposureSourceSystemKeyQCCepal AS LoanIDQualco, 
    CaseCode,
    d.exposurekey,
    CASE WHEN len(ExposureKey)>3 THEN right(exposurekey, len(exposurekey)-3) ELSE exposurekey END AS LoanBankCode_NoPrefix,
    PartyKey,
    d.cdi,
    OutstandingTotalBalance,
    -- CASE WHEN d.currency = 'EUR' or d.Currency is null THEN d.Outstandingtotalbalance ELSE d.Outstandingtotalbalance / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx WHERE fx.OriginalCurrency = d.currency AND fx.Date = d.businessdate) END AS outstandingTotalBananceEUR, -- VP 21/09/2026
    OnBalance,
    -- CASE WHEN d.currency = 'EUR' or d.Currency is null THEN d.onbalance ELSE d.onbalance / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx WHERE fx.OriginalCurrency = d.currency AND fx.Date = d.businessdate) END AS OnBalanceEUR,
    OffBalance, -- VP 21/09/2026
    -- CASE WHEN d.currency = 'EUR' or d.Currency is null THEN d.offbalance ELSE d.offbalance / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx WHERE fx.OriginalCurrency = d.currency AND fx.Date = d.businessdate) END AS OffBalanceEUR, -- VP 21/09/2026
    TotalPrincipal,
    TotalInterest,
    TotalExpense,
    TotalAccruals,
    -- CASE WHEN d.currency = 'EUR' or d.Currency is null THEN d.totalaccruals ELSE d.totalaccruals / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx WHERE fx.OriginalCurrency = d.currency AND fx.Date = d.businessdate) END AS AccrualsEUR, -- VP 21/09/2026
    GBV,
    -- CASE WHEN d.currency = 'EUR' or d.Currency is null THEN d.gbv ELSE d.gbv / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx WHERE fx.OriginalCurrency = d.currency AND fx.Date = d.businessdate) END AS GBVEUR, -- VP 21/09/2026
    ExposureStatus,
    DenouncedDate,
    CASE WHEN Partytaxregistrationnumber in ('', ' ', '000000000') THEN NULL ELSE PartyTaxRegistrationNumber END as PartyTaxRegistrationNumber,
    LPSEng,
    PartyRelationshipType,
    SettlementBucket,
    CASE WHEN ExposureStatus IN ('Normal','Overdue') THEN DuePrincipal+Dueinterest+Penaltyinterest+DueExpense+DueUnrealizedinterest+PenaltyUnrealizedinterest + UnrealizedInterest  ELSE OutstandingTotalBalance END AS DueAmount,
    CoCStatus,
    LPSDate,
    CASE WHEN IsActiveFlag=True THEN "Open" ELSE "Closed" END AS LoanActiveFlag, -- VP 21/09/2026
    d.BusinessDate as BusinessDate, 
    d.BusinessDate as LastBusinessDateActive, 
    d.currency,
    assetsegment,
    CASE WHEN EXISTS (
                SELECT 1
                FROM GuarantorDistinctLoanKeys gu
                WHERE gu.loancode = d.ExposureKey
                AND gu.BusinessDate <= d.BusinessDate
            ) THEN "Yes" ELSE "No" END AS HasGuarantor,
    productkey,
    AgreementKey as ContractNumber,
    NextInstallmentAmount,
    d.NextInstallmentDate
  FROM  `{env}-cdh`.derivative.get_exposures_by_business_date((SELECT DISTINCT Month_end_Business_date from calendar), false) d
  LEFT JOIN `{env}-bi`.common.fx_rates fx ON d.currency = fx.OriginalCurrency AND d.businessdate = fx.Date
  WHERE PortfolioName IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND d.sourcesystem = 'profits'

'''
)
dexposure.createOrReplaceTempView('dexposure')

# COMMAND ----------

# MAGIC %md
# MAGIC # Dparty

# COMMAND ----------

dparty = spark.sql(
f'''
  SELECT 
    extrasegment
    ,subsegment
    ,economicactivity
    ,cdi
    ,businessdate
    ,partykey
    ,affectedcategory
    ,CASE WHEN taxregistrationnumber in ('', ' ', '000000000') THEN NULL ELSE TaxRegistrationNumber END as TaxRegistrationNumber
    ,segment
    ,PortfolioName
    ,LastNameGR
    ,FirstNameGR
  FROM `{env}-cdh`.derivative.dparty
  WHERE portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  and SourceSystem = 'profits'
'''
)
dparty.createOrReplaceTempView('dparty')

# COMMAND ----------

# MAGIC %md
# MAGIC # MultipleCIF

# COMMAND ----------

multiplecif = spark.sql(
f'''
  SELECT  
    d1.businessdate,
    d1.TaxRegistrationNumber as PartyTaxRegistrationNumber,
   array_join(array_sort(collect_set(d1.CDI)), ',') as MultipleCIF
  FROM dparty d1
  INNER JOIN dparty d2
  ON d1.TaxRegistrationNumber = d2.TaxRegistrationNumber 
  AND d1.BusinessDate <= d2.BusinessDate
  WHERE 1=1 
  AND d1.businessdate <= (SELECT MAX(month_end_business_date) FROM Calendar)
  AND d2.businessDate <= (SELECT MAX(month_end_business_date) FROM Calendar)
  GROUP BY d1.businessdate, d1.TaxRegistrationNumber

'''
)
multiplecif.createOrReplaceTempView('multiplecif')

# COMMAND ----------

# MAGIC %md
# MAGIC # BalancePrimary

# COMMAND ----------

balanceprimary = spark.sql(
f'''
    SELECT 
        d.businessdate,
        d.PartyKey, 
        SUM(d.OutstandingTotalBalance) AS BalancePrimary,
        CASE WHEN d.currency = 'EUR' or d.Currency is null THEN SUM(d.OutstandingTotalBalance) ELSE SUM(d.OutstandingTotalBalance) / (SELECT max(fx.filled_fxrate) FROM `{env}-bi`.common.fx_rates fx) END AS BalancePrimaryEUR
    FROM `{env}-cdh`.derivative.dexposure d
    LEFT JOIN `{env}-bi`.common.fx_rates fx ON d.currency = fx.OriginalCurrency AND d.businessdate = fx.Date
    WHERE d.PortfolioName IN (SELECT DISTINCT PortfolioName FROM portfolios)
    AND d.businessdate IN (SELECT month_end_business_date FROM Calendar)
    GROUP BY d.businessdate, d.PartyKey, d.currency
'''
)
balanceprimary.createOrReplaceTempView('balanceprimary')

# COMMAND ----------

# MAGIC %md
# MAGIC # ContactDetails

# COMMAND ----------

# contactdetails = spark.sql( -- VP 21/09/2026
# f'''
#   SELECT DISTINCT 
#   coalesce(dp.TaxRegistrationNumber, dp.CDI) as ParentPartyId,
#   dp.TaxRegistrationNumber,
#   dp.CDI,
#   --dp.PartyKey,
#   cd.Street,
#   cd.Street1,
#   cd.Street2,
#   cd.Street3,
#   cd.County,
#   cd.City,
#   cd.PostalCode,
#   cd.PhoneNumber1,
#   cd.PhoneNumber2,
#   cd.PhoneNumber3,
#   cd.LastUpdatedDate,
#   cast(cd.CreatedDate as date),
#   dp.BusinessDate,
#   cd.CommunicationFlag
#   from `{env}-cdh`.primary.contactdetails cd 
#   INNER JOIN  dparty dp
#   ON dp.cdi = cd.cdi
#   and dp.businessdate = cd.BusinessDate 
#   and dp.PortfolioName = cd.PortfolioName 
#   WHERE cd.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
#   and cd.CreatedDate is not null

    
# '''
# )
# contactdetails.createOrReplaceTempView('contactdetails')

# COMMAND ----------

# UpdateContactDetails = spark.sql( -- VP 21/09/2026
# f'''
# select *,
# /* If for any of the rows a change was found then the ParentPartyId should be labeled as one with updated address */ 
# row_number() over (partition by BusinessDate, ParentPartyId order by case when UpdateAddress_5d = 'Yes' THEN 1 ELSE 0 END desc) AS rn
# from (
# Select DISTINCT  b1.BusinessDate, 
# b1.ParentPartyId,
# b1.TaxRegistrationNumber, 
# CASE 
# WHEN EXISTS (
#     SELECT 1
#     FROM contactdetails b2
#     WHERE b2.TaxRegistrationNumber = b1.TaxRegistrationNumber and b2.CDI = b1.CDI -- b2.ParentPartyId = b1.ParentPartyId
#         AND datediff(b1.BusinessDate, b2.BusinessDate) <= 5
#         AND (
#             coalesce(b2.Street1, '') <> coalesce(b1.Street1, '') OR
#             coalesce(b2.Street2, '') <> coalesce(b1.Street2, '') OR
#             coalesce(b2.Street3, '') <> coalesce(b1.Street3, '') OR 
#             coalesce(b2.Street, '') <> coalesce(b1.Street, '') OR 
#             coalesce(b2.City, '') <> coalesce(b1.City, '') OR 
#             coalesce(b2.County, '') <> coalesce(b1.County, '') OR 
#             coalesce(b2.PostalCode, '') <> coalesce(b1.PostalCode, '')
#         )
#        AND CommunicationFlag = 1
# ) 
# AND b1.CommunicationFlag = 1 
# THEN "Yes" ELSE "No"
# END AS UpdateAddress_5d,
# CASE WHEN datediff(b1.BusinessDate, coalesce(MaxCreatedDate, '2000-01-01')) <= 5 THEN 'Yes' ELSE 'No' END AS UpdatePhone_5d  
# from contactdetails b1 
# LEFT JOIN 
# (
#     SELECT  
#     cd2.CDI, cd2.BusinessDate, MAX(try_cast(cd.CreatedDate as date)) as MaxCreatedDate
    
#     FROM contactdetails cd
#     inner join contactdetails cd2
#     on cd.CDI = cd2.CDI 
#     and cd.BusinessDate <= cd2.BusinessDate
#     --and cd.CreatedDate is not null
#     GROUP BY 
#     cd2.CDI, cd2.BusinessDate
# ) cd3
# ON b1.cdi = cd3.cdi 
# AND b1.BusinessDate = cd3.BusinessDate 
# WHERE B1.businessdate IN (SELECT month_end_business_date FROM Calendar)
# )
# '''
# )
# UpdateContactDetails.createOrReplaceTempView('UpdateContactDetails')

# COMMAND ----------

# MAGIC %md
# MAGIC # Assignment

# COMMAND ----------

assignment = spark.sql(
f'''
  SELECT DISTINCT a.PortfolioName,
      regexp_replace(a.ExposureKey, '[^0-9]', '') as ExposureKey,-- a.ExposureKey
    CASE WHEN a.AssignmentProcess ilike '%CA Dialer%' THEN 'Unassigned' ELSE  a.agentfullname END as AgentFullName
    , IFNULL(a.Team, 'Unassigned') as Team 
    , a.Channel Channel -- VP 09/07/2026
    , a.externalcompanyfullname
    , a.assignmentprocess
    , f.ManagerPartyName as Manager
    , CASE 
      WHEN a.Channel = 'Financial Advisors' THEN 'Logariastaki Georgia'
      WHEN a.Channel = 'Legal Agencies' THEN 'Theodora Papoulidou'
      WHEN a.Channel = 'IRT' THEN 'Nikoleta Markouli'
      END AS Supervisor -- VP 09/07/2026
    ,a.BusinessDate
    , a.SourceSystem
    , a.CDI -- VP 09/07/2026
    , a.ProcessType -- VP 09/07/2026

  FROM `{env}-cdh`.primary.assignment a
  LEFT JOIN `{env}-cdh`.primary.firmstructure f 
  ON a.AgentPartyKey = f.EmployeePartyKey and a.BusinessDate = f.BusinessDate and f.SourceSystem = 'qca' -- and a.Team = f.Team
  WHERE a.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  and a.SourceSystem = 'qca'
  and a.AssignmentType = 'Channel Assignment'
'''
)
assignment.createOrReplaceTempView('assignment') # VP 30/06/2026

# COMMAND ----------

# MAGIC %md
# MAGIC # CaseCode

# COMMAND ----------

# casecode = spark.sql(
# f'''
#   SELECT
#     ParentStrategyStep
#     ,StrategyType
#     ,StrategyStep
#     , Queue
#     , TRIM(
#         REPLACE(
#             REPLACE(
#                 REPLACE(
#                     REPLACE(
#                         REPLACE(
#                             Queue,
#                             'Mercury', ''),
#                         'Cell', ''),
#                     'Orbit', ''),
#                 'Light', ''),
#             'Pearl', '')
#     ) AS QueueTrimmed
#     ,BusinessDate
#     ,casecodekey
#   FROM `{env}-cdh`.primary.casecode
#   WHERE portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
# '''
# )
# casecode.createOrReplaceTempView('casecode')

# COMMAND ----------

casecode = spark.sql(
f'''
WITH BaseData AS (
    SELECT
        ParentStrategyStep,
        StrategyType,
        StrategyStep,
        Queue,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                Queue,
                                'Mercury', ''
                            ),
                            'Cell', ''
                        ),
                        'Orbit', ''
                    ),
                    'Light', ''
                ),
                'Pearl', ''
            )
        ) AS QueueTrimmed,
        BusinessDate,
        CaseCodeKey
    FROM `{env}-cdh`.primary.casecode
    WHERE PortfolioName IN (
        SELECT DISTINCT PortfolioName
        FROM portfolios
    )
),

PreviousStep AS (
    SELECT
        *,
        LAG(StrategyStep) OVER (
            PARTITION BY CaseCodeKey
            ORDER BY BusinessDate
        ) AS PreviousStrategyStep
    FROM BaseData
),

StepChanges AS (
    SELECT
        *,
        CASE
            WHEN PreviousStrategyStep IS NULL THEN 1
            WHEN NOT (StrategyStep <=> PreviousStrategyStep) THEN 1 -- The <=> guarantees that NULL -> NULL are not considered as Step Changes
            ELSE 0
        END AS IsNewStepPeriod
    FROM PreviousStep
),

StepPeriods AS (
    SELECT
        *,
        SUM(IsNewStepPeriod) OVER (
            PARTITION BY CaseCodeKey
            ORDER BY BusinessDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS StepPeriodId
    FROM StepChanges
),

StepStartDates AS (
    SELECT
        *,
        MIN(BusinessDate) OVER (
            PARTITION BY CaseCodeKey, StepPeriodId
        ) AS StepStartDate
    FROM StepPeriods
)

SELECT
    ParentStrategyStep,
    StrategyType,
    StrategyStep,
    Queue,
    QueueTrimmed,
    BusinessDate,
    CaseCodeKey,
    StepStartDate,
    DATEDIFF(BusinessDate, StepStartDate) AS DaysInStep
FROM StepStartDates
'''
)

casecode.createOrReplaceTempView('casecode')

# COMMAND ----------

# MAGIC %md
# MAGIC # SettlementExposure

# COMMAND ----------

settlementexposure = spark.sql( 
f'''
  SELECT -- VP 07/09/2026
    settlementtype
    ,settlementstatus
    ,businessdate
    ,remainamount
    ,SettlementAmount
    ,NextInstalmentAmount
    ,NextInstalmentDate
    ,approvaldate
    ,createddate
    ,Bucket
    ,CASE WHEN LEFT(se.ExposureKey,3) = p.Prefix THEN se.ExposureKey ELSE CONCAT(p.Prefix,se.ExposureKey) END ExposureKey -- VP 07/09/2026
    ,CASE WHEN LEFT(se.CDI,3) = p.Prefix THEN se.CDI ELSE CONCAT(p.Prefix,se.CDI) END CDI -- VP 07/09/2026
    ,isActiveFlag
    ,se.PortfolioName -- VP 07/09/2026
    ,se.ImplementationDate -- VP 07/09/2026
  FROM `{env}-cdh`.derivative.dsettlementexposure se -- `p-cdh`.primary.settlementexposure se -- VP 07/09/2026
  INNER JOIN Portfolios p ON p.PortfolioName = se.PortfolioName
  WHERE businessdate IN (SELECT month_end_business_date FROM Calendar)
'''
)
settlementexposure.createOrReplaceTempView('settlementexposure')

# COMMAND ----------

# MAGIC %md
# MAGIC # Interaction_V0

# COMMAND ----------

interaction_v0 = spark.sql(
f'''
SELECT DISTINCT 
CASE WHEN LEFT(intr.ExposureKey,3) = md.Prefix THEN intr.ExposureKey ELSE CONCAT(md.Prefix,intr.ExposureKey) END ExposureKey,
intr.PortfolioName,
CASE WHEN LEFT(intr.CDI,3) = md.Prefix THEN intr.CDI ELSE CONCAT(md.Prefix,intr.CDI) END CDI,
INTR.InteractionTypeCode,
INTR.INTERACTIONTYPE,
CAST(INTR.InteractionDate AS DATE) AS InteractionDate,
INTR.InteractionKey,
INTR.INTERACTIONTYPEKEY,
intr.InteractionCategory,
intr.FirstLevelCategory,
intr.SecondLevelCategory,
intr.AttemptOutcome,
intr.IsPositiveFlag,
intr.IsPromiseFlag,
intr.ContactType,
intr.IsCallFlag,
intr.AttemptType,
intr.IsFtFAppointmentFlag,
intr.createddate,
intr.SourceSystem,
intr.PromiseDueDate--CONVERT(DATE,intr.PromiseDueDate, 112) as PromiseDueDate -- TRY_TO_DATE(intr.promiseduedate, 'yyyyMMdd') as PromiseDueDate
,intr.PromiseAmount
,intr.BusinessDate
from 
(
SELECT * FROM `{env}-cdh`.primary.get_interactions_by_business_date('{decommissionDate}') intr
WHERE InteractionDate <= '{decommissionDate}'
and intr.PortfolioName IN (SELECT DISTINCT PortfolioName FROM Portfolios)

UNION ALL 

SELECT * FROM `{env}-cdh`.primary.get_interactions_by_business_date('{run_date}') intr
WHERE 
InteractionDate > '{decommissionDate}' 
AND InteractionDate <= (SELECT month_end_business_date FROM Calendar)
and intr.PortfolioName IN (SELECT DISTINCT PortfolioName FROM Portfolios)
) intr 
left join portfolios md 
on md.PortfolioName = intr.PortfolioName
where 1=1
and intr.PortfolioName IN (SELECT DISTINCT PortfolioName FROM Portfolios)
'''
)
interaction_v0.createOrReplaceTempView("interaction_v0")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dialer_v0

# COMMAND ----------

Dialer_v0 = spark.sql(
f'''

select DISTINCT  
       d.BusinessDate,
    NULL AS ExposureKey,--      de.ExposureKey
     cast('' as string) as InteractionTypeKey 
    , cast(D.DateHour as date ) as InteractionDate
    , D.DateHour as CreatedDate    
    , d.DialerKey AS InteractionKey
    , CASE WHEN LEFT(d.CDI,3) = md.Prefix THEN d.CDI ELSE CONCAT(md.Prefix,d.CDI) END CDI
    , d.CallDisposition
    , d.PortfolioName
from `{env}-cdh`.primary.get_dialer_by_business_date('{run_date}') D 
LEFT JOIN Portfolios md 
on md.PortfolioName = D.PortfolioName
WHERE D.PortfolioName IN (SELECT DISTINCT PortfolioName FROM Portfolios)
AND d.CallDisposition IS NOT NULL
AND d.CallDisposition <> ''
AND cast(D.DateHour as date ) <= (SELECT month_end_business_date FROM Calendar)
'''
)
Dialer_v0.createOrReplaceTempView("Dialer_v0")

# COMMAND ----------

# MAGIC %md
# MAGIC # LegalAction

# COMMAND ----------

legalaction = spark.sql(
f'''
select CASE WHEN LEFT(la.ExposureKey,3) = md.Prefix THEN la.ExposureKey ELSE CONCAT(md.Prefix,la.ExposureKey) END ExposureKey,
CASE WHEN LEFT(la.CDI,3) = md.Prefix THEN la.CDI ELSE CONCAT(md.Prefix,la.CDI) END CDI, 
LawyerPartyName, 
LegalActionDate, 
la.PortfolioName,
la.AgreementKey,
RANK() OVER (PARTITION BY  la.AgreementKey ORDER BY legalactiondate DESC, legalActionStepStartDate DESC, IFNULL(LegalActionStepEndDate, "9999-12-31") DESC, LegalActionId DESC) AS rn
from `{env}-cdh`.primary.legalaction la 
left join portfolios md 
on md.PortfolioName = la.PortfolioName
where la.PortfolioName IN (SELECT DISTINCT PortfolioName FROM Portfolios)
and BusinessDate IN (SELECT month_end_business_date FROM Calendar)
and SourceSystem = 'arotron'
'''
)
legalaction.createOrReplaceTempView('legalaction')

# COMMAND ----------

# MAGIC %md
# MAGIC # Interaction

# COMMAND ----------

interaction = spark.sql(f'''

SELECT  intr.*, -- VP 07/09/2026
CASE WHEN UPPER(COALESCE(ContactType, '')) <> 'SMS'

            AND (
                InteractionDate <= '{decommissionDate}'
                AND (
                intr.ContactType IN ('Right Person Contact', '3rd Person Contact','RIGHT PARTY CONTACT') 
                OR COALESCE(intr.IsCallFlag, false) = true
                OR COALESCE(intr.IsFtFAppointmentFlag, false) = true
                OR LOWER(COALESCE(intr.InteractionType, '')) LIKE '%appointment%'
                OR LOWER(COALESCE(intr.AttemptType, '')) LIKE '%unsuccess%'
                OR LOWER(COALESCE(intr.AttemptOutcome, '')) LIKE '%no answer%'
                OR LOWER(COALESCE(intr.AttemptOutcome, '')) LIKE '%wrong%'
                OR LOWER(COALESCE(intr.InteractionType, '')) LIKE '%planning business trip%'
                OR intr.interactionType = 'Dialer Attempts (SIP)' -- This InteractionType is NOT included for Total Attempts, however it is used for the calculation of the total successful attempts - EK 
                )
            
                OR 
                ( InteractionDate > '{decommissionDate}' AND 
                (FirstLevelCategory <> '0. Legal Action - no contact' 
                AND NOT (FirstLevelCategory = "1. no contact" AND SecondLevelCategory = "1.2 no contact - attempt")
                )
                )
                
            ) THEN 1 ELSE 0 END AS Attempt, 

CASE 
WHEN CAST(intr.InteractionDate AS DATE) <= '{decommissionDate}' and 
(
   UPPER(COALESCE(ContactType, '')) IN ('RIGHT PARTY CONTACT', 'FACE TO FACE', 'RIGHT PERSON CONTACT') OR LOWER(COALESCE(InteractionType, '')) LIKE '%planning business trip%'
) -- Face to Face is not included in Contact Types for Hoist Portfolios
THEN 1 
WHEN CAST(intr.InteractionDate AS DATE) > '{decommissionDate}' AND (intr.FirstLevelCategory NOT IN ('1. no contact', '2. contact - Not RPC', '0. Legal Action - no contact'))
THEN 1
ELSE 0 
END AS RPC

FROM (                                   
        SELECT 
         --   intr.BusinessDate,
            intr.PortfolioName,
            intr.ExposureKey,
            intr.CDI,
            CAST( intr.InteractionDate AS DATE) InteractionDate,
            intr.InteractionKey,
            intr.InteractionType,
            INTR.InteractionTypeCode,
            intr.PromiseAmount,
            intr.PromiseDueDate,
            intr.IsCallFlag,
            intr.IsFtFAppointmentFlag,
            intr.AttemptType,
            intr.ContactType,
            intr.InteractionCategory, 
            intr.AttemptOutcome, 
            intr.IsPositiveFlag,
            intr.IsPromiseFlag,
            intr.FirstLevelCategory,
            intr.SecondLevelCategory,
            intr.BusinessDate
        FROM interaction_v0 intr  
        WHERE 1=1 
            
        UNION 
        
        SELECT
       -- di.BusinessDate,
            di.PortfolioName,
            di.ExposureKey,
            di.CDI, 
            di.InteractionDate,
            di.InteractionKey,
            'Dialer' as InteractionType,
            'Dialer' as InteractionTypeCode,
            NULL AS PromiseAmount,
            NULL As PromiseDueDate,
            NULL AS IsCallFlag,
            NULL AS IsFtFAppointmentFlag,
            NULL AS AttemptType,
            CASE WHEN di.CallDisposition IN ('ANSWERED', 'ANSWER') THEN 'Dialer Answer' else null end as ContactType,
            'Dialer' AS InteractionCategory, 
            NULL as AttemptOutcome,
            NULL AS IsPositiveFlag,
            NULL AS IsPromiseFlag,
            NULL AS FirstLevelCategory,
            NULL AS SecondLevelCategory, 
            di.BusinessDate
        FROM Dialer_v0 as di
) AS intr 
LEFT JOIN portfolios as md on md.portfolioname = intr.portfolioname

'''
)
interaction.createOrReplaceTempView("interaction")

# COMMAND ----------

# MAGIC %md
# MAGIC # LastSettlement

# COMMAND ----------

lastsettlement = spark.sql(
f'''
  SELECT -- VP 07/09/2026
    c.month_end_business_date
    ,se.ExposureKey -- VP 07/09/2026
    ,se.settlementstatus
    ,se.businessdate
    ,se.settlementtype
    ,se.Bucket
    ,se.implementationdate
    ,se.RemainAmount
    ,se.ApprovalDate
    ,ROW_NUMBER() OVER (PARTITION BY c.month_end_business_date, se.ExposureKey ORDER BY CreatedDate DESC, implementationdate DESC) AS rn
  FROM Calendar c
  LEFT JOIN settlementexposure se -- `p-cdh`.primary.settlementexposure se -- VP 07/09/2026
  ON se.businessdate <= c.month_end_business_date
  WHERE se.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios) -- VP 07/09/2026
'''
)
lastsettlement.createOrReplaceTempView('lastsettlement')

# COMMAND ----------

# %sql
# select * from lastsettlement where exposurekey like '%375533016630003' -- '%5100990427196001'

# COMMAND ----------

# MAGIC %md
# MAGIC # LastInteraction

# COMMAND ----------

lastinteraction = spark.sql(
f'''
  SELECT
    c.month_end_business_date
    ,i.interactiontypecode
    ,i.interactiontype
    ,i.interactiondate
    ,i.cdi
    ,ROW_NUMBER() OVER (PARTITION BY c.month_end_business_date, i.cdi ORDER BY interactiondate DESC) AS rn
  FROM Calendar c
  LEFT JOIN interaction i 
  ON i.InteractionDate <= c.month_end_business_date
  WHERE portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND Attempt = 1 
'''
)
lastinteraction.createOrReplaceTempView('lastinteraction')

# COMMAND ----------

# MAGIC %md
# MAGIC # PaymentSchedule

# COMMAND ----------

paymentschedule = spark.sql(
f'''
  Select distinct ps.BusinessDate, ps.ExposureKey,
MAX(
    CASE 
        WHEN se.SettlementStatus in ('Out of Collection','Cancelled')  and se.isActiveFlag = 1   THEN se.NextInstalmentDate
        WHEN (ps.InstallmentDate BETWEEN ref.CurrentMonthFirstWorkingDate AND ref.CurrentMonthLastWorkingDate)  and ps.PaymentScheduleType = 'Settlement Payment Schedule'  THEN ps.installmentdate
        ELSE NULL
    END 
) AS CurrentMonthInstallmentDate,
MAX(
    CASE 
        WHEN se.SettlementStatus in ('Out of Collection','Cancelled')  and se.isActiveFlag = 1   THEN se.NextInstalmentAmount  
        WHEN (ps.InstallmentDate  BETWEEN ref.CurrentMonthFirstWorkingDate AND ref.CurrentMonthLastWorkingDate) and ps.PaymentScheduleType = 'Settlement Payment Schedule' THEN ps.installmentamount  
        ELSE NULL
    END
) AS CurrentMonthInstallmentAmount
FROM `{env}-cdh`.derivative.dpaymentschedule ps 
INNER JOIN 
(   SELECT 
    DATE_FORMAT(CalendarDate, 'yyyy-MM') as MonthSelection, 
    MIN(CalendarDate) as CurrentMonthFirstWorkingDate, 
    MAX(CalendarDate) CurrentMonthLastWorkingDate 
    FROM `{env}-cdh`.reference.ref_calendar 
    WHERE IsBusinessDay = 1 
    GROUP BY DATE_FORMAT(CalendarDate, 'yyyy-MM')
) ref
ON DATE_FORMAT(ps.BusinessDate, 'yyyy-MM') = ref.MonthSelection
INNER JOIN settlementexposure se ON se.ExposureKey = ps.ExposureKey and se.BusinessDate = ps.BusinessDate
WHERE 1=1
AND ps.businessdate IN (SELECT month_end_business_date FROM Calendar)
AND ps.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
-- and ps.PaymentScheduleType = 'Settlement Payment Schedule'
group by ps.BusinessDate, ps.ExposureKey
'''
)
paymentschedule.createOrReplaceTempView('paymentschedule')

# COMMAND ----------

# MAGIC %md
# MAGIC # Product

# COMMAND ----------

product = spark.sql(
f'''
  SELECT
  p.BusinessDate,
  p.productkey,
  p.SubProduct
  FROM `{env}-cdh`.primary.product p
  Where BusinessDate IN (SELECT month_end_business_date FROM Calendar)
'''
)
product.createOrReplaceTempView('product')

# COMMAND ----------

# MAGIC %md
# MAGIC # LastAttemptDates

# COMMAND ----------

LastAttemptDates = spark.sql( 
f'''
  SELECT 
    c.month_end_business_date
    ,i.cdi
    ,MAX(CASE WHEN Attempt = 1 THEN i.interactionDate ELSE NULL END) AS LastAttemptDate
    ,Max(CASE WHEN RPC = 1 THEN i.interactiondate ELSE NULL END) AS LastRPCDate
  FROM interaction i 
  INNER JOIN  calendar c 
  ON i.interactiondate <= c.month_end_business_date
  GROUP BY i.cdi, c.month_end_business_date
'''
)
LastAttemptDates.createOrReplaceTempView('lastattemptdates')

# COMMAND ----------

# MAGIC %md
# MAGIC # Activity3005

# COMMAND ----------

activity3005 = spark.sql(
f'''
  SELECT 
    c.month_end_business_date
    ,i.exposurekey
    ,Max(i.interactiondate) AS Max3005
  FROM Calendar c
  INNER JOIN 
  (
  SELECT ExposureKey, InteractionDate, i.BusinessDate 
  FROM interaction i
  WHERE 1=1 
  AND ((InteractionDate < '{decommissionDate}' and InteractionTypeCode = '3005') OR (InteractionDate > '{decommissionDate}' and InteractionTypeCode =   '3541')) -- VP 07/09/2026
  AND InteractionDate >= '2020-01-01'
  AND portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios ) -- VP 07/09/2026
  ) i
  ON i.businessdate <= c.month_end_business_date
  GROUP BY  c.month_end_business_date, i.exposurekey
'''
)
activity3005.createOrReplaceTempView('activity3005')

# COMMAND ----------

# MAGIC %md
# MAGIC # Activity3120

# COMMAND ----------

activity3120 = spark.sql(
f'''
  SELECT 
    c.month_end_business_date
    ,i.exposurekey
    ,Max(i.interactiondate) AS Max3120
  FROM Calendar c
  INNER JOIN 
  (
  SELECT ExposureKey, InteractionDate, i.BusinessDate 
  FROM interaction i 
  WHERE 1=1 
  AND ((InteractionDate < '{decommissionDate}' and InteractionTypeCode = '3120') OR (InteractionDate > '{decommissionDate}' and InteractionTypeCode =   '3556'))  -- VP 07/09/2026
  AND InteractionDate >= '2020-01-01'
  AND portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios ) -- VP 07/09/2026
  ) i
  ON i.businessdate <= c.month_end_business_date
  GROUP BY  c.month_end_business_date, i.exposurekey
'''
)
activity3120.createOrReplaceTempView('activity3120')

# COMMAND ----------

# MAGIC %md
# MAGIC # CollectionActual

# COMMAND ----------

collectionactual = spark.sql(
f'''
  SELECT 
    col.businessdate, col.ActualDate
    ,CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix 
          THEN CAST(col.ExposureKey AS STRING) 
          ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING))  
          END AS ExposurekeyPrefix
   -- ,MAX(col.actualdate) AS ActualDate
    ,SUM(col.amount) AS amount
    ,row_number() over (partition by col.businessdate,
                        CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(col.ExposureKey AS STRING) ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING)) END
                        order by col.actualdate desc) AS rn
  FROM `{env}-cdh`.primary.collection  COL 
  LEFT JOIN `{env}-bi`.common.dim_portfolio po ON po.PortfolioName = col.PortfolioName
  WHERE 1=1
  AND FinalReason='Matched'
  AND col.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND col.businessdate IN (SELECT month_end_business_date FROM Calendar)
  and col.actualdate < col.BusinessDate
  group by col.businessdate, col.ActualDate, CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix 
          THEN CAST(col.ExposureKey AS STRING) 
          ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING))  
          END
'''
)
collectionactual.createOrReplaceTempView('collectionactual')

# COMMAND ----------

# MAGIC %md
# MAGIC # CollectionCurrentMonth

# COMMAND ----------

collectioncurrentmonth = spark.sql(
f'''
  SELECT 
    col.businessdate
    ,CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(col.ExposureKey AS STRING) ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING)) END AS ExposurekeyPrefix
    ,SUM(col.amount) AS ActualPaymentsCurrentMonth
  FROM `{env}-cdh`.primary.collection col
  LEFT JOIN `{env}-bi`.common.dim_portfolio po ON po.PortfolioName = col.PortfolioName
  WHERE FinalReason='Matched'
  AND MONTH(col.actualdate) = MONTH(col.businessdate) AND YEAR(col.actualdate) = YEAR(col.businessdate)
  AND col.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND col.businessdate IN (SELECT month_end_business_date FROM Calendar)
  GROUP BY ALL
'''
)
collectioncurrentmonth.createOrReplaceTempView('collectioncurrentmonth')

# COMMAND ----------

# MAGIC %md
# MAGIC # CollectionLast3M

# COMMAND ----------

collectionlast3m = spark.sql(
f'''
  SELECT 
    col.businessdate
    ,CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(col.ExposureKey AS STRING) ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING)) END AS ExposurekeyPrefix
    ,SUM(col.amount) AS TotalPaymentAmountLast3M
  FROM 
    `{env}-cdh`.primary.collection col
  LEFT JOIN `{env}-bi`.common.dim_portfolio po ON po.PortfolioName = col.PortfolioName
  WHERE 
      FinalReason='Matched'
  AND CASE WHEN day(col.ActualDate) > day(col.BusinessDate) THEN datediff(MONTH, col.ActualDate, col.businessdate) + 1 ELSE datediff(MONTH, col.ActualDate, col.businessdate) END  < 3
  AND col.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND col.businessdate IN (SELECT month_end_business_date FROM Calendar)
  GROUP BY ALL
'''
)
collectionlast3m.createOrReplaceTempView("collectionlast3m")

# COMMAND ----------

# MAGIC %md
# MAGIC # CollectionLast6M

# COMMAND ----------

collectionlast6m = spark.sql(
f'''
  SELECT 
    col.businessdate
    ,CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(col.ExposureKey AS STRING) ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING)) END AS ExposurekeyPrefix
    ,SUM(col.amount) AS TotalPaymentAmountLast6M
  FROM 
    `{env}-cdh`.primary.collection col
  LEFT JOIN `{env}-bi`.common.dim_portfolio po ON po.PortfolioName = col.PortfolioName
  WHERE 
      FinalReason='Matched'
  AND CASE WHEN day(col.ActualDate) > day(col.BusinessDate) THEN datediff(MONTH, col.ActualDate, col.businessdate) + 1 ELSE datediff(MONTH, col.ActualDate, col.businessdate) END < 6
  AND col.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND col.businessdate IN (SELECT month_end_business_date FROM Calendar)
  GROUP BY ALL
'''
)
collectionlast6m.createOrReplaceTempView("collectionlast6m")

# COMMAND ----------

# MAGIC %md
# MAGIC # CollectionLast12M

# COMMAND ----------

collectionlast12m = spark.sql(
f'''
  SELECT 
    col.businessdate
    ,CASE WHEN LEFT(CAST(col.ExposureKey AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(col.ExposureKey AS STRING) ELSE CONCAT(po.prefix, CAST(col.ExposureKey AS STRING)) END AS ExposurekeyPrefix
    ,SUM(col.amount) AS TotalPaymentAmountLast12M
  FROM 
    `{env}-cdh`.primary.collection col
  LEFT JOIN `{env}-bi`.common.dim_portfolio po ON po.PortfolioName = col.PortfolioName
  WHERE 
      FinalReason='Matched'
  AND CASE WHEN day(col.ActualDate) > day(col.BusinessDate) THEN datediff(MONTH, col.ActualDate, col.businessdate) + 1 ELSE datediff(MONTH, col.ActualDate, col.businessdate) END < 12
  AND col.portfolioname IN (SELECT DISTINCT PortfolioName FROM portfolios)
  AND col.businessdate IN (SELECT month_end_business_date FROM Calendar)
  GROUP BY ALL
'''
)
collectionlast12m.createOrReplaceTempView("collectionlast12m") 

# COMMAND ----------

# MAGIC %md
# MAGIC # LegalProcessStatusDebtor

# COMMAND ----------

legalprocessstatusdebtor = spark.sql(
f'''
 select lps.portfolioname,
  lps.businessdate,
  lps.LegalProcessStatusLastChangeDate,
  lps.legalprocessstatuslevel1,
  lps.legalprocessstatuslevel2,
  lps.operationallegalstatuslevel3,
  lps.cdi,
  CASE WHEN LEFT(CAST(lps.CDI AS STRING), LENGTH(po.prefix)) = po.prefix THEN CAST(lps.cdi AS STRING) ELSE CONCAT(po.prefix, CAST(lps.cdi AS STRING)) END as CDIPrefix,
  row_number() over (partition by lps.cdi,lps.businessdate order by lps.LegalProcessStatusLastChangeDate desc) as rn
  from `{env}-cdh`.derivative.dlegalprocessstatusdebtor lps
  left join `{env}-bi`.common.dim_portfolio po on po.PortfolioName = lps.PortfolioName
  where lps.businessdate = (select max(businessdate) from `{env}-cdh`.derivative.dlegalprocessstatusdebtor)
'''
)
legalprocessstatusdebtor.createOrReplaceTempView('legalprocessstatusdebtor')

# COMMAND ----------

# MAGIC %md
# MAGIC # FinalTable

# COMMAND ----------

finaltable = spark.sql(
f'''
SELECT DISTINCT 
   dexp.businessdate
  ,DATE_FORMAT(dexp.businessdate, 'yyyy-MM') AS MonthSelection
--  ,dexp.PortfolioKey AS PortfolioID
  ,dexp.portfolioname AS PortfolioName
  ,dexp.exposurekey AS LoanBankCode
  ,dexp.LoanBankCode_NoPrefix
  ,dexp.cdi AS CDI
  ,coalesce(dexp.partytaxregistrationnumber, dexp.cdi) AS TaxNumber
  ,mcif.MultipleCIF
  ,trim(concat(trim(p2.LastNameGR), ' ', trim(p2.FirstNameGR))) AS PartyName
  --,dexp.partyname AS PartyName
  , e.MasterExposureKey
  ,IFNULL(a.channel, 'Unassigned') AS ChannelName
  ,IFNULL(CASE WHEN a2.ExposureKey IS NULL THEN a.Channel ELSE a2.channel END, 'Unassigned') AS SlaveDueToMasterChannelName
  ,IFNULL(a.Team, 'Unassigned') AS TeamName
  ,IFNULL(CASE WHEN a2.ExposureKey IS NULL THEN a.Team ELSE a2.Team END, 'Unassigned') AS SlaveDueToMasterTeamName
  ,CASE WHEN a.agentfullname in ('', ' ') or a.AgentFullName is null THEN  'Unassigned' ELSE a.agentfullname END AS AgentName
  ,la.LawyerPartyName as LegalChannelName 
  ,a.assignmentprocess AS AssignementProcess
  ,a.ProcessType AS ProcessType -- VP 09/09/2026   
  --,a.manager AS Head -- VP 21/09/2026
  -- ,a.supervisor AS Supervisor -- VP 21/09/2026
  ,p.EconomicActivity AS Industry
  ,p.segment AS CurrentSegment
  ,p.extrasegment AS PrimaryCustomerExtraSegment
  ,p.subsegment AS PrimaryCustomerSubSegment
  ,c.parentstrategystep AS Strategy
  ,c.strategystep AS StrategyStep --
  ,c.strategytype AS ReasonOfNOP
  ,c.DaysInStep AS DaysInStep
  ,dexp.outstandingtotalbalance AS OutstandingTotalBalance
  -- ,dexp.outstandingTotalBananceEUR AS OutstandingTotalBalanceEUR -- VP 21/09/2026
  ,dexp.onbalance AS OnBalance
  -- ,dexp.onbalanceeur AS OnBalanceEUR -- VP 21/09/2026
  ,dexp.offbalance AS OffBalance
  -- ,dexp.offbalanceeur AS OffBalanceEUR -- VP 21/09/2026
  ,dexp.totalprincipal AS Principal
  ,dexp.totalinterest AS Interest
  ,dexp.totalexpense AS Expense
  ,dexp.totalaccruals AS Accruals
  -- ,dexp.accrualseur AS AccrualsEUR -- VP 21/09/2026
  ,cast(dexp.gbv AS decimal(10,2)) AS GBV
  -- ,cast(dexp.gbveur AS decimal(10,2)) AS GBVEUR -- VP 21/09/2026
  , dexp.exposurestatus AS LoanStatus
  ,dexp.denounceddate AS DenouncedDate
  ,prod.Subproduct AS SubProductDesc   
  ,dexp.LoanIDQualco
  ,dexp.casecode  AS CaseCode
  ,dexp.Partykey AS PartyIDQualco
  -- ,cd.UpdateAddress_5d AS UpdateAddress_5d -- VP 21/09/2026
  -- ,cd.UpdatePhone_5d AS UpdatePhone_5d -- VP 21/09/2026
  ,DATEDIFF(day,lad.LastAttemptDate,dexp.businessdate) as DaysFromLastAttempt
  ,DATEDIFF(day,lad.LastRPCDate,dexp.businessdate) as DaysFromLastRPC
  ,bp.BalancePrimary  
  -- ,bp.BalancePrimaryEUR -- VP 21/09/2026
  ,p.affectedcategory AS AffectedBy
  ,dexp.lpseng AS AccountLegalProcessStatus
  ,CASE WHEN e.ExposureKey IS NOT NULL THEN 'Yes' ELSE 'No' END AS SlaveAccount -- CASE WHEN lower(c.Queue) LIKE '%slave%' THEN 'Yes' ELSE 'No' END AS SlaveAccount -- VP 07/09/2026
  ,cast(ls.implementationDate AS date) AS MaxImplementationDate 
  ,ps.CurrentMonthInstallmentDate AS CurrentMonthInstallmentDate 
  ,ps.CurrentMonthInstallmentAmount AS CurrentMonthInstallmentAmount 
  ,dexp.assetsegment AS AssetSegment
  ,dexp.HasGuarantor 
  ,i.promiseduedate AS PromiseDueDate 
  ,i.promiseamount AS PromiseAmount  
  , ROW_NUMBER() OVER (PARTITION BY dexp.exposurekey, dexp.businessdate ORDER BY  i.PromiseDueDate DESC) AS rn
  ,cola.Amount AS PaymentAmount
  ,cola.actualdate AS PaymentDate
  ,colcm.ActualPaymentsCurrentMonth 
  ,col3m.TotalPaymentAmountLast3M
  ,col6m.TotalPaymentAmountLast6M 
  ,col12m.TotalPaymentAmountLast12M 
  ,CASE WHEN ls.settlementStatus <> 'Cancelled' THEN ls.settlementType ELSE NULL END as LastSettlementType
  ,CASE WHEN ls.settlementStatus <> 'Cancelled' THEN ls.settlementStatus ELSE NULL END as LastSettlementStatus 
  ,ls.Bucket as LastSettlementBucket -- VP 07/09/2026
  ,CASE WHEN ls.SettlementStatus = 'Cancelled' THEN NULL 
    ELSE 
    CASE WHEN ls.RemainAmount < 0 THEN 0 
      ELSE ls.RemainAmount END 
    END AS LastSettlementRemainingAmount 
  , CASE WHEN ls.SettlementStatus <> 'Cancelled' THEN dexp.NextInstallmentAmount ELSE NULL END as InstallmentAmount 
  , CASE WHEN ls.SettlementStatus <> 'Cancelled' THEN dexp.NextInstallmentDate ELSE NULL END as InstallmentDate 
  ,CASE WHEN ls.SettlementStatus <> 'Cancelled'  THEN ls.approvaldate ELSE NULL END AS ApprovalDate 
  ,dexp.dueamount AS DueAmount

  , CAST(NULL AS STRING) AS CoCDebtorStatus /*CASE WHEN p2.segment like '%3869%' and p2.segment not like '%3869%Rej%'
						  then 'Not Applicable for CoC/N3869'
						  else (
							CASE WHEN cocl.InteractionType IS NULL AND cocd.InteractionType  IS NULL
								 THEN 'Not Applicable for CoC' 
								 WHEN cocl.InteractionType= 'Αποστολή 1ης επιστολής ΔΕΚ' AND cocd.InteractionType  IS NULL
								 THEN 'Αναμονή 2ης επιστολής'
								 WHEN cocl.InteractionType='Αποστολή 2ης επιστολής ΔΕΚ' AND cocd.InteractionType  IS NULL
								 THEN 'Αναμονή 3ης επιστολής'
								 ELSE ifnull(cocd.InteractionType,'N/A')
							END)
						  END		as CoCDebtorStatus		  --dexp.CoCStatus AS CoCDebtorStatus */

  ,CASE WHEN a3120.Max3120 > a3005.Max3005 THEN 'Yes' ELSE 'No' END AS PendingSubmission 
  ,CASE WHEN ls.settlementtype like '%Mercury Reignition L3869%' AND ls.settlementstatus='For Approval' THEN "Yes" ELSE "No" END AS NotApprovedDuePendingL3869Resignation
  ,li.interactiontypecode AS LastActivityCode
  ,li.interactiontype AS LastActivityName
  ,cast(li.interactiondate AS date) AS LastActivityCreatedDate
  ,cast(dexp.lpsdate AS date) AS LPSDate
  ,c.queue AS Queue 
  ,c.QueueTrimmed 
  ,dexp.loanactiveflag AS LoanActiveFlag -- VP 21/09/2026
  ,dexp.ContractNumber
  ,lps.legalprocessstatuslevel1
  ,lps.legalprocessstatuslevel2
  ,lps.operationallegalstatuslevel3 
 
FROM DExposure dexp
LEFT JOIN BalancePrimary bp ON bp.partyKey = dexp.PartyKey AND bp.businessdate=dexp.businessdate
LEFT JOIN MultipleCIF mcif ON mcif.PartyTaxRegistrationNumber=dexp.PartyTaxRegistrationNumber AND mcif.BusinessDate = dexp.BusinessDate
LEFT JOIN DParty p ON p.cdi=dexp.CDI AND  p.businessdate=dexp.businessdate
LEFT JOIN DParty p2 ON p2.PartyKey = dexp.PartyKey AND  p2.businessdate=dexp.businessdate
LEFT JOIN Assignment a ON regexp_replace(a.ExposureKey, '[^0-9]', '')= regexp_replace(dexp.ExposureKey, '[^0-9]', '') and regexp_replace(a.CDI, '[^0-9]', '')= regexp_replace(dexp.CDI, '[^0-9]', '') AND a.BusinessDate =  dexp.businessdate and a.PortfolioName = dexp.PortfolioName -- VP 30/06/2026
LEFT JOIN Exposure e ON e.ExposureKey = dexp.LoanBankCode_NoPrefix AND e.PortfolioName = dexp.PortfolioName 
LEFT JOIN Assignment a2 ON e.MasterExposureKey = regexp_replace(a2.ExposureKey, '[^0-9]', '') AND e.PortfolioName = a2.PortfolioName
LEFT JOIN CaseCode c ON c.casecodekey=dexp.casecode AND c.businessdate = dexp.businessdate 
LEFT JOIN CollectionActual cola ON cola.ExposurekeyPrefix=dexp.exposurekey  AND cola.BusinessDate = dexp.BusinessDate AND cola.rn = 1 
LEFT JOIN CollectionCurrentMonth colcm ON colcm.ExposurekeyPrefix=dexp.exposurekey AND colcm.businessdate = dexp.businessdate 
LEFT JOIN CollectionLast3M col3m ON col3m.ExposurekeyPrefix=dexp.exposurekey AND col3m.businessdate = dexp.businessdate 
LEFT JOIN CollectionLast6M col6m ON col6m.ExposurekeyPrefix=dexp.exposurekey AND col6m.businessdate = dexp.businessdate 
LEFT JOIN CollectionLast12M col12m ON col12m.ExposurekeyPrefix=dexp.exposurekey AND col12m.businessdate = dexp.businessdate
--LEFT JOIN SettlemEntexposure setl ON regexp_replace(setl.ExposureKey, '[^0-9]', '')= regexp_replace(dexp.ExposureKey, '[^0-9]', '') AND  setl.businessdate = dexp.businessdate -- VP 9/7/2026
LEFT JOIN Interaction i ON dexp.exposurekey = i.exposurekey and i.interactiondate <= dexp.BusinessDate and i.Attempt = 1 
-- LEFT JOIN CoCLetterStatus cocl ON cocl.ExposureKey = dexp.ExposureKey and  cocl.interactiondate <= dexp.BusinessDate and cocl.rank = 1
-- LEFT JOIN CoCDebtorStatus cocd ON cocd.ExposureKey = dexp.ExposureKey and  cocd.interactiondate <= dexp.BusinessDate and cocd.rank = 1
LEFT JOIN LastSettlement ls ON ls.exposurekey=dexp.exposurekey AND ls.month_end_business_date = dexp.businessdate AND ls.rn = 1
LEFT JOIN LastInteraction li ON li.cdi=dexp.cdi AND li.month_end_business_date = dexp.businessdate AND li.rn = 1
LEFT JOIN PaymentSchedule ps ON ps.exposurekey=dexp.exposurekey AND ps.businessdate = dexp.businessdate
LEFT JOIN LastAttemptDates lad on lad.cdi=dexp.cdi AND lad.month_end_business_date = dexp.BusinessDate
LEFT JOIN Activity3005 a3005 ON a3005.exposurekey=dexp.exposurekey AND a3005.month_end_business_date = dexp.businessdate
LEFT JOIN Activity3120 a3120 ON a3120.exposurekey=dexp.exposurekey AND a3120.month_end_business_date = dexp.businessdate
LEFT JOIN Product prod ON prod.productkey=dexp.productkey and dexp.BusinessDate = prod.BusinessDate
LEFT JOIN legalprocessstatusdebtor lps on lps.cdiprefix=dexp.cdi and lps.LegalProcessStatusLastChangeDate<=dexp.businessdate and lps.portfolioname=dexp.portfolioname and lps.rn=1
-- LEFT JOIN updatecontactdetails cd ON coalesce(dexp.PartyTaxRegistrationNumber, dexp.CDI) = cd.ParentPartyId and cd.rn = 1 -- VP 21/09/2026
LEFT JOIN LegalAction la ON dexp.ContractNumber = la.AgreementKey and la.rn = 1 -- VP 07/09/2026

'''
)
finaltable.createOrReplaceTempView("finaltable")

# COMMAND ----------

# MAGIC %md
# MAGIC # df_detailed

# COMMAND ----------

df_detailed = spark.sql('''
SELECT 
    BusinessDate,
    MonthSelection,
    PortfolioName,
    LoanBankCode,
    LoanBankCode_NoPrefix,
    CDI,
    TaxNumber,
    MultipleCIF,
    PartyName,
    SlaveAccount,
    MasterExposureKey,
    ChannelName,
    SlaveDueToMasterChannelName,
    TeamName,
    SlaveDueToMasterTeamName,
    AgentName,
    LegalChannelName,
    AssignementProcess,
    ProcessType,
    --Head, -- VP 21/09/2026
    --Supervisor, -- VP 21/09/2026
    Industry,
    CurrentSegment,
    PrimaryCustomerExtraSegment,
    PrimaryCustomerSubSegment,
    Strategy,
    StrategyStep,
    ReasonOfNOP,
    CAST(OutstandingTotalBalance AS DECIMAL(19,2)) AS OutstandingTotalBalance,
    -- CAST(OutstandingTotalBalanceEUR AS DECIMAL(19,2)) AS OutstandingTotalBalanceEUR, -- VP 21/09/2026
    CAST(OnBalance AS DECIMAL(19,2)) AS OnBalance,
    -- CAST(OnBalanceEUR AS DECIMAL(19,2)) AS OnBalanceEUR, -- VP 21/09/2026
    CAST(OffBalance AS DECIMAL(19,2)) AS OffBalance,
    --CAST(OffBalanceEUR AS DECIMAL(19,2)) AS OffBalanceEUR, -- VP 21/09/2026
    CAST(Principal AS DECIMAL(19,2)) AS Principal,
    CAST(Interest AS DECIMAL(19,2)) AS Interest,
    CAST(Expense AS DECIMAL(19,2)) AS Expense,
    CAST(Accruals AS DECIMAL(19,2)) AS Accruals,
    -- CAST(AccrualsEUR AS DECIMAL(19,2)) AS AccrualsEUR, -- VP 21/09/2026
    CAST(GBV AS DECIMAL(19,2)) AS GBV,
    -- CAST(GBVEUR AS DECIMAL(19,2)) AS GBVEUR, -- VP 21/09/2026
    LoanStatus,
    date_format(DenouncedDate, 'dd/M/yyyy') AS DenouncedDate,
    SubProductDesc,
    LoanIDQualco,
    CaseCode,
    PartyIDQualco,
    -- UpdateAddress_5d, -- VP 21/09/2026
    -- UpdatePhone_5d, -- VP 21/09/2026
    DaysFromLastAttempt,
    DaysFromLastRPC,
    CAST(BalancePrimary AS DECIMAL(19,2)) AS BalancePrimary,
    -- CAST(BalancePrimaryEUR AS DECIMAL(19,2)) AS BalancePrimaryEUR, -- VP 21/09/2026
    AffectedBy,
    AccountLegalProcessStatus,
    date_format(MaxImplementationDate, 'dd/M/yyyy') AS MaxImplementationDate,
    date_format(CurrentMonthInstallmentDate, 'dd/M/yyyy') AS CurrentMonthInstallmentDate,
    CAST(CurrentMonthInstallmentAmount AS DECIMAL(19,2)) AS CurrentMonthInstallmentAmount,
    AssetSegment,
    HasGuarantor,
    PromiseDueDate as PromiseDueDate, --date_format(cast(PromiseDueDate as date), 'dd/M/yyyy') as PromiseDueDate ,  
    CAST(PromiseAmount AS DECIMAL(19,2)) AS PromiseAmount,
    CAST(PaymentAmount AS DECIMAL(19,2)) AS PaymentAmount,
    date_format(PaymentDate, 'dd/M/yyyy') AS PaymentDate,
    CAST(ActualPaymentsCurrentMonth AS DECIMAL(19,2)) AS ActualPaymentsCurrentMonth,
    CAST(TotalPaymentAmountLast3M AS DECIMAL(19,2)) AS TotalPaymentAmountLast3M,
    CAST(TotalPaymentAmountLast6M AS DECIMAL(19,2)) AS TotalPaymentAmountLast6M,
    CAST(TotalPaymentAmountLast12M AS DECIMAL(19,2)) AS TotalPaymentAmountLast12M,
    LastSettlementType,
    LastSettlementStatus,
    LastSettlementBucket,
    CAST(LastSettlementRemainingAmount AS DECIMAL(19,2)) AS LastSettlementRemainingAmount,
    CAST(InstallmentAmount AS DECIMAL(19,2)) AS InstallmentAmount,
    date_format(InstallmentDate, 'dd/M/yyyy') AS InstallmentDate,
    date_format(ApprovalDate, 'dd/M/yyyy') AS ApprovalDate,
    CAST(DueAmount AS DECIMAL(19,2)) AS DueAmount,
    CoCDebtorStatus,
    PendingSubmission,
    NotApprovedDuePendingL3869Resignation,
    LastActivityCode,
    LastActivityName,
    date_format(LastActivityCreatedDate, 'dd/M/yyyy') AS LastActivityCreatedDate,
    date_format(LPSDate, 'dd/M/yyyy') AS LPSDate,
    Queue,
    CASE
        WHEN QueueTrimmed IN (
            '_Profits_Settlement_Αρση_Εγγ_Settl_Buc_01-03',
            '_Collection_Profits_Settlements_DNC_ext',
            '_Collection_Profits_Settlements_ext',
            '_Collection_Qualco_Settlements_ext',
            '_OCW_Collection_Assigned_ext',
            '_Collection_Assign_to_Ext',
            '_B4+_from_Collection_to_Offer_Assign_int',
            '_B4+_Profits_Settlement_to_Offer_Assign_int',
            '_B4+_Qualco_Settlement_to_Offer_Assign_int',
            '_Offer_CREV_50-100%_CA_int',
            '_Offer_CREV_0-50%_CA_int',
            '_Offer_Assign_ext',
            '_Offer_FA_int',
            '_Offer_FA_no_officer_int',
            '_Offer_CA_Personal_Ass_int',
            '_Offer_CA_int',
            '_No_Segment_(Balance <50)',
            '_Default'
        ) THEN 'Active'

        WHEN QueueTrimmed = '_Slave_Accounts' AND ProcessType IN ('Collection_Settlement', 'Collection_Settlement_Paladino','Collection_Settlement_Teleperformance', 'Offer') THEN 'Active'

        WHEN QueueTrimmed IN (
        ' Vulnerable Debtor_Certification_',
        ' Vulnerable Debtor_Application_',
        '_Arotron_Legal_Phase_cases',
        '_Slave_Accounts',
        '_L3869_Accepted_SR',
        '_Profits_Settlement_almost_Paid_Off',
        '_Qualco_Settlement_almost_Paid_Off',
        '_Profits_Fulfilled_Αρση_Εγγύησης',
        '_Profits_Fulfilled_Settlement',
        '_Qualco_Fulfilled_Settlements',
        '_L3869',
        '_Profits_Settlement_Pending_Implementation',
        '_Profits_Settlement_Αρση_Εγγύησης_Settl_Buc00',
        '_Profits_Settlement_Bucket_00_DNC',
        '_Qualco_Settlement_Bucket_00_DNC',
        '_Profits_Settlement_Grace<10€_DNC',
        '_Profits_Settlement_under_11_Dpds_DNC',
        '_Profits_Settlement_Bucket_00',
        '_Qualco_Settlement_Bucket_00',
        '_Profits_Settlement_Grace<10€',
        '_Profits_Settlement_under_11_Dpds',
        '_Current_Bucket_00',
        '_Grace<10€',
        '_Under_11_Dpds',
        '_SR_Non-Delinquent_Profits_Settlements',
        '_SR_Non-Delinquent_Qualco_Settlements',
        '_Bankruptcy_queue_non_final_stages',
        '_Bankruptcy_final_stage_completion',
        '_OCW_queue_non_final_stages',
        '_OCW_final_stage_completion',
        '_No_Treatment_state_guaranteed'
    ) THEN 'Passive'

        ELSE 'Migrated'
    END AS ActiveOrPassiveAssignment,
    ContractNumber,
    LoanActiveFlag,
    legalprocessstatuslevel1,
    legalprocessstatuslevel2,
    operationallegalstatuslevel3,
    CAST(DaysInStep AS INT) AS DaysInStep,
    CAST(NULL AS STRING) AS WorkableFlag, 
    CAST(NULL AS STRING) AS NonWorkableReason, 
    getdate() as IngestionTimeStamp 
FROM finaltable
where rn = 1 or rn is null
'''
)
df_detailed.createOrReplaceTempView("df_detailed") 

# COMMAND ----------

# MAGIC %md
# MAGIC # Export

# COMMAND ----------

from export_utils import export_data
from helpers import archive_existing_files
from pyspark.sql import functions as F

df_detailed_exp = (
    spark.table("df_detailed")
      #   .filter(F.col("BusinessDate") == run_date)
         .filter(F.col("PortfolioName") == portfolioName))

# Archive existing files before writing new export
os.makedirs(report_output, exist_ok=True)
archive_existing_files(report_output)

# Export using shared utility (auto xlsx/csv based on row count)
export_data(df_detailed_exp, "DetailedPortfolio", run_date, report_output, output_file_prefix)
