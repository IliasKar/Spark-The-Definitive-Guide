# Databricks notebook source
# DBTITLE 1,Initialization
import os
import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/HoistDebtJson/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../aws_utils")
    sys.path.append("../01-utils")

from aws_utils import get_env_prefix, get_env_fullname, connect_to_aws_and_access_ssm, get_scope_key_names, get_aws_credentials
from helpers import get_validated_eom_run_date, archive_existing_files
import boto3

# COMMAND ----------

# DBTITLE 1,Parameters
env = get_env_prefix()
env_name = get_env_fullname()

dbutils.widgets.text("run_date", "")
run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))

# SSM: resolve export base path
secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)
aws_key = aws_keys[0] if aws_keys else None
aws_secret_key = get_aws_credentials(secrets_scope, aws_key)
ssm_client = boto3.client("ssm", region_name="eu-central-1",
                          aws_access_key_id=aws_key,
                          aws_secret_access_key=aws_secret_key)

EXPORT_BASE_PATH = connect_to_aws_and_access_ssm(
    "/reports/exports/s3_main_path", ssm_client, aws_secret_key
).replace("envprefix", env)

print(f"run_date: {run_date} | env: {env} | export_path: {EXPORT_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,Resolve previous month end date
prev_eom_date = str(spark.sql(f"""
    SELECT PreviousMonthLastWorkingDate AS prev_eom
    FROM `{env}-cdh`.reference.ref_calendar
    WHERE CalendarDate = '{run_date}'
""").first()["prev_eom"])

print(f"run_date: {run_date} | prev_eom_date: {prev_eom_date}")

# COMMAND ----------

# DBTITLE 1,Resolve Hoist portfolios dynamically
hoist_portfolios = spark.sql(f"""
    SELECT DISTINCT PortfolioName, Prefix
    FROM `{env}-cdh`.primary.portfolio
    WHERE BusinessDate = '{run_date}'
      AND Short_Name_Asset_Owner = 'Hoist'
""").collect()

portfolio_names = [r["PortfolioName"] for r in hoist_portfolios]
prefix_map = {r["PortfolioName"]: r["Prefix"] for r in hoist_portfolios}
portfolio_filter = ", ".join(f"'{p}'" for p in portfolio_names)

print(f"Hoist portfolios ({len(portfolio_names)}): {portfolio_names}")

# COMMAND ----------

# DBTITLE 1,Query builder
def build_debts_query(run_date):
    """Build the debts query for a given reference date."""
    return f"""
    WITH realestatebuilding_deduped AS (
        SELECT DISTINCT
            RealEstateKey, BusinessDate, RealEstateCategory,
            RealEstateEvaluationDate, RealEstateMarketValue
        FROM `{env}-cdh`.primary.realestatebuilding
        WHERE BusinessDate = '{run_date}'
          AND RealEstateActive = 'Active'
          AND RealEstateEvaluationDate <= '{run_date}'
          AND SourceSystem = 'thesis'
    ),
    collateral_ranked AS (
        SELECT
            col.AgreementKey, reb.RealEstateCategory,
            reb.RealEstateEvaluationDate, reb.RealEstateMarketValue,
            ROW_NUMBER() OVER (PARTITION BY col.AgreementKey ORDER BY reb.RealEstateMarketValue DESC) AS val_ranking
        FROM `{env}-cdh`.derivative.dcollateral col
        INNER JOIN realestatebuilding_deduped reb
            ON col.RealEstateKey = reb.RealEstateKey AND col.BusinessDate = reb.BusinessDate
        WHERE col.BusinessDate = '{run_date}'
          AND col.CollateralStatusCode = '\u0395\u03bd\u03b5\u03c1\u03b3\u03ae'
          AND col.SourceSystem = 'thesis'
    ),
    collateral_agg AS (
        SELECT AgreementKey,
            CASE WHEN MAX(CASE WHEN val_ranking = 1 THEN RealEstateCategory END) IN ('Land', 'Residential', 'Commercial')
                 THEN MAX(CASE WHEN val_ranking = 1 THEN RealEstateCategory END) ELSE 'Other'
            END AS `Type of property collateral`,
            'EUR' AS `Currency of collateral value`,
            CAST(SUM(RealEstateMarketValue) AS DECIMAL(19,2)) AS `Property/collateral value`,

            MAX(RealEstateEvaluationDate) AS `Property/Collateral Valuation Date`
        FROM collateral_ranked
        GROUP BY AgreementKey
    )
    SELECT
        dex.ExposureKey AS `Debt ID`,
        SUBSTRING(dex.AgreementKey, 4,
            CASE WHEN LOCATE('-', dex.AgreementKey) > 0
                 THEN LOCATE('-', dex.AgreementKey) ELSE LENGTH(dex.AgreementKey)
            END - 4) AS `Original Debt ID`,
        CASE WHEN sec.CDI IS NOT NULL THEN CONCAT(dex.PortfolioName, ' SECURED') ELSE dex.PortfolioName END AS `Portfolio ID`,
        CASE WHEN cre.AgreementKey IS NOT NULL THEN 'SECURED' ELSE 'UNSECURED' END AS `Asset class`,
        cre.`Type of property collateral`,
        cre.`Currency of collateral value`,
        cre.`Property/collateral value`,
        cre.`Property/Collateral Valuation Date`,
        DATE_FORMAT(dex.OpenDate, 'yyyyMMdd') AS `Origination date`,
        DATE_FORMAT(dex.DefaultDate, 'yyyyMMdd') AS `Default date`,
        dex.Currency AS `Currency`,
        dex.InitialAssignmentAmount AS `Total Claim Amount`,
        CASE WHEN dex.IsActiveFlag = false THEN 'Y' ELSE 'N' END AS `Closed`,
        CASE WHEN dex.IsActiveFlag = false THEN DATE_FORMAT(dex.ClosedDate, 'yyyyMMdd') END AS `Closure Date`,
        CASE WHEN dex.IsActiveFlag = false THEN dex.ClosedReason END AS `Closure Reason`,
        dex.OutstandingTotalBalance AS `Remaining Total Claim`,
        dex.Principal + dex.DuePrincipal AS `Remaining Claim PRINCIPAL`,
        dex.Interest + dex.UnrealizedInterest + dex.DueInterest + dex.DueUnrealizedInterest
            + dex.PenaltyInterest + dex.PenaltyUnrealizedInterest AS `Remaining Claim INTEREST`,
        dex.OutstandingTotalBalance - (dex.Principal + dex.DuePrincipal)
            - (dex.Interest + dex.UnrealizedInterest + dex.DueInterest + dex.DueUnrealizedInterest
               + dex.PenaltyInterest + dex.PenaltyUnrealizedInterest) AS `Remaining Claim OTHER`
    FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date('{run_date}') dex
    LEFT JOIN `d-cdh`.reference.hoist_secure_cdi sec
        ON dex.CDI = sec.CDI AND dex.PortfolioName = sec.PortfolioName
    LEFT JOIN collateral_agg cre ON dex.AgreementKey = cre.AgreementKey
    WHERE dex.PortfolioName IN ({portfolio_filter})
    """

# COMMAND ----------

# DBTITLE 1,Execute queries & compute debt_df
current_month = spark.sql(build_debts_query(run_date))
current_month.createOrReplaceTempView("current_month")

previous_month = spark.sql(build_debts_query(prev_eom_date))
previous_month.createOrReplaceTempView("previous_month")

debt_df = spark.sql("""
    SELECT * FROM current_month
    EXCEPT
    SELECT * FROM previous_month
""")

# Add reporting date as Remaining Total Claim Date
from pyspark.sql.functions import lit, col
debt_df = debt_df.withColumn("Remaining Total Claim Date", lit(run_date.replace("-", "")))
debt_df.createOrReplaceTempView("debt_df")

# COMMAND ----------

# DBTITLE 1,Export JSON per portfolio
import json

run_date_fmt = run_date.replace("-", "")
output_dir = EXPORT_BASE_PATH + f"investorreports/monthlyhoist/debt/{run_date_fmt}/"
os.makedirs(output_dir, exist_ok=True)

archive_existing_files(output_dir)

for portfolio_name, prefix in prefix_map.items():
    # Filter debt_df for this portfolio (Portfolio ID may include " SECURED" suffix)
    pdf = debt_df.filter(
        col("Portfolio ID").startswith(portfolio_name)
    ).orderBy("Closed").toPandas()

    if pdf.empty:
        print(f"No data for {portfolio_name} ({prefix}), skipping.")
        continue

    # Convert to JSON array (include nulls like FOR JSON PATH, INCLUDE_NULL_VALUES)
    records = json.loads(pdf.to_json(orient="records", date_format="iso"))
    json_content = json.dumps(records, ensure_ascii=False)

    filename = f"{prefix}Debt_{run_date_fmt}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json_content)
    print(f"Exported {filename} ({len(pdf)} rows)")

print(f"\nAll JSON files written to {output_dir}")
