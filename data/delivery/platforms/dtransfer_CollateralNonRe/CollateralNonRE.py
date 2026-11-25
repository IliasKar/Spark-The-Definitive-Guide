# Databricks notebook source
# DBTITLE 1,Parameters & setup
import sys
from pyspark.sql import functions as F

dbutils.widgets.removeAll()
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")

sys.path.append("/Workspace/Shared")
import aws_utils

env = aws_utils.get_env_prefix()

business_date_param = dbutils.widgets.get("run_date").strip()

if business_date_param:
    run_date = business_date_param
else:
    run_date = str(
        spark.table(f"`{env}-cdh`.primaryactive.exposure")
        .filter(F.col("SourceSystem") == "profits")
        .agg({"BusinessDate": "max"})
        .collect()[0][0]
    )

TARGET_TABLE = f"`{env}-cdh-delivery`.`it`.`dtransfer_collateralNonRE`"

print(f"env={env}  run_date={run_date}  target={TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Build CollateralNonRE DataFrame
from pyspark.sql import functions as F, Window

ACTIVE = ["ΕΝΕΡΓΗ", "Ενεργή", "Ενεργη", "ενεργή", "ενεργη", "ΟΥΣΙΑΣ", "Ενεργό", "N/A"]

# 1. collateral — NonRE

coll = (
    spark.table(f"`{env}-cdh`.primaryactive.collateral")
    .filter(
        (F.col("BusinessDate") == run_date)
        & F.col("NonRealEstateKey").isNotNull()
        & F.col("CollateralStatus").isin(ACTIVE)
        & F.col("NonRealEstateCollateralStatus").isin(["Ενεργή"])
        & F.col("PortfolioName").isNotNull()
    )
)

# 1b. portfolio lookup
pf_lookup = (
    spark.table(f"`{env}-cdh`.primaryactive.portfolio")
    .filter(F.col("portfolio_id_thesis").isNotNull())
    .select(
        F.col("portfolio_id_thesis").alias("pf_thesis_id"),
        F.col("PortfolioId").alias("pf_PortfolioId"),
        F.col("TiresiasName").alias("pf_TiresiasName"),
        F.coalesce(
            F.col("vendor_code_qualco").cast("string"),
            F.regexp_extract(F.col("PortfolioKey"), r"(\d+)", 1)
        ).alias("pf_vendorCode")
    )
    .dropDuplicates(["pf_thesis_id"])
)

coll = (
    coll
    .join(pf_lookup, F.col("PortfolioKey").cast("int") == F.col("pf_thesis_id"), "left")
    .drop("pf_thesis_id")
)

# 2. nonrealestate 
nre = (
    spark.table(f"`{env}-cdh`.primaryactive.nonrealestate")
    .filter(F.col("BusinessDate") == run_date)
    .select(
        F.col("NonRealEstateKey").alias("nre_key"),
        F.trim(F.col("NonRealEstateItemDescription")).alias("detItemDescr"),
        F.col("ISIN"),
        F.trim(F.col("NonRealEstateDescription")).alias("det_descr"),
    )
)

# 3. join + dedup
w_nre = Window.partitionBy(
    "BusinessDate", "PortfolioKey",
    "CollateralOldKey", "CollateralCode",
    "AgreementKey", "CollateralCurrency", "CollateralStatusChangeDate",
    "CollateralAmount", "ISIN",
    "Category", "SubCategory"
).orderBy(
    F.desc(F.coalesce(F.col("detItemDescr"), F.lit(""))),
    F.desc(F.coalesce(F.col("det_descr"),     F.lit("")))
)


joined = (
    coll
    .join(
        nre,
        F.col("NonRealEstateKey").cast("string") == F.col("nre_key"),
        "inner"
    )
    .withColumn("rn", F.row_number().over(w_nre))
    .filter(F.col("rn") == 1)
    .drop("rn", "nre_key")
)

# 4. final select
df = joined.select(
    F.lit(run_date).cast("timestamp").alias("EtlLoadingDatetime"),
    F.col("BusinessDate").alias("ReferenceDate"),
    F.col("pf_PortfolioId").alias("PortfolioId"),
    F.col("pf_TiresiasName").alias("Portfolio"),
    F.col("pf_vendorCode").alias("vendorCode"),
    F.coalesce(F.col("CollateralOldKey"), F.concat(F.lit("N/A - "), F.col("CollateralCode"))).alias("OldCollateralId"),
    F.col("AgreementKey").alias("AgreementNumber"),
    F.col("CollateralCurrency").alias("Currency"),
    F.col("CollateralStatusChangeDate"),
    F.col("CollateralAmount").alias("collateralAmount"),
    F.col("CollateralAmount").cast("decimal(19,2)").alias("Det_collateralAmount"), 
    F.col("detItemDescr"),
    F.col("ISIN").alias("isin"),
    F.lit("1").alias("lien_type"),
    F.lit("1900-01-01").cast("date").alias("latestEvaluationDate"),
    F.col("det_descr"),
    F.col("Category"),
    F.col("SubCategory"),
    F.col("CollateralCode").alias("thesisCollaterId"),
)

row_count = df.count()
assert row_count > 0, f"0 rows for run_date={run_date} — aborting write"
print(f"Records to write: {row_count:,}")

# COMMAND ----------

# DBTITLE 1,Write to Delta table
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Written {row_count:,} rows to {TARGET_TABLE}  (ReferenceDate={run_date})")
