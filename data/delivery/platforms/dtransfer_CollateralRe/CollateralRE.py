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

TARGET_TABLE = f"`{env}-cdh-delivery`.`it`.`dtransfer_collateralRE`"

print(f"env={env}  run_date={run_date}  target={TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Build CollateralRE DataFrame
from pyspark.sql import functions as F, Window

ACTIVE = ["ΕΝΕΡΓΗ", "Ενεργή", "Ενεργη", "ενεργή", "ενεργη", "ΟΥΣΙΑΣ", "Ενεργό", "N/A"]

# 1. collateral_re

w_coll = Window.partitionBy(
    F.coalesce(F.col("CollateralOldKey"), F.col("CollateralKey")),
    "CollateralKey",
    "AgreementKey", "OwnershipKey",
    "InitialLegalOrder"
).orderBy("CollateralId")

collateral_re = (
    spark.table(f"`{env}-cdh`.primaryactive.collateral")
    .filter(
        F.col("RealEstateKey").isNotNull()
        & (F.col("BusinessDate") == run_date)
        & F.col("CollateralStatus").isin(ACTIVE)
        & F.col("RealEstateCollateralStatus").isin(ACTIVE)
        & F.col("PortfolioName").isNotNull()
    )
    .withColumn("rn", F.row_number().over(w_coll))
    .filter(F.col("rn") == 1)
    .drop("rn")
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

collateral_re = (
    collateral_re
    .join(pf_lookup, F.col("PortfolioKey").cast("int") == F.col("pf_thesis_id"), "left")
    .drop("pf_thesis_id")
)

# 2. realestatebuilding
w_reb = Window.partitionBy("RealEstateKey").orderBy(
    F.when(
        (F.col("RealEstateBuildingType") == "Real Estate") & F.col("SourceTableName").isNull(), 0
    ).otherwise(1),
    "RealEstateBuildingKey"
)

reb = (
    spark.table(f"`{env}-cdh`.primaryactive.realestatebuilding")
    .filter(F.col("BusinessDate") == run_date)
    .withColumn("rn", F.row_number().over(w_reb))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("RealEstateKey").alias("reb_key"),
        F.col("RealEstateCategory").alias("ParentCategory"),
        F.col("RealEstateSubCategory").alias("PropertyCategory"),
        F.col("RealEstateDescription").alias("PropertyDescription"),
        F.col("RealEstateMarketValue").cast("decimal(19,2)").alias("MarketValue"),
        F.coalesce(
            F.col("RealEstateValueDate").cast("date"),
            F.col("RealEstateEvaluationDate").cast("date"),
            F.lit("1900-01-01").cast("date")
        ).alias("LastValuationDate"),
        F.col("RealEstateCity").alias("city"),
        F.col("RealEstateArea").alias("area"),
        F.col("RealEstatePostalCode").alias("ZipCode"),
        F.col("RealEstateAddress1").alias("Address"),
        F.coalesce(F.col("RealEstateDistrict"), F.lit("N/A")).alias("Prefecture"),  
        F.col("RealEstateLandRegistryCode").alias("public_book"),
        F.col("RealEstateUsageCodeTypeCode").alias("UsageCode"),
    )
)

# 3. dparty 
pty = (
    spark.table(f"`{env}-cdh`.derivativeactive.dparty")
    .filter(
        (F.col("businessdate") == run_date)
        & F.col("PartyKeyThesis").isNotNull()
    )
    .select(
        F.col("PartyKeyThesis").cast("string").alias("dp_thesis_key"),
        F.col("TaxRegistrationNumber").alias("OwnerTaxNumber"),
        F.trim(
            F.coalesce(
                F.col("LegalNameGR"),
                F.concat(F.col("FirstNameGR"), F.lit("  "), F.col("LastNameGR"))
            )
        ).alias("PropertyOwnerName"),
        F.col("BirthDate").cast("date").alias("BirthDate"),
    )
    .dropDuplicates(["dp_thesis_key"])
)

# 4. Join & project
df = (
    collateral_re
    .join(reb,
          collateral_re["RealEstateKey"].cast("string") == reb["reb_key"],
          "inner")
    .join(pty,
          collateral_re["OwnerPartyKey"].cast("string") == pty["dp_thesis_key"],
          "left")
    .select(
        F.lit(run_date).cast("timestamp").alias("EtlLoadingDatetime"),
        collateral_re["BusinessDate"].alias("ReferenceDate"),
        F.col("pf_PortfolioId").alias("PortfolioId"),
        F.col("pf_TiresiasName").alias("Portfolio"),
        F.col("pf_vendorCode").alias("vendorCode"),
        F.coalesce(collateral_re["CollateralOldKey"], F.concat(F.lit("N/A - "), collateral_re["CollateralKey"])).alias("OldCollateralId"),
        collateral_re["SubCategory"].alias("CollateralType"),
        collateral_re["CollateralStatus"],
        collateral_re["CollateralAmount"],
        collateral_re["CollateralKey"].alias("AssetId"),
        collateral_re["AgreementKey"].alias("AgreementNumber"),
        collateral_re["RealEstateKey"].alias("PropertyKey"),
        collateral_re["InitialLegalOrder"].alias("lien"),
        reb["ParentCategory"],
        reb["PropertyCategory"],
        reb["PropertyDescription"],
        reb["MarketValue"],
        reb["LastValuationDate"],
        reb["city"],
        reb["area"],
        reb["ZipCode"],
        reb["Address"],
        reb["Prefecture"],
        collateral_re["OwnershipKey"].alias("PropertyOwnershipId"),
        pty["OwnerTaxNumber"],
        pty["PropertyOwnerName"],
        collateral_re["OwnershipPercentage"],
        collateral_re["RealTitleDescription"].alias("OwnershipType"),
        pty["BirthDate"],
        reb["public_book"],
        reb["UsageCode"],
    )
)

row_count = df.count()
assert row_count > 0, f"0 rows for run_date={run_date} -- aborting write"
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
