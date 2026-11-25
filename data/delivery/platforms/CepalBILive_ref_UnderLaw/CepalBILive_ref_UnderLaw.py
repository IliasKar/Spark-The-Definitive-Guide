# Databricks notebook source
# DBTITLE 1,Configuration & Date Variables
import sys
sys.path.append("/Workspace/Shared")
import aws_utils
from pyspark.sql import functions as F

env = aws_utils.get_env_prefix()

TARGET_TABLE = f"`{env}-cdh-delivery`.it.cepalbilive_ref_underlaw"

business_date_param = dbutils.widgets.get("business_date").strip()

if business_date_param:
    datadate = business_date_param
else:
    datadate = str(
        spark.sql(f"SELECT MAX(BusinessDate) AS bd FROM `{env}-cdh`.primaryactive.exposure")
        .collect()[0].bd
    )

# COMMAND ----------

# DBTITLE 1,Read Source & Transform
# =============================================================================
# Read & combine underlaw data (incremental load for specific datadate)
# =============================================================================
# Dual-source strategy:
#   - N3869:     primaryactive.underlaw    (InLawFlag=1) → 100% CDI, PortfolioKey
#   - Non-N3869: derivativeactive.dunderlaw (InLawFlag=1) → 100% CDI, PortfolioKey
#
# Why two sources?
#   primaryactive.underlaw has more N3869 rows (170K vs 128K in dunderlaw)
#   but has InLawFlag=NULL / CDI=NULL / PortfolioKey=NULL for non-N3869.
#   dunderlaw fills all those fields for non-N3869 via its derivative ETL.
# =============================================================================

UNDERLAW_TABLE  = f"`{env}-cdh`.primaryactive.underlaw"
PARTY_TABLE     = f"`{env}-cdh`.primaryactive.party"
DUNDERLAW_TABLE = f"`{env}-cdh`.derivativeactive.dunderlaw"

# --- Part A: N3869 from primaryactive.underlaw (InLawFlag=1) ---
df_n3869 = (
    spark.table(UNDERLAW_TABLE)
    .filter(
        (F.col("BusinessDate") == datadate) &
        (F.col("InLawFlag") == 1)
    )
    .select(
        F.col("PortfolioKey").alias("PortfolioId"),
        F.col("CDI"),
        # On-prem Classification returns '3869' not 'N3869'
        F.regexp_replace(F.col("UnderLawType"), "^N", "").alias("Law"),
        # NBSP normalization + LEFT(50) truncation to match on-prem varchar(50)
        F.substring(
            F.trim(F.regexp_replace(F.col("ApplicationStatus"), "\u00A0", "")),
            1, 50
        ).alias("ApplicationStatus"),
        F.col("ApplicationStatusDate").cast("date"),
    )
)

# --- Part B: Non-N3869 from derivativeactive.dunderlaw (InLawFlag=1) ---
df_non_n3869 = (
    spark.table(DUNDERLAW_TABLE)
    .filter(
        (F.col("BusinessDate") == datadate) &
        (F.col("InLawFlag") == 1) &
        (F.col("UnderLawType") != "N3869")
    )
    .select(
        F.col("PortfolioKey").alias("PortfolioId"),
        F.col("CDI"),
        # Build Law with sub-type for N4738: "N4738/2020-Εξωδικαστικός μηχανισμός"
        F.when(
            (F.col("LawSystem").isNotNull()) & (F.col("LawSystem") != ""),
            F.concat_ws("-", F.col("UnderLawType"), F.col("LawSystem"))
        ).otherwise(F.col("UnderLawType")).alias("Law"),
        # LEFT(50) truncation to match on-prem varchar(50)
        F.substring(F.col("ApplicationStatus"), 1, 50).alias("ApplicationStatus"),
        F.col("ApplicationStatusDate").cast("date"),
    )
)

# --- UNION both law groups ---
df_underlaw = df_n3869.unionByName(df_non_n3869)

# --- Party table for taxNumber (LEFT JOIN on CDI, same BusinessDate) ---
df_party_tax = (
    spark.table(PARTY_TABLE)
    .filter(F.col("BusinessDate") == datadate)
    .select(
        F.col("CDI").alias("party_CDI"),
        F.col("TaxRegistrationNumber").alias("taxNumber")
    )
    .dropDuplicates(["party_CDI"])
)

# --- Final result: LEFT JOIN for taxNumber + add datadate ---
df_result = (
    df_underlaw
    .join(df_party_tax, df_underlaw["CDI"] == df_party_tax["party_CDI"], "left")
    .drop("party_CDI")
    .select(
        "PortfolioId", "CDI", "taxNumber", "Law",
        "ApplicationStatus", "ApplicationStatusDate",
        F.lit(datadate).cast("date").alias("datadate")
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Delta table
(    df_result.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"datadate = '{datadate}'")
    .saveAsTable(TARGET_TABLE)
)
