# Databricks notebook source
# DBTITLE 1,Parameters & setup
import sys
from pyspark.sql import functions as F, Window

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

TARGET_VAT  = f"`{env}-cdh-delivery`.`it`.`dtransfer_ref_vat`"
TARGET_DEBT = f"`{env}-cdh-delivery`.`it`.`dtransfer_ref_debt`"

print(f"env={env}  run_date={run_date}")
print(f"ref_vat  → {TARGET_VAT}")
print(f"ref_debt → {TARGET_DEBT}")

# COMMAND ----------

# DBTITLE 1,Build ref_vat DataFrame

# ref_vat — Customer VAT / party reference per portfoli

# 1. Portfolio lookup 
w_pf = Window.partitionBy("PortfolioName").orderBy("PortfolioId", F.desc("TiresiasName"))

portfolio = (
    spark.table(f"`{env}-cdh`.primaryactive.portfolio")
    .filter(F.col("unit_code_profits").isNotNull())
    .withColumn("rn", F.row_number().over(w_pf))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        "PortfolioId", "PortfolioName",
        F.coalesce(F.col("TiresiasName"), F.col("PortfolioName")).alias("TiresiasName"),
        F.coalesce("vendor_code_qualco", "spv_qualco_AB", "unit_code_profits").alias("vendorCode"),
        "AssignmentDate", "Prefix",
    )
)

# 2. dparty 
w_dp = Window.partitionBy("CDI").orderBy("CDI")
dparty = (
    spark.table(f"`{env}-cdh`.derivativeactive.dparty")
    .filter(F.col("businessdate") == run_date)
    .withColumn("rn", F.row_number().over(w_dp))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("CDI").alias("dp_CDI"),
        F.col("PartyKeyProfits").cast("int").alias("cust_id"),
        F.col("L3869Flag").cast("boolean").alias("IsL3869Flag"),
    )
)

# 3. Exposure base
exposure_all = (
    spark.table(f"`{env}-cdh`.primaryactive.exposure")
    .filter((F.col("BusinessDate") == run_date) & (F.col("IsActiveFlag") == True))
)

# 4. Build ref_vat
w_vat = Window.partitionBy("CDI", "PortfolioId").orderBy("ExposureId")

ref_vat_df = (
    exposure_all.alias("e")
    .join(portfolio.alias("p"), F.col("e.PortfolioName") == F.col("p.PortfolioName"), "inner")
    .filter(F.col("p.PortfolioId") != 18)  
    .join(dparty, F.col("e.CDI") == F.col("dp_CDI"), "left")
    .withColumn("rn", F.row_number().over(w_vat))
    .filter(F.col("rn") == 1)
    .select(
        F.lit(run_date).cast("date").alias("datadate"),
        F.col("p.PortfolioId"),
        F.col("p.TiresiasName").alias("Portfolio"),
        F.col("p.vendorCode").cast("int").alias("vendorCode"),
        F.col("e.CDI"),
        F.col("e.PartyTaxRegistrationNumber").alias("TaxNumber"),
        F.col("cust_id"),
        F.col("p.Prefix").alias("prefix"),
        F.col("IsL3869Flag"),
        F.lit(run_date).cast("date").alias("EtlLoadingDate"),
    )
    .distinct()
)
vat_count = ref_vat_df.count()
assert vat_count > 0, f"ref_vat is empty for run_date={run_date}"
print(f"ref_vat: {vat_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Build ref_debt DataFrame
# ref_debt 

# 1. Exposure base 
w_exp = Window.partitionBy("ExposureKey", "CDI", "AgreementKey").orderBy("ExposureId")

exposure = (
    spark.table(f"`{env}-cdh`.primaryactive.exposure")
    .filter(
        (F.col("BusinessDate") == run_date)
        & (F.col("IsActiveFlag") == True)
        & (F.col("SourceSystem") == "profits")
    )
    .withColumn("rn", F.row_number().over(w_exp))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 2. Product lookup
w_pr = Window.partitionBy("ProductKey").orderBy("ProductKey")
product = (
    spark.table(f"`{env}-cdh`.primaryactive.product")
    .filter(F.col("BusinessDate") == run_date)
    .withColumn("rn", F.row_number().over(w_pr))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("ProductKey").alias("pr_ProductKey"),
        F.col("ProductDescription").alias("PrdDescription"),
        F.col("SubProduct").alias("SprdDescription"),
    )
)

# 3. Settlement lookup
settlement = (
    spark.table(f"`{env}-cdh`.primaryactive.settlementexposure")
    .filter(
        (F.col("BusinessDate") == run_date)
        & (F.col("IsActiveFlag") == 1)
        & (F.col("isPrimarySettlementFlag") == True)
    )
    .select(
        F.col("ExposureKey").alias("s_ExposureKey"),
        F.col("SettlementExposureKey"),
        F.col("SettlementStatus"),
        F.col("BucketCode").cast("int").alias("s_BucketCode"),
        F.col("OverdueDate").alias("s_OverdueDate"),
    )
)

# 4. dexposure lookup 
w_de = Window.partitionBy("de_numeric_key", "de_AgreementKey").orderBy(
    F.when(F.col("SourceSystem") == "profits", 0).otherwise(1),
    F.col("ExposureDerivativeID")
)

dexposure = (
    spark.table(f"`{env}-cdh`.derivativeactive.dexposure")
    .filter(F.col("businessdate") == run_date)
    .withColumn("de_numeric_key", F.regexp_extract("ExposureKey", r"([0-9].*)", 1))
    .withColumn("de_AgreementKey", F.col("AgreementKey"))
    .withColumn("rn", F.row_number().over(w_de))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        "de_numeric_key", "de_AgreementKey",
        F.col("BucketCode").alias("de_BucketCode"),
        F.col("GBVFunctionResult").cast("decimal(19,2)").alias("de_GBV"),
        F.col("ExposureSourceSystemKeyProfits").alias("de_loanid"),
        F.col("OnBalance").alias("de_OnBalance"),
        F.col("OffBalance").alias("de_OffBalance"),
        F.col("DaysPastDueFinal").alias("de_DPD"),
        F.col("LPSDate").alias("de_LPSDate"),
        F.col("Overduedate").alias("de_Overduedate"),
        F.col("DenouncedDate").alias("de_DenouncedDate"),
        F.col("ExposureStatus").alias("de_ExposureStatus"),
        F.col("DueAccrued").alias("de_DueAccrued"),
        F.col("Dueinterest").alias("de_Dueinterest"),
        F.col("DueUnrealizedinterest").alias("de_DueUnrealizedinterest"),
        F.col("Penaltyinterest").alias("de_Penaltyinterest"),
        F.col("PenaltyUnrealizedinterest").alias("de_PenaltyUnrealizedinterest"),
        F.col("AssetSegment").alias("de_AssetSegment"),
        F.col("RestructuringPEEDate").alias("de_RestructuringPEEDate"),
        F.col("LastRestructuringDate").alias("de_LastRestructuringDate"),
        F.col("BankingUnitCode").alias("de_BankingUnitCode"),
        F.col("SlaveFlag").alias("de_SlaveFlag"),
    )
)

# 5. LPS agreement-level
w_lps = Window.partitionBy("AgreementKey").orderBy(F.col("LegalProcessStatusLastChangeDate").desc())
lps_agr = (
    spark.table(f"`{env}-cdh`.derivativeactive.dlegalprocessstatusagreement")
    .filter(F.col("BusinessDate") == run_date)
    .withColumn("rn", F.row_number().over(w_lps))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("AgreementKey").alias("lps_AgreementKey"),
        F.col("LegalProcessStatusLevel2").alias("LPS_value"),
    )
)

# 6. Exchange rates
exr = (
    spark.table(f"`{env}-cdh`.reference.exchangerates")
    .filter(F.col("BusinessDate") == run_date)
    .select(
        F.col("OriginalCurrency").alias("exr_Currency"),
        F.col("ExchangeRate").alias("exr_Rate"),
    )
)

# 7. JOIN all
joined = (
    exposure.alias("e")
    .join(portfolio.alias("p"), F.col("e.PortfolioName") == F.col("p.PortfolioName"), "inner")
    .join(product, F.col("e.ProductKey") == F.col("pr_ProductKey"), "left")
    .join(settlement, F.col("e.ExposureKey") == F.col("s_ExposureKey"), "left")
    .join(
        dexposure,
        (F.regexp_extract(F.col("e.ExposureKey"), r"([0-9].*)", 1) == F.col("de_numeric_key"))
        & (F.col("e.AgreementKey") == F.col("de_AgreementKey")),
        "left"
    )
    .join(lps_agr, F.col("e.AgreementKey") == F.col("lps_AgreementKey"), "left")
    .join(exr, F.col("e.Currency") == F.col("exr_Currency"), "left")
)

# 8. Final select
ref_debt_df = joined.select(
    F.lit(run_date).cast("date").alias("DataDate"),
    F.col("p.PortfolioId"),
    F.col("p.TiresiasName").alias("Portfolio"),
    F.col("p.vendorCode").cast("int").alias("vendorCode"),
    F.col("e.PartyTaxRegistrationNumber").alias("TaxNumber"),
    F.col("e.AgreementKey").alias("AgreementNumber"),
    F.when(F.col("e.PartyRelationshipType") == "Primary Owner", "Primary Debtor")
     .when(F.col("e.PartyRelationshipType") == "Co-Owner", "Co-Debtor")
     .when(F.col("e.PartyRelationshipType") == "Guarantor", "Guarantor")
     .otherwise(F.coalesce(F.col("e.PartyRelationshipType"), F.lit("Primary Debtor")))
     .alias("Relation"),
    F.col("e.ExposureKey").alias("AccountNumber"),
    F.col("e.CDI"),
    F.lit(None).cast("string").alias("UcId"),
    F.col("e.PartyName"),
    F.col("e.MechanismCode").alias("AccMechanism"),
    F.substring(F.col("pr_ProductKey"), 1, 5).alias("ProfitsProduct"),
    F.col("PrdDescription"),
    F.col("SprdDescription"),
    F.col("e.Currency"),
    F.coalesce("de_AssetSegment", F.col("e.AssetSegment")).alias("AssetSegment"),
    # DenouncedDate: -90 offset for status 3,4
    F.when(
        F.col("e.ExposureStatusCode").cast("int").isin([3, 4]),
        F.date_add(F.coalesce("de_DenouncedDate", F.col("e.Denounceddate")), -90)
    ).alias("DenouncedDate"),
    F.col("e.ExposureStatusCode").cast("int").alias("LoanStatus"),
    F.coalesce("de_ExposureStatus", F.col("e.ExposureStatus")).alias("LoanStatusLit"),
    F.col("e.OutstandingTotalBalance"),
    F.col("de_OnBalance").alias("OnBalance"),
    F.col("de_OffBalance").alias("OffBalance"),
    F.coalesce("de_GBV", F.col("e.OutstandingTotalBalance").cast("decimal(19,2)")).alias("GBV"),
    # OverdueInterest from dexposure
    (F.coalesce("de_DueAccrued", F.lit(0)) + F.coalesce("de_Dueinterest", F.lit(0))
     + F.coalesce("de_DueUnrealizedinterest", F.lit(0))
     + F.coalesce("de_Penaltyinterest", F.lit(0))
     + F.coalesce("de_PenaltyUnrealizedinterest", F.lit(0))).alias("OverdueInterest"),
    (F.coalesce("de_DueAccrued", F.lit(0)) + F.coalesce("de_Dueinterest", F.lit(0))
     + F.coalesce("de_Penaltyinterest", F.lit(0))).alias("OverdueInterestNonUrl"),
    (F.coalesce("de_DueUnrealizedinterest", F.lit(0))
     + F.coalesce("de_PenaltyUnrealizedinterest", F.lit(0))).alias("OverdueInterestUrl"),
    F.coalesce("de_BucketCode", F.col("e.BucketCode")).alias("SourceBucketId"),
    F.coalesce("de_DPD", F.col("e.DaysPastDue")).alias("DPD"),
    F.coalesce("de_Overduedate", F.col("e.Overduedate")).alias("OverdueDate"),
    F.when(F.col("s_ExposureKey").isNotNull(), 1).otherwise(0).alias("HasActiveSettlementFlag"),
    F.col("SettlementExposureKey").alias("settlementCode"),
    F.col("SettlementStatus").alias("settlementStatus"),
    F.coalesce("de_BucketCode", F.col("e.Bucket").cast("int")).cast("string").alias("BucketId"),
    F.col("s_BucketCode").alias("SettlementBucketId"),
    F.when(
        F.col("s_ExposureKey").isNotNull() & (F.col("s_BucketCode") > 0),
        F.datediff(F.lit(run_date).cast("date"), F.col("s_OverdueDate"))
    ).when(F.col("s_ExposureKey").isNotNull(), 0).alias("SettlementDPD"),
    F.col("s_OverdueDate").alias("SettlementOverdueDate"),
    F.col("p.AssignmentDate").cast("date").alias("DebtDate"),
    F.when(
        F.coalesce("de_RestructuringPEEDate", F.col("e.RestructuringPEEDate")).isNotNull(), 1
    ).otherwise(0).alias("IsRestructuredFlag"),
    F.when(
        F.coalesce("de_LastRestructuringDate", F.col("e.LastRestructuringDate")) != F.lit("1753-01-01").cast("date"),
        F.coalesce("de_LastRestructuringDate", F.col("e.LastRestructuringDate"))
    ).alias("LastRestructuredDate"),
    F.coalesce("LPS_value", F.lit("N/A")).alias("LPS"),
    F.col("de_LPSDate").alias("LPSDate"),
    F.coalesce("exr_Rate", F.lit(1)).cast("decimal(12,6)").alias("ExchangeRate"),
    F.col("de_loanid").alias("loanid"),
    F.col("e.PartyKey").cast("int").alias("cust_id"),
    F.coalesce("de_BankingUnitCode", F.col("e.BankingUnitCode")).alias("FKGD_BANK_SECTOR"),
    F.col("de_SlaveFlag").cast("boolean").alias("pr_isSlave"),
    F.lit(run_date).cast("date").alias("ETLLoadingDate"),
)

debt_count = ref_debt_df.count()
assert debt_count > 0, f"ref_debt is empty for run_date={run_date}"
print(f"ref_debt: {debt_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Write to Delta
# Write ref_vat + ref_debt to Delta (full overwrite per run_date)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{env}-cdh-delivery`.`it`")

print(f"ref_vat  : {vat_count:,} rows")
print(f"ref_debt : {debt_count:,} rows")

ref_vat_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_VAT)
ref_debt_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_DEBT)

print(f"\nDone for run_date={run_date}")
print(f"   ref_vat  → {vat_count:,} rows → {TARGET_VAT}")
print(f"   ref_debt → {debt_count:,} rows → {TARGET_DEBT}")
