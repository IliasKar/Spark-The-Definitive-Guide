# Databricks notebook source
import os
import sys
sys.path.append("../aws_utils")
 
import aws_utils
 
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from datetime import date
from functools import reduce

env = aws_utils.get_env_prefix()

# Parameters
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# Reference tables
ref_payment_exception = f"`{env}-cdh`.reference.settlement_payment_exception"
ref_initialization_exposure_info = f"`{env}-cdh`.reference.initializationexposureinfo"
ref_migration_snapshot = f"`{env}-cdh`.reference.settlement_migration_snapshot"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 0: Load Tables

# COMMAND ----------

# Settlement exposure for run_date
src_settlementexposure = spark.sql(f"""
    SELECT SettlementKey, PortfolioKey,
           SettlementAmount, PortfolioName,
           DiscountAmount, NumOfinstalments,
           ExpiredDate, ProposedAmount,
           ExposureKey, CaseCode, CDI, AgreementKey,
           SettlementStatusCode, SettlementStatus,
           SubmissionDate, SourceSystem,
           LastUpdatedDate AS StatusChangeDate,
           CreatedDate
    FROM `{env}-cdh`.primaryactive.settlementexposure
    WHERE SourceSystem IN ('profits', 'qca') AND BusinessDate = '{run_date}'
""")

# Full history of settlementexposure for the calculation of dates
src_settlementexposure_history = spark.sql(f"""
    SELECT SettlementKey, SourceSystem, SettlementStatus,
           BusinessDate, LastUpdatedDate AS StatusChangeDate,
           SubmissionDate
    FROM `{env}-cdh`.primaryactive.settlementexposure
    WHERE SourceSystem IN ('profits', 'qca')
""").distinct()

# all dexposures ever — latest snapshot per ExposureKey up to run_date.
src_exposure = spark.sql(f"""
    SELECT * EXCEPT (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY ExposureKey ORDER BY BusinessDate DESC
               ) AS rn
        FROM (
            SELECT ExposureKey, DiasNumber, OriginationAmount, CDI,
                   Principal, DuePrincipal, SourceSystem, BusinessDate,
                   AgreementKey, CaseCode
            FROM `{env}-cdh`.derivativeactive.dexposure
            WHERE BusinessDate <= '{run_date}'
              AND COALESCE(JustificationCode, 0) != 43023

            UNION ALL

            SELECT ExposureKey, DiasNumber, OriginationAmount, CDI,
                   Principal, DuePrincipal, SourceSystem, BusinessDate,
                   AgreementKey, CaseCode
            FROM `{env}-cdh`.derivativearchive.dexposure
            WHERE BusinessDate <= '{run_date}'
              AND BusinessDate < DATE'2024-01-30'
              AND COALESCE(JustificationCode, 0) != 43023
        )
    )
    WHERE rn = 1
""")

# settlement payment plans per exposure
src_paymentschedule = spark.sql(f"""
    SELECT SettlementKey, InstallmentNumber,
           InstallmentDate, InstallmentAmount, SourceSystem
    FROM `{env}-cdh`.primaryactive.paymentschedule
    WHERE SourceSystem IN ('profits', 'qca') AND SettlementKey IS NOT NULL AND BusinessDate = '{run_date}'
""")

# matched collections to match to payment plans
src_collection = spark.sql(f"""
    SELECT CollectionKey, ExposureKey, DiasNumber,
           DiasChannel, Amount,ActualDate
    FROM `{env}-cdh`.primaryactive.collection
    WHERE FinalReason = 'Matched' AND BusinessDate = '{run_date}'
""")

# pre-migration state for settlements that were migrated from QCSor to QCA & Profits.
# Contains dates, flags, and amounts that cannot be derived from the cloud history because
# the history only starts after migration.
src_migration_snapshot = spark.table(ref_migration_snapshot)

src_portfolio = spark.sql(f"""
    SELECT Prefix, PortfolioName
    FROM `{env}-cdh`.primaryactive.portfolio
    WHERE BusinessDate = '{run_date}'
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Status Category Derivation

# COMMAND ----------

# Map settlement status to a status category
STATUS_TO_CATEGORY = {
    # QCA statuses
    "Working": "working",
    "For Approval": "pending approval",
    "Running": "running",
    "Partially Fulfilled": "final",
    "Fulfilled": "final",
    "Not Fulfilled": "final",
    "Cancelled": "cancel",
    "Cancelled - Communication Failed": "cancel",
    "Cancelled - Client Rejection": "cancel",
    "Cancelled - Internal Rejection": "cancel",
    "Out of Collection": "final",
    "1st Approval": "pending approval",
    "2nd Approval": "pending approval",
    "Additional 1": "cancel",
    "Additional 2": "cancel",
    "Watchlist": "cancel",
    "Broken": "final",
    "Cancelled - Merged CDI": "cancel",
    "Cancelled - Common Customer": "cancel",
    "Cancelled - Merged PRF": "cancel",
    "Cancelled - Galaxy Migration": "cancel",
    "Cancelled - Cosmos Migration": "cancel",
    "Cancelled - SPVII Migration": "cancel",
    "Cancelled - GXR Migration": "cancel",
    "Cancelled - Hermes Migration": "cancel",
    "Cancelled - CTS Migration": "cancel",
    "Cancelled - DOMUS Migration": "cancel",
    "Cancelled - GAIA Migration": "cancel",
    "Cancelled - ASTREA Migration": "cancel",
    "Client Cancelled (Inactive SOR MGR)": "cancel",
    "For Approval (Inactive SOR MGR)": "pending approval",
    "Implemented (Inactive SOR MGR)": "final",
    "Internal Cancelled (Inactive SOR MGR)": "cancel",
    "Non-Active (Inactive SOR MGR)": "final",
    "Not Fulfilled (Inactive SOR MGR)": "final",
    "Partially Fulfilled (Inactive SOR MGR)": "final",
    "Pending Implementation (Inactive SOR MGR)": "final",
    "Rejected (Inactive SOR MGR)": "rejected",
    "Working (Inactive SOR MGR)": "final",
    # Profits statuses
    "PENDING FOR IMPLEMENTATION": "approved",
    "IMPLEMENTED": "running",
    "FULFILLED": "final",
    "NOT FULFILLED": "final",
    "LOAN CLOSED": "final",
    "CANCELLED-AUTO": "cancel",
    "CANCELLED-MANUAL": "cancel",
}

category_expr = reduce(
    lambda expr, kv: expr.when(F.col("SettlementStatus") == kv[0], kv[1]),
    STATUS_TO_CATEGORY.items(),
    F.when(F.col("SettlementStatus").isNull(), "unknown")
).otherwise("unknown")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Load Settlement Loans

# COMMAND ----------
portfolio_lookup = src_portfolio.select(
    F.col("PortfolioName").alias("_pf_name"),
    F.col("Prefix").alias("_pf_prefix"))

src_settlementexposure = (
    src_settlementexposure.alias("se")
    .join(portfolio_lookup, F.col("se.PortfolioName") == F.col("_pf_name"), "left")
    .withColumn("ExposureKeyFull",
        F.when(F.col("se.ExposureKey").isNull(), F.col("se.ExposureKey"))
         .when(F.col("_pf_prefix").isNull(), F.col("se.ExposureKey"))
         .when(F.substring(F.col("se.ExposureKey"), 1, 3) == F.col("_pf_prefix"), F.col("se.ExposureKey"))
         .otherwise(F.concat(F.col("_pf_prefix"), F.col("se.ExposureKey"))))
    .drop("ExposureKey", "_pf_name", "_pf_prefix")
    .withColumnRenamed("ExposureKeyFull", "ExposureKey")
)

# Materialize the exposure function result
src_exposure = src_exposure.cache()
src_exposure.count() #force cache

# split by source system
# Exposure-joined subset: QCA + Profits with ExposureKey
settlementexposure_exp = src_settlementexposure.filter(
    (F.col("SourceSystem") == "qca") |
    ((F.col("SourceSystem") == "profits") & F.col("ExposureKey").isNotNull())
)
# Agreement subset: Profits without ExposureKey. Joins on AgreementKey.
settlementexposure_profits_agr = src_settlementexposure.filter((F.col("SourceSystem") == "profits") & (F.col("ExposureKey").isNull()))

# --- QCA + Profits Exposure Path (both join on ExposureKey) ---
df_loan_exp = (
    settlementexposure_exp.alias("se")
    .join(src_exposure.alias("ex"),
          (F.col("se.ExposureKey") == F.col("ex.ExposureKey")),
          "left")
    .select(
        F.col("se.SettlementKey").alias("SettlementKey"),
        F.col("se.SourceSystem").alias("SourceSystem"),
        F.col("se.CaseCode").alias("CaseCode"), 
        F.col("se.ExposureKey").alias("ExposureKey"),
        F.col("ex.AgreementKey").alias("AgreementKey"),
        F.col("ex.DiasNumber").alias("DiasNumber"),
        F.col("ex.Principal").alias("Principal"),
        F.col("ex.DuePrincipal").alias("DuePrincipal"),
        F.col("ex.OriginationAmount").alias("OriginationAmount"),
    )
)

# --- Profits Agreement Path --- 
df_loan_agreement = (
    settlementexposure_profits_agr.alias("sea")
    .join(src_exposure.alias("ex"),
          (F.col("sea.AgreementKey") == F.col("ex.AgreementKey")),
          "left")
    .select(
        F.col("sea.SettlementKey").alias("SettlementKey"),
        F.col("sea.SourceSystem").alias("SourceSystem"),
        F.lit(None).cast("string").alias("CaseCode"),
        F.col("ex.ExposureKey").alias("ExposureKey"),
        F.col("sea.AgreementKey").alias("AgreementKey"),
        F.col("ex.DiasNumber").alias("DiasNumber"),
        F.col("ex.Principal").alias("Principal"),
        F.col("ex.DuePrincipal").alias("DuePrincipal"),
        F.col("ex.OriginationAmount").alias("OriginationAmount"),
    )
)

# --- Union settlemet subsets ---
df_loan = df_loan_exp.unionByName(df_loan_agreement)

df_loan.cache()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Load Payment Plan

# COMMAND ----------

# Aggregate to settlement level
df_plan = (
    src_paymentschedule.groupBy(
        F.col("SettlementKey"),
        F.col("SourceSystem"),
        F.col("InstallmentNumber"),
        F.col("InstallmentDate"),
    ).agg(
        F.sum("InstallmentAmount").alias("InstallmentAmount"),
    )
)

# running cumulative amount to later determine paid installments 
window_accumulate = Window.partitionBy("SettlementKey", "SourceSystem").orderBy("InstallmentNumber")
df_plan = df_plan.withColumn("InstallmentAmountAccum", F.sum("InstallmentAmount").over(window_accumulate))

# Materialize the plan once so the downstream aggregations reuse it
df_plan = df_plan.cache()
df_plan.count() #force cache

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Load Settlement Evaluation & Enrich with installments

# COMMAND ----------

# Deduplicate to settlement level: one row per (SettlementKey, SourceSystem)
settlementexposure_dedup = (
    src_settlementexposure.withColumn("rn", F.row_number().over(
        Window.partitionBy("SettlementKey", "SourceSystem").orderBy("ExposureKey")))
    .filter(F.col("rn") == 1).drop("rn")
    .withColumn("StatusCategory", category_expr)
)

# Plan-derived fields — computed in a single groupBy pass over df_plan.
plan_agg = (
    df_plan.groupBy("SettlementKey", "SourceSystem").agg(
        F.min(F.when(F.col("InstallmentDate") >= F.lit(run_date),
                     F.struct("InstallmentNumber", "InstallmentDate", "InstallmentAmount"))
              ).alias("_next"),
        F.max(F.when(F.col("InstallmentDate") < F.lit(run_date),
                     F.struct("InstallmentNumber", "InstallmentDate", "InstallmentAmount"))
              ).alias("_current"),
        F.min("InstallmentDate").alias("FirstInstalmentDate"),
        F.max("InstallmentDate").alias("LastInstalmentDate"),
        F.count("*").alias("PlanInstallmentCount"),
    )
)

next_installment = plan_agg.select(
    "SettlementKey", "SourceSystem",
    F.col("_next.InstallmentAmount").alias("NextInstallmentAmount"),
    F.col("_next.InstallmentDate").alias("NextInstallmentDate"),
    F.col("_next.InstallmentNumber").alias("NextInstallmentNumber"),
)

current_installment = plan_agg.select(
    "SettlementKey", "SourceSystem",
    F.col("_current.InstallmentAmount").alias("CurrentInstallmentAmount"),
)

first_plan_date = plan_agg.select(
    "SettlementKey", "SourceSystem",
    "FirstInstalmentDate", "LastInstalmentDate", "PlanInstallmentCount",
)

# Account counts (distinct ExposureKeys per settlement — not CDI duplicates)
account_counts = df_loan.select("SettlementKey", "SourceSystem", "ExposureKey").dropDuplicates().groupBy("SettlementKey", "SourceSystem").agg(
    F.count("*").alias("NumOfAccounts"))

# Join all plan-derived fields to settlement
df_eval = (
    settlementexposure_dedup.alias("sexp")
    .join(next_installment.alias("next_inst"),
          (F.col("next_inst.SettlementKey") == F.col("sexp.SettlementKey")) &
          (F.col("next_inst.SourceSystem") == F.col("sexp.SourceSystem")), "left")
    .join(current_installment.alias("curr_inst"),
          (F.col("curr_inst.SettlementKey") == F.col("sexp.SettlementKey")) &
          (F.col("curr_inst.SourceSystem") == F.col("sexp.SourceSystem")), "left")
    .join(first_plan_date.alias("first_plan"),
          (F.col("first_plan.SettlementKey") == F.col("sexp.SettlementKey")) &
          (F.col("first_plan.SourceSystem") == F.col("sexp.SourceSystem")), "left")
    .join(account_counts.alias("acct"),
          (F.col("acct.SettlementKey") == F.col("sexp.SettlementKey")) &
          (F.col("acct.SourceSystem") == F.col("sexp.SourceSystem")), "left")
    .select(
        F.col("sexp.SettlementKey"),
        F.col("sexp.SourceSystem"),
        F.col("sexp.PortfolioKey"),
        F.col("sexp.SettlementAmount"),
        F.col("sexp.DiscountAmount"),
        F.coalesce(F.col("sexp.NumOfinstalments"), F.col("first_plan.PlanInstallmentCount")).alias("NumOfinstalments"),
        F.col("sexp.SettlementStatusCode"),
        F.col("sexp.SettlementStatus"),
        F.col("sexp.StatusChangeDate"),
        F.col("sexp.StatusCategory"),
        F.coalesce(F.col("sexp.ExpiredDate"), F.col("first_plan.LastInstalmentDate")).alias("ExpiredDate"),
        F.col("sexp.ProposedAmount"),
        F.col("sexp.CreatedDate"),
        F.col("sexp.SubmissionDate"),
        F.col("next_inst.NextInstallmentAmount").alias("NextInstallmentAmount"),
        F.col("next_inst.NextInstallmentDate").alias("NextInstallmentDate"),
        F.when(F.col("next_inst.NextInstallmentNumber").isNull(), F.coalesce(F.col("sexp.NumOfinstalments"), F.col("first_plan.PlanInstallmentCount")))
         .otherwise(F.col("next_inst.NextInstallmentNumber") - 1).alias("PastInstalmentsNum"),
        F.col("curr_inst.CurrentInstallmentAmount"),
        F.col("first_plan.FirstInstalmentDate"),
        F.col("first_plan.LastInstalmentDate"),
        F.col("acct.NumOfAccounts"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Date Stamps from History

# COMMAND ----------

# Derive StatusCategory on full history
history_with_cat = src_settlementexposure_history.withColumn("StatusCategory", category_expr)

# All date stamps in a single pass
all_dates = (
    history_with_cat
    .groupBy("SettlementKey", "SourceSystem")
    .agg(
        F.min(F.when(F.col("StatusCategory").isin("approved", "running"), F.col("StatusChangeDate"))).alias("ApprovalDate"),
        F.min(F.when(F.col("StatusCategory") == "running", F.col("StatusChangeDate"))).alias("ImplementationDate"),
        F.min(F.when(F.col("StatusCategory") == "cancel", F.col("StatusChangeDate"))).alias("CancelledDate"),
        F.min(F.when(F.col("StatusCategory") == "rejected", F.col("StatusChangeDate"))).alias("RejectedDate"),
        F.min(F.when(F.col("StatusCategory").isNotNull(), F.col("StatusChangeDate"))).alias("HistorySubmissionDate"),
    )
)

# Join all date stamps to eval in one join
df_eval = (
    df_eval
    .join(all_dates, ["SettlementKey", "SourceSystem"], "left")
)

# --- Override with migration snapshot dates ---
# Migrated settlements do not have enough history in the cloud to derive dates.
# Migration snapshot dates take priority over history-derived values when they exist.
# Dedup to settlement level (snapshot is at loan level — one row per settlement, exposure)
migration_dates = (
    src_migration_snapshot
    .dropDuplicates(["SettlementKey", "NewSourceSystem"])
    .select(
        F.col("SettlementKey"),
        F.col("NewSourceSystem").alias("SourceSystem"),
        F.col("ApprovalDate").alias("MigApprovalDate"),
        F.col("ImplementationDate").alias("MigImplementationDate"),
        F.col("SubmissionDate").alias("MigSubmissionDate"),
        F.col("CancelledDate").alias("MigCancelledDate"),
        F.col("RejectedDate").alias("MigRejectedDate"),
        F.col("InitialSettlementKey").alias("MigInitialSettlementKey"),
        F.col("PrincipalProposedAmount").alias("MigPrincipalProposedAmount"),
        F.col("AlreadyPaidAmount").alias("MigAlreadyPaidAmount"),
        F.col("AmountCalculatedAsPaid").alias("MigAmountCalculatedAsPaid"),
        F.col("CovidSubsidyExpectedAmount").alias("MigCovidSubsidyExpectedAmount"),
        F.col("PoolName"),
        F.col("IsMigratedFlag").cast("int").alias("IsMigratedFlag"),
        F.col("CreatedDate").alias("MigCreatedDate"),
        F.col("BalloonAmount").alias("MigBalloonAmount"),
    )
)

df_eval = (
    df_eval.join(migration_dates, ["SettlementKey", "SourceSystem"], "left")
    .withColumn("ApprovalDate", F.coalesce(F.col("MigApprovalDate"), F.col("ApprovalDate")))
    .withColumn("ImplementationDate", F.coalesce(F.col("MigImplementationDate"), F.col("ImplementationDate")))
    .withColumn("SubmissionDate", F.coalesce(F.col("MigSubmissionDate"), F.col("SubmissionDate"), F.col("HistorySubmissionDate")))
    .withColumn("CancelledDate", F.coalesce(F.col("MigCancelledDate"), F.col("CancelledDate")))
    .withColumn("RejectedDate", F.coalesce(F.col("MigRejectedDate"), F.col("RejectedDate")))
    .withColumn("IsMigratedFlag", F.coalesce(F.col("IsMigratedFlag"), F.lit(0)))
    .withColumn("CreatedDate", F.coalesce(F.col("MigCreatedDate"), F.col("CreatedDate")))
    .withColumn("BalloonAmount", F.col("MigBalloonAmount"))
    .drop("MigApprovalDate", "MigImplementationDate", "MigSubmissionDate",
          "MigCancelledDate", "MigRejectedDate", "MigCreatedDate", "MigBalloonAmount", "HistorySubmissionDate")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Activity Status & Auto-Cancel Duplicates

# COMMAND ----------

# Determine IsActiveFlag from StatusCategory
inactive_categories = ["pending approval", "cancel", "final", "rejected", "working"]
df_eval = df_eval.withColumn("IsActiveFlag",
    F.when(F.col("StatusCategory").isin(inactive_categories), 0).otherwise(1)
)

# Auto-cancel duplicates: same ExposureKey + CDI with multiple active settlements
# When the same person has 2+ active settlements on the same loan, keep only the newest.
# The older settlement's AutoCancelledDate = the newer settlement's ImplementationDate.
settlement_cdi = (
    src_settlementexposure
    .select("SettlementKey", "SourceSystem", "CDI")
    .dropDuplicates(["SettlementKey", "SourceSystem"])
)

eval_with_exposure = (
    df_eval.filter(F.col("IsActiveFlag") == 1)
    .join(df_loan.select("SettlementKey", "SourceSystem", "ExposureKey"),
          ["SettlementKey", "SourceSystem"], "inner")
    .join(settlement_cdi, ["SettlementKey", "SourceSystem"], "left")
)

# Rank by (ExposureKey, CDI): newest settlement wins
ranked = (
    eval_with_exposure
    .withColumn("rnk", F.rank().over(
        Window.partitionBy("ExposureKey", "CDI")
        .orderBy(F.desc("SettlementKey"))))
)

# The winner (rnk=1) — get their ImplementationDate to stamp on the losers
winners = (
    ranked.filter(F.col("rnk") == 1)
    .select("ExposureKey", "CDI", F.col("ImplementationDate").alias("WinnerImplementationDate"))
)

# The losers (rnk>1) — get the winner's ImplementationDate as their AutoCancelledDate
losers = (
    ranked.filter(F.col("rnk") > 1)
    .join(winners, ["ExposureKey", "CDI"])
    .select("SettlementKey", "SourceSystem", "WinnerImplementationDate")
    .distinct()
    .withColumn("IsAutoCancelled", F.lit(True))
)

# Apply auto-cancel
df_eval = (
    df_eval.join(losers, ["SettlementKey", "SourceSystem"], "left")
    .withColumn("IsActiveFlag",
        F.when(F.col("IsAutoCancelled") == True, 0).otherwise(F.col("IsActiveFlag")))
    .withColumn("AutoCancelledDate",
        F.when(F.col("IsAutoCancelled") == True, F.col("WinnerImplementationDate")).otherwise(None))
    .drop("IsAutoCancelled", "WinnerImplementationDate")
)

# IsPrimarySettlement: newest active settlement per ExposureKey wins
primary_flag = (
    df_eval.filter(F.col("IsActiveFlag") == 1)
    .join(df_loan.select("SettlementKey", "SourceSystem", "ExposureKey"),
          ["SettlementKey", "SourceSystem"], "inner")
    .withColumn("rnk", F.row_number().over(
        Window.partitionBy("ExposureKey")
        .orderBy(F.desc("SettlementKey"))))
    .select("SettlementKey", "SourceSystem", "ExposureKey",
            F.when(F.col("rnk") == 1, 1).otherwise(0).alias("IsPrimarySettlement"))
)

df_loan = df_loan.join(primary_flag, ["SettlementKey", "SourceSystem", "ExposureKey"], "left")
df_loan = df_loan.withColumn("IsPrimarySettlement", F.coalesce(F.col("IsPrimarySettlement"), F.lit(0)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Materialize to break lineage

# COMMAND ----------

# Run-scoped intermediary tables
run_suffix = run_date.replace("-", "_")
internal_settlement_eval = f"`{env}-cdh`.derivativeinternal.settlementevaluation_{run_suffix}"
internal_settlement_loan = f"`{env}-cdh`.derivativeinternal.settlementloan_{run_suffix}"

df_eval.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(internal_settlement_eval)
df_loan.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(internal_settlement_loan)

df_loan.unpersist()
src_exposure.unpersist()

df_eval = spark.table(internal_settlement_eval)
df_loan = spark.table(internal_settlement_loan)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Payment Matching

# COMMAND ----------

# All settlements with ApprovalDate get payments matched
eval_for_payments = df_eval.filter(
    F.col("ApprovalDate").isNotNull()
).select("SettlementKey", "SourceSystem", "ApprovalDate")

# Normal payments (DiasChannel IS NULL)
collection_normal = src_collection.filter(F.col("DiasChannel").isNull())

normal_pay = (
    collection_normal.alias("collection")
    .join(df_loan.alias("settlement_loan"),
          F.regexp_replace(F.col("settlement_loan.ExposureKey"), "^[A-Za-z]+", "")
          == F.regexp_replace(F.col("collection.ExposureKey"), "^[A-Za-z]+", ""))
    .join(eval_for_payments.alias("eval"),
          (F.col("eval.SettlementKey") == F.col("settlement_loan.SettlementKey")) &
          (F.col("eval.SourceSystem") == F.col("settlement_loan.SourceSystem")))
    .filter(
        (F.col("collection.ActualDate") >= F.col("eval.ApprovalDate")))
    .withColumn("rnk", F.rank().over(
        Window.partitionBy("collection.CollectionKey")
        .orderBy(F.desc("settlement_loan.SettlementKey"))))
    .filter(F.col("rnk") == 1)
    .select(F.col("eval.SettlementKey"), F.col("eval.SourceSystem"),
            F.col("collection.Amount"), F.col("collection.ActualDate"))
)

# DIAS payments (DiasChannel IS NOT NULL)
collection_dias = src_collection.filter(F.col("DiasChannel").isNotNull())
settlement_loan_dias = df_loan.filter(F.col("DiasNumber").isNotNull())

dias_pay = (
    collection_dias.alias("collection")
    .join(settlement_loan_dias.alias("settlement_loan"), F.col("collection.DiasNumber") == F.col("settlement_loan.DiasNumber"))
    .join(eval_for_payments.alias("eval"),
          (F.col("eval.SettlementKey") == F.col("settlement_loan.SettlementKey")) &
          (F.col("eval.SourceSystem") == F.col("settlement_loan.SourceSystem")))
    .filter(F.col("collection.ActualDate") >= F.col("eval.ApprovalDate"))
    .withColumn("rnk", F.rank().over(
        Window.partitionBy("collection.CollectionKey")
        .orderBy(F.desc("settlement_loan.SettlementKey"))))
    .filter(F.col("rnk") == 1)
    .select(F.col("eval.SettlementKey"), F.col("eval.SourceSystem"),
            F.col("collection.Amount"), F.col("collection.ActualDate"))
)

# Combine normal + DIAS, aggregate per settlement
all_payments = normal_pay.unionAll(dias_pay)
total_payments = (
    all_payments.groupBy("SettlementKey", "SourceSystem")
    .agg(
        F.sum("Amount").alias("TotalPaymentAmount"),
        F.max("ActualDate").alias("LastPaymentDate"))
)

# Join payments to eval
df_eval = df_eval.join(total_payments, ["SettlementKey", "SourceSystem"], "left")

# --- Payment Exceptions ---
# Some settlements have payments that are missing from collection
# Add these before calculating financials.
payment_exceptions = (
    spark.table(ref_payment_exception)
    .groupBy("SettlementKey")
    .agg(
        F.sum("Amount").alias("ExcAmount"),
        F.max("ActualDate").alias("ExcLastDate"),
    )
)

# Build final TotalPaymentAmount as matched collections + migration amounts + payment exceptions
df_eval = (
    df_eval.join(payment_exceptions, ["SettlementKey"], "left")
    .withColumn("TotalPaymentAmount",
               F.coalesce(F.col("TotalPaymentAmount"), F.lit(0))
               + F.coalesce(F.col("MigAlreadyPaidAmount"), F.lit(0))
               + F.coalesce(F.col("MigAmountCalculatedAsPaid"), F.lit(0))
               + F.coalesce(F.col("MigCovidSubsidyExpectedAmount"), F.lit(0))
               + F.coalesce(F.col("ExcAmount"), F.lit(0)))
    .withColumn("LastPaymentDate",
        F.when(F.col("ExcLastDate").isNotNull(),
               F.greatest(F.coalesce(F.col("LastPaymentDate"), F.col("ExcLastDate")), F.col("ExcLastDate")))
        .otherwise(F.col("LastPaymentDate")))
    .drop("ExcAmount", "ExcLastDate")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8: Financial Calculations

# COMMAND ----------

# TotalPaid helper
total_paid = F.coalesce(F.col("TotalPaymentAmount"), F.lit(0))

# ExpectedSettlementAmount: sum of installments due before today
expected = (
    df_plan.filter(F.col("InstallmentDate") < F.lit(run_date))
    .groupBy("SettlementKey", "SourceSystem")
    .agg(F.sum("InstallmentAmount").alias("ExpectedSettlementAmount"))
)

df_eval = (
    df_eval.join(expected, ["SettlementKey", "SourceSystem"], "left")
    .withColumn("ExpectedSettlementAmount",
               F.coalesce(F.col("ExpectedSettlementAmount"), F.lit(0)))
)

# Financial derivations
df_eval = (
    df_eval
    .withColumn("RemainAmount",
               F.col("SettlementAmount") - total_paid)
    .withColumn("DebtAmount",
               F.greatest(F.lit(0), F.coalesce(F.col("ExpectedSettlementAmount"), F.lit(0)) - total_paid))
    .withColumn("KeptPercentage",
        F.when(F.col("SettlementAmount") > 0,
               F.round(total_paid / F.col("SettlementAmount"), 2))
        .otherwise(F.lit(None)))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 9: Bucket & Overdue Date

# COMMAND ----------

# Allocate payments to plan: mark fulfilled installments
plan_with_pay = (
    df_plan.join(
        df_eval.select("SettlementKey", "SourceSystem", "TotalPaymentAmount"),
        ["SettlementKey", "SourceSystem"], "inner")
    .withColumn("FulfilledFlag",
        F.when(F.col("InstallmentAmountAccum") <= F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)), 1)
        .when(
            (F.col("InstallmentAmountAccum") > F.coalesce(F.col("TotalPaymentAmount"), F.lit(0))) &
            (F.col("InstallmentAmountAccum") - F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) < 50),
            2)
        .when(
            (F.col("InstallmentAmountAccum") > F.coalesce(F.col("TotalPaymentAmount"), F.lit(0))) &
            (F.col("InstallmentAmountAccum") - F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) >= 50),
            3)
        .otherwise(0))
)

# Overdue: unpaid installments that are past due (only FulfilledFlag IN (0, 3) count)
overdue_source = (
    plan_with_pay
    .filter(
        (F.col("InstallmentDate") <= F.lit(run_date)) &
        F.col("FulfilledFlag").isin(0, 3))
    .groupBy("SettlementKey", "SourceSystem")
    .agg(
        F.min("InstallmentDate").alias("OverdueDate"))
)

# DPD-based bucket
overdue_with_dpd = (
    overdue_source
    .withColumn("DaysPastDue", F.datediff(F.lit(run_date), F.col("OverdueDate")))
    .withColumn("SettlementBucket",
        F.when(F.col("DaysPastDue") <= 0, 0)
         .when(F.col("DaysPastDue").between(1, 29), 1)
         .when(F.col("DaysPastDue").between(30, 59), 2)
         .when(F.col("DaysPastDue").between(60, 89), 3)
         .when(F.col("DaysPastDue").between(90, 119), 4)
         .when(F.col("DaysPastDue").between(120, 149), 5)
         .when(F.col("DaysPastDue").between(150, 179), 6)
         .when(F.col("DaysPastDue").between(180, 209), 7)
         .when(F.col("DaysPastDue").between(210, 239), 8)
         .when(F.col("DaysPastDue").between(240, 269), 9)
         .when(F.col("DaysPastDue").between(270, 299), 10)
         .when(F.col("DaysPastDue").between(300, 329), 11)
         .when(F.col("DaysPastDue").between(330, 359), 12)
         .when(F.col("DaysPastDue").between(360, 720), 13)
         .when(F.col("DaysPastDue") > 720, 14)
         .otherwise(0))
    .select("SettlementKey", "SourceSystem", "OverdueDate", "SettlementBucket", "DaysPastDue")
)

# Join bucket/overdue to eval
df_eval = (
    df_eval.join(overdue_with_dpd, ["SettlementKey", "SourceSystem"], "left")
    .withColumn("OverdueDate", F.col("OverdueDate"))
    .withColumn("SettlementBucket", F.coalesce(F.col("SettlementBucket"), F.lit(0)))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 10: Eval Status Classification

# COMMAND ----------

# Materialize df_eval to cut lineage before eval status + principal + participation
df_eval.cache()

# Eval status classification
df_eval = df_eval.withColumn("EvalStatus",
    F.when(
        (F.col("IsActiveFlag") == 1) & (F.col("SettlementAmount") > 0) &
        (F.col("KeptPercentage") >= 0.95) & F.col("ImplementationDate").isNotNull() &
        ((F.col("NumOfinstalments") == F.col("PastInstalmentsNum")) | (F.col("KeptPercentage") >= 1)),
        F.lit("Fulfilled"))
    .when(
        (F.col("IsActiveFlag") == 1) & (F.col("ExpiredDate") < F.lit(run_date)) &
        (F.col("SettlementAmount") > 0) & F.col("ImplementationDate").isNotNull() &
        (F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) > 0) &
        (F.col("KeptPercentage") >= 0.95),
        F.lit("Fulfilled"))
    .when(
        (F.col("IsActiveFlag") == 1) & (F.col("ExpiredDate") < F.lit(run_date)) &
        (F.col("SettlementAmount") > 0) & F.col("ImplementationDate").isNotNull() &
        (F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) > 0) &
        F.col("KeptPercentage").between(0.85, 0.9499),
        F.lit("Partially Fulfilled"))
    .when(
        (F.col("IsActiveFlag") == 1) & (F.col("ExpiredDate") < F.lit(run_date)) &
        (F.col("SettlementAmount") > 0) & F.col("ImplementationDate").isNotNull() &
        (F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) > 0) &
        (F.col("KeptPercentage") < 0.85),
        F.lit("Not Fulfilled"))
    .when(
        (F.col("IsActiveFlag") == 1) &
        (F.coalesce(F.col("TotalPaymentAmount"), F.lit(0)) > 0) &
        F.col("ImplementationDate").isNotNull(),
        F.lit("Implemented"))
    .when(F.col("IsActiveFlag") == 1, F.lit("Pending Implementation"))
    .otherwise(F.lit("Non-Active"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 11: Principal Proposed Amount

# COMMAND ----------

# Principal proposed amount: SUM(Principal + DuePrincipal) as of the SubmissionDate.
# Only calculated for settlements NOT already covered by the migration snapshot.

# Settlements needing the calculation
sub_dates = (
    df_eval.filter(
        F.col("SubmissionDate").isNotNull() &
        (F.col("SubmissionDate") <= F.lit(run_date)) &
        F.col("MigPrincipalProposedAmount").isNull())
    .select("SettlementKey", "SourceSystem",
            F.col("SubmissionDate").cast("date").alias("SubmissionDate"))
)

sub_loans = sub_dates.join(
    df_loan.select("SettlementKey", "SourceSystem", "ExposureKey").dropDuplicates(),
    ["SettlementKey", "SourceSystem"])

# dexposure history to get Principal + DuePrincipal for the correct businessdate
dexposure_hist = spark.sql(f"""
    SELECT ExposureKey, BusinessDate,
           COALESCE(Principal, 0)    AS Principal,
           COALESCE(DuePrincipal, 0) AS DuePrincipal
    FROM `{env}-cdh`.derivativeactive.dexposure
    UNION ALL
    SELECT ExposureKey, BusinessDate,
           COALESCE(Principal, 0)    AS Principal,
           COALESCE(DuePrincipal, 0) AS DuePrincipal
    FROM `{env}-cdh`.derivativearchive.dexposure
    WHERE BusinessDate < DATE'2024-01-30'
""")

# For each loan the latest snapshot on or before SubmissionDate is chosen
w_asof = Window.partitionBy("SettlementKey", "SourceSystem", "ExposureKey").orderBy(F.col("bd").desc())

principal_source = (
    sub_loans.alias("l")
    .join(dexposure_hist.alias("dx"),
          (F.col("l.ExposureKey") == F.col("dx.ExposureKey")) &
          (F.col("dx.BusinessDate") <= F.col("l.SubmissionDate")))
    .select("l.SettlementKey", "l.SourceSystem", "l.ExposureKey",
            F.col("dx.BusinessDate").alias("bd"),
            (F.col("dx.Principal") + F.col("dx.DuePrincipal")).alias("loan_principal"))
    .withColumn("rn", F.row_number().over(w_asof))
    .filter(F.col("rn") == 1)
    .groupBy("SettlementKey", "SourceSystem")
    .agg(F.sum("loan_principal").alias("CalculatedPrincipal"))
)

df_eval = (
    df_eval.join(principal_source, ["SettlementKey", "SourceSystem"], "left")
    .withColumn("PrincipalProposedAmount",
        F.coalesce(
            F.col("MigPrincipalProposedAmount"),
            F.col("CalculatedPrincipal"),
            F.col("ProposedAmount")))
    .drop("CalculatedPrincipal")
)

# Initial settlement code: traces settlement lineage across portfolio migrations.
df_eval = df_eval.withColumn("InitialSettlementKey",
    F.coalesce(F.col("MigInitialSettlementKey"), F.col("SettlementKey"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 12: Participation Percentage

# COMMAND ----------

# Single account → 1.0
# Multi account → balance-proportional
initialization_exposure_info = spark.table(ref_initialization_exposure_info)

loan_balance = (
    df_loan.alias("settlement_loan")
    .join(initialization_exposure_info.alias("init_exposure_info"),
          F.col("settlement_loan.ExposureKey") == F.regexp_replace(F.col("init_exposure_info.ExposureKey"), "^[A-Za-z]+", ""), "left")
    .groupBy(F.col("settlement_loan.SettlementKey"), F.col("settlement_loan.SourceSystem"), F.col("settlement_loan.ExposureKey"))
    .agg(F.coalesce(
        F.max("init_exposure_info.InitialAssignmentBalance"),
        F.max("settlement_loan.OriginationAmount"),
        F.lit(0)).alias("LoanBalance"))
)

settlement_aggregate = loan_balance.groupBy("SettlementKey", "SourceSystem").agg(
    F.count("*").alias("AccountCount"),
    F.min("LoanBalance").alias("MinBalance"),
    F.sum("LoanBalance").alias("TotalBalance"))

participation = (
    loan_balance.alias("lb")
    .join(settlement_aggregate.alias("sa"),
          (F.col("lb.SettlementKey") == F.col("sa.SettlementKey")) &
          (F.col("lb.SourceSystem") == F.col("sa.SourceSystem")))
    .withColumn("ParticipationPercentage",
        F.when((F.col("sa.AccountCount") == 1), F.lit(100.0))
        .when((F.col("sa.MinBalance") == 0) | F.col("sa.MinBalance").isNull() | (F.col("sa.TotalBalance") == 0),
              F.round(F.lit(100.0) / F.col("sa.AccountCount"), 4))
        .otherwise(F.round(F.lit(100.0) * F.col("lb.LoanBalance") / F.col("sa.TotalBalance"), 4)))
    .select(F.col("lb.SettlementKey"), F.col("lb.SourceSystem"),
            F.col("lb.ExposureKey"), "ParticipationPercentage")
)

# Update df_loan with participation (join on ExposureKey grain, then broadcast to all CaseCode rows)
df_loan = df_loan.join(
    participation.select("SettlementKey", "SourceSystem", "ExposureKey", "ParticipationPercentage"),
    ["SettlementKey", "SourceSystem", "ExposureKey"], "left"
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 13: Write Results to Internal

# COMMAND ----------

# Final eval table (settlement level — one row per settlement)
result_eval = df_eval.select(
    "SettlementKey", "SourceSystem",
    "PortfolioKey", "SettlementAmount", "DiscountAmount",
    "NumOfinstalments", "SettlementStatusCode", "SettlementStatus",
    "StatusChangeDate", "StatusCategory", "ExpiredDate", "ProposedAmount",
    "InitialSettlementKey", "PrincipalProposedAmount", "RemainAmount",
    "NumOfAccounts", "NextInstallmentDate", "NextInstallmentAmount",
    "FirstInstalmentDate", "ApprovalDate", "RejectedDate", "OverdueDate",
    "DebtAmount", "ExpectedSettlementAmount", "SettlementBucket",
    "SubmissionDate", "ImplementationDate", "CancelledDate", "AutoCancelledDate",
    "CurrentInstallmentAmount", "PastInstalmentsNum", "DaysPastDue",
    "TotalPaymentAmount", "LastPaymentDate", "IsActiveFlag", "KeptPercentage",
    "EvalStatus", "IsMigratedFlag", "PoolName", "CreatedDate", "BalloonAmount",
    F.lit(run_date).cast("date").alias("BusinessDate"),
    F.from_utc_timestamp(F.current_timestamp(), "Europe/Athens").cast("timestamp").alias("IngestionTimestamp"),
)

(result_eval.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(internal_settlement_eval))

# Final loan table (loan level — one row per settlement+loan)
result_loan = df_loan.select(
    "SettlementKey", "SourceSystem",
    "CaseCode", "ExposureKey", "AgreementKey",
    "DiasNumber", "ParticipationPercentage", "IsPrimarySettlement",
    F.lit(run_date).cast("date").alias("BusinessDate"),
    F.from_utc_timestamp(F.current_timestamp(), "Europe/Athens").cast("timestamp").alias("IngestionTimestamp"),
)

(result_loan.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(internal_settlement_loan))

df_plan.unpersist()
df_eval.unpersist()