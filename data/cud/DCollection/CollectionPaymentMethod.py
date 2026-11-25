# Databricks notebook source
# MAGIC %md
# MAGIC # CollectionPaymentMethod
# MAGIC Calculates the **PaymentMethod** field per collection based on the collecting bank file
# MAGIC (Alpha / Credia / NBG) and the relevant source lookups.
# MAGIC Writes to `derivativeinternal.collectionpaymentmethod` (keyed by `CollectionKey`).
# MAGIC
# MAGIC Runs BEFORE:
# MAGIC - DCollection (requires PaymentMethod)
# MAGIC
# MAGIC ## Logic (per portfolio's collectionBankFile)
# MAGIC - **Alpha**: DIAS (PaymentFileChannel like %DIAS%) > Direct Debit (SOrders ImportId) >
# MAGIC   TunCodePrefix: 096=APS, 099=Web Banking, 101-996=Bank Branch_Cashier
# MAGIC - **Credia**: transaction_code 563=DIAS, 777=Direct Debit, (220,556,571)=Invoice,
# MAGIC   (193,213,990)=Web Banking, 52=Bank Branch_Cashier
# MAGIC - **NBG**: PaymentChannel O=Bank Branch_Cashier, E=Direct Debit, (A,B,S)=APS, (I,M)=Web Banking
# MAGIC
# MAGIC ## Auxiliary table (no history)
# MAGIC This is an auxiliary table with NO BusinessDate / no history. It is TRUNCATED at the
# MAGIC start of every run and rewritten with a single current snapshot keyed by CollectionKey.
# MAGIC DCollection joins it on CollectionKey (not on BusinessDate).
# MAGIC
# MAGIC ## Duplicate handling
# MAGIC Each source is reduced to 1 row per join key. If a join key has *conflicting* values
# MAGIC (more than one distinct value in the decisive column), that key is logged and dropped
# MAGIC (no match) rather than picking arbitrarily.

# COMMAND ----------

# DBTITLE 1,Imports
import sys
sys.path.append("../aws_utils")

import aws_utils

env = aws_utils.get_env_prefix()
import delta_table_creator, transform

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("unity", "True", "Unity")

# COMMAND ----------

# DBTITLE 1,Schema, Create table, Truncate (auxiliary - no history)
schema = (
    "CollectionKey STRING,"
    "PortfolioName STRING,"
    "PaymentMethod STRING,"
    "IngestionTimestamp TIMESTAMP"
)

# Auxiliary snapshot table: no partitioning, no BusinessDate.
partition_columns = []
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "collectionpaymentmethod"
derivative_table_path = ""

# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path = f"`{env}-cdh`.derivativeinternal.{table_name}"
    if not spark.catalog.tableExists(derivative_table_path):
        delta_table_creator.create_unity_delta_table(spark, derivative_table_path, partition_columns, schema)

# ================== Truncate (drop previous snapshot) ==================
if unity == "True" and spark.catalog.tableExists(derivative_table_path):
    spark.sql(f"TRUNCATE TABLE {derivative_table_path}")

# COMMAND ----------

# DBTITLE 1,Collection perimeter (Matched only) + portfolio bank file
# Only Matched collections get a PaymentMethod (same as the source query FinalReason='Matched').
# collectionBankFile drives which branch of the logic applies.
collection = spark.sql(f"""
    SELECT
        c.CollectionKey,
        c.CollectionSourceKey,
        c.ExposureKey,
        -- ExposureKey can be stored with or without the 3-char portfolio prefix across
        -- tables (e.g. primary.tun). Precompute BOTH forms (same approach as
        -- CollectionPaymentType) so joins match regardless of which form each side uses.
        CASE WHEN LEFT(c.ExposureKey, 3) = p.Prefix
             THEN c.ExposureKey
             ELSE concat(p.Prefix, c.ExposureKey) END AS ExposureKeyWithPrefix,
        CASE WHEN LEFT(c.ExposureKey, 3) = p.Prefix
             THEN substring(c.ExposureKey, 4, 100)
             ELSE c.ExposureKey END AS ExposureKeyWithoutPrefix,
        c.CepalKey,
        c.Amount,
        c.ActualDate,
        c.PaymentFileChannel,
        c.DiasNumber,
        c.FinalReason,
        c.PortfolioName,
        p.collectionBankFile
    FROM `{env}-cdh`.primary.collection c
    JOIN `{env}-cdh`.primary.portfolio p
        ON p.PortfolioName = c.PortfolioName
        AND p.BusinessDate = c.BusinessDate
    WHERE c.BusinessDate = '{run_date}'
      AND c.FinalReason = 'Matched'
""")
collection.createOrReplaceTempView("collection")

# COMMAND ----------

# DBTITLE 1,Conflict-check helper
# Reduces a source to 1 row per join key. If a join key holds >1 distinct value in the
# decisive column(s), it is a conflict: we LOG it and DROP the key (it will not match),
# instead of arbitrarily picking one. Harmless duplicates (identical value) pass through.
def dedup_or_drop_conflicts(df, key_cols, value_cols, source_name):
    """Return a DataFrame with 1 row per key_cols.

    Rules:
    - NULL and '' (empty string) are treated as "no value" and are ignored.
    - A key with one real value + any number of NULL/'' rows -> keep the real value.
    - A key with >1 distinct REAL (non-empty) value -> conflict: logged and dropped.
    - A key with only NULL/'' -> the (empty) value is kept.
    The orderBy is only a deterministic tie-break; among tied rows the value is identical.
    """
    # Normalize each value col: NULL and '' both become NULL ("no value").
    norm_cols = []
    for c in value_cols:
        norm = f"_norm_{c}"
        df = df.withColumn(
            norm,
            F.when(F.trim(F.col(c).cast("string")) == "", F.lit(None)).otherwise(F.col(c).cast("string")),
        )
        norm_cols.append(norm)

    # Combined non-empty signature per row (NULL if all value cols are empty).
    combo = F.concat_ws("||", *[F.col(n) for n in norm_cols])
    df = df.withColumn("_combo", F.when(F.length(combo) == 0, F.lit(None)).otherwise(combo))

    w = Window.partitionBy(*key_cols)
    # Count only DISTINCT REAL values (collect_set already drops NULLs).
    marked = df.withColumn("_distinct_real", F.size(F.collect_set(F.col("_combo")).over(w)))

    conflicts = marked.filter(F.col("_distinct_real") > 1)
    conflict_count = conflicts.select(*key_cols).distinct().count()
    if conflict_count > 0:
        print(
            f"[CollectionPaymentMethod][WARN] Source '{source_name}': "
            f"{conflict_count} join key(s) have conflicting REAL values in {value_cols} "
            f"-> dropped (no match). Sample:"
        )
        conflicts.select(*key_cols, *value_cols).distinct().show(20, truncate=False)

    # Keep non-conflicting keys (0 or 1 distinct real value).
    clean = marked.filter(F.col("_distinct_real") <= 1)
    # Prefer the row that carries the real value (_combo NOT NULL) over NULL/'' rows.
    rn = Window.partitionBy(*key_cols).orderBy(F.col("_combo").asc_nulls_last())
    clean = (
        clean.withColumn("_rn", F.row_number().over(rn))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_distinct_real", "_combo", *norm_cols)
    )
    return clean

# COMMAND ----------

# DBTITLE 1,SOrders (Alpha - Direct Debit via ImportId)
# importidFlag=1 => Direct Debit. Join on AccountNumber/Amount/ResDate/BusinessDate.
sid_raw = spark.sql(f"""
    SELECT
        AccountNumber,
        Amount,
        ResDate,
        BusinessDate,
        CASE WHEN ImportId IS NOT NULL THEN 1 ELSE 0 END AS importidFlag
    FROM `{env}-cdh`.landed.profitsintegration_sordersimportdata
    WHERE BusinessDate = '{run_date}'
""")

sid = dedup_or_drop_conflicts(
    sid_raw,
    key_cols=["AccountNumber", "Amount", "ResDate", "BusinessDate"],
    value_cols=["importidFlag"],
    source_name="profitsintegration_sordersimportdata",
)
sid.createOrReplaceTempView("sid")

# COMMAND ----------

# DBTITLE 1,Attica transactions (Credia - transaction_code)
trn_raw = spark.sql(f"""
    SELECT id, transaction_code, BusinessDate
    FROM `{env}-cdh`.landed.process_collection_attica_transactions_cloud
    WHERE BusinessDate = '{run_date}'
""")

trn = dedup_or_drop_conflicts(
    trn_raw,
    key_cols=["id", "BusinessDate"],
    value_cols=["transaction_code"],
    source_name="process_collection_attica_transactions_cloud",
)
trn.createOrReplaceTempView("trn")

# COMMAND ----------

# DBTITLE 1,SymNbg payments (NBG - PaymentChannel)
symnbg_raw = spark.sql(f"""
    SELECT
        CEPAL_ID,
        TransactionDate,
        Amount,
        QCAccountNumber,
        PaymentChannel
    FROM `{env}-cdh`.landed.cepalplatformportpayment_symnbgpayments
    WHERE BusinessDate = '{run_date}'
""")

symnbg = dedup_or_drop_conflicts(
    symnbg_raw,
    key_cols=["CEPAL_ID", "TransactionDate", "Amount", "QCAccountNumber"],
    value_cols=["PaymentChannel"],
    source_name="cepalplatformportpayment_symnbgpayments",
)
symnbg.createOrReplaceTempView("symnbg")

# COMMAND ----------

# DBTITLE 1,CollectionTun (Alpha - TunCodePrefix)
# TUN code must start with 'TUN:' (anchored). TunCodePrefix = chars 13-15 of TunCode.
# Reduced to 1 row per CollectionKey; conflicting prefixes are logged and dropped.
collection_tun_raw = spark.sql(f"""
    SELECT
        c.CollectionKey,
        SUBSTR(t.TunCode, 13, 3) AS TunCodePrefix
    FROM collection c
    JOIN `{env}-cdh`.primary.tun t
        ON t.BusinessDate = '{run_date}'
        AND c.CollectionSourceKey = regexp_extract(t.TunCode, '^TUN:([^$]*)', 1)
        -- tun.ExposureKey may be stored with or without the portfolio prefix, so match
        -- against both precomputed forms of the collection exposure key.
        AND t.ExposureKey IN (c.ExposureKeyWithPrefix, c.ExposureKeyWithoutPrefix)
        AND c.ActualDate = CAST(t.TransactionDateTime AS DATE)
""")

collection_tun = dedup_or_drop_conflicts(
    collection_tun_raw,
    key_cols=["CollectionKey"],
    value_cols=["TunCodePrefix"],
    source_name="collection_tun",
)
collection_tun.createOrReplaceTempView("collection_tun")

# COMMAND ----------

# DBTITLE 1,Previous PaymentMethod (carry-forward source = derivativeactive.DCollection)
# ASSUMPTION: a collection's PaymentMethod is IMMUTABLE once set. Therefore we carry
# forward the previous value from DCollection and only (re)compute PaymentMethod for
# collections that do NOT already have one. If this assumption ever changes (a
# collection's PaymentMethod can be updated after being set), remove the perimeter
# cut below and recompute for all collections.
#
# Carry-forward source is derivativeactive.DCollection (MAX BusinessDate < run_date),
# NOT this auxiliary table (which has no history). This is rerun-safe: reruns of the
# same run_date always read the same previous DCollection snapshot.
# DCollection stores the collection key as DCollectionKey (= concat_ws(";", CollectionKey),
# i.e. identical to CollectionKey for this single-field key). Alias it to CollectionKey
# so the downstream join on c.CollectionKey works.
prev_dcollection = spark.sql(f"""
    SELECT DCollectionKey AS CollectionKey,
           PaymentMethod AS prev_PaymentMethod
    FROM `{env}-cdh`.derivativeactive.DCollection
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate)
        FROM `{env}-cdh`.derivativeactive.DCollection
        WHERE BusinessDate < '{run_date}'
    )
      AND PaymentMethod IS NOT NULL
""")
prev_dcollection.createOrReplaceTempView("prev_dcollection")

# COMMAND ----------

# DBTITLE 1,Final calculation - PaymentMethod (perimeter cut + carry-forward)
# Perimeter cut: collections that already have a PaymentMethod in the previous DCollection
# snapshot are carried forward as-is (source joins are skipped for them via the
# prev.prev_PaymentMethod IS NULL guard). Only collections WITHOUT a previous value go
# through the source-lookup logic.
result_df = spark.sql(f"""
SELECT
    c.CollectionKey,
    c.PortfolioName,
    COALESCE(
        prev.prev_PaymentMethod,
        -- Rule 1 (general, highest priority): Kritikos / Opap by PaymentFileChannel
        CASE
            WHEN c.PaymentFileChannel = 'LOYALTY_KRITIKOS' THEN 'Kritikos'
            WHEN c.PaymentFileChannel = 'ΟΠΑΠ Πληρωμές' THEN 'Opap'
        END,
        -- Rule 2: bank-specific rules (only reached if Rule 1 was NULL)
        CASE
            WHEN c.collectionBankFile = 'Alpha' THEN
                CASE
                    WHEN c.PaymentFileChannel ILIKE '%DIAS%' THEN 'DIAS'
                    WHEN sid.importidFlag = 1 THEN 'Direct Debit'
                    WHEN ct.TunCodePrefix = '096' THEN 'APS'
                    WHEN ct.TunCodePrefix = '099' THEN 'Web Banking'
                    WHEN try_cast(ct.TunCodePrefix AS INT) >= 101
                         AND try_cast(ct.TunCodePrefix AS INT) <= 996 THEN 'Bank Branch_Cashier'
                END
            WHEN c.collectionBankFile = 'Credia' THEN
                CASE
                    WHEN trn.transaction_code = 563 THEN 'DIAS'
                    WHEN trn.transaction_code = 777 THEN 'Direct Debit'
                    WHEN trn.transaction_code IN (220, 556, 571) THEN 'Invoice'
                    WHEN trn.transaction_code IN (193, 213, 990) THEN 'Web Banking'
                    WHEN trn.transaction_code = 52 THEN 'Bank Branch_Cashier'
                END
            WHEN c.collectionBankFile = 'NBG' THEN
                CASE
                    WHEN symnbg.PaymentChannel = 'O' THEN 'Bank Branch_Cashier'
                    WHEN symnbg.PaymentChannel = 'E' THEN 'Direct Debit'
                    WHEN symnbg.PaymentChannel IN ('A', 'B', 'S') THEN 'APS'
                    WHEN symnbg.PaymentChannel IN ('I', 'M') THEN 'Web Banking'
                END
        END,
        -- Rule 3 (fallback): DIAS when not a DIAS file but has a DiasNumber and a
        -- TunCodePrefix < 100 (reached only if Rules 1 and 2 were both NULL)
        CASE
            WHEN c.PaymentFileChannel <> 'DIAS Πληρωμές'
                 AND c.DiasNumber IS NOT NULL
                 AND try_cast(ct.TunCodePrefix AS INT) < 100 THEN 'DIAS'
        END
    ) AS PaymentMethod
FROM collection c
LEFT JOIN prev_dcollection prev
    ON prev.CollectionKey = c.CollectionKey
-- Source lookups only matter when prev.prev_PaymentMethod IS NULL (perimeter cut):
LEFT JOIN sid
    ON prev.prev_PaymentMethod IS NULL
    AND sid.AccountNumber = c.ExposureKey
    AND sid.Amount = c.Amount
    AND sid.ResDate = c.ActualDate
    AND sid.BusinessDate = '{run_date}'
LEFT JOIN trn
    ON prev.prev_PaymentMethod IS NULL
    AND trn.id = c.CollectionSourceKey
LEFT JOIN symnbg
    ON prev.prev_PaymentMethod IS NULL
    AND symnbg.CEPAL_ID = SUBSTRING_INDEX(c.CepalKey, '_', 1)
    AND symnbg.TransactionDate = c.ActualDate
    AND symnbg.Amount * (-1) = c.Amount
    AND symnbg.QCAccountNumber = c.ExposureKey
LEFT JOIN collection_tun ct
    ON prev.prev_PaymentMethod IS NULL
    AND ct.CollectionKey = c.CollectionKey
""")

# Add IngestionTimestamp (Europe/Athens)
result_df = result_df.withColumn(
    "IngestionTimestamp",
    lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")),
)

# COMMAND ----------

# DBTITLE 1,Uniqueness check on CollectionKey (log only, no fail)
dupes = result_df.groupBy("CollectionKey").count().filter("count > 1")
dupe_count = dupes.count()
if dupe_count > 0:
    print(
        f"[CollectionPaymentMethod][WARN] {dupe_count} CollectionKey(s) appear more than once "
        f"in the result - investigate source fan-out. Sample:"
    )
    dupes.orderBy(F.col("count").desc()).show(20, truncate=False)
else:
    print("[CollectionPaymentMethod] OK - CollectionKey is unique in the result.")

# COMMAND ----------

# DBTITLE 1,Write to derivativeinternal (append after truncate = single snapshot)
if unity == "True":
    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path)
