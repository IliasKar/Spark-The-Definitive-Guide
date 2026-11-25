# Databricks notebook source
# MAGIC %md
# MAGIC # CollectionPaymentType
# MAGIC Calculates Payment Type fields for **ALL** collections.
# MAGIC Writes to `derivativeinternal.collectionpaymenttype`.
# MAGIC
# MAGIC Runs BEFORE:
# MAGIC - CollectionsAssignment (requires PaymentTypeGroup for rules)
# MAGIC - DCollection (requires all payment type fields)

# COMMAND ----------

# DBTITLE 1,Imports
import sys
sys.path.append("../aws_utils")

import aws_utils

env = aws_utils.get_env_prefix()
import delta_table_creator, transform

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("unity", "True", "Unity")

# COMMAND ----------

# DBTITLE 1,Schema, Create table, Delete existing records for run_date
schema = (
    "CollectionKey STRING,"
    "PortfolioKey INT,"
    "PortfolioName STRING,"
    "PaymentTypeCode INT,"
    "PaymentType STRING,"
    "PaymentTypeEng STRING,"
    "PaymentTypeGroup STRING,"
    "IsPaymentFlag BOOLEAN,"
    "BusinessDate DATE"
)

partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "collectionpaymenttype"
derivative_table_path = ""

# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path = f"`{env}-cdh`.derivativeinternal.{table_name}"
    if not spark.catalog.tableExists(derivative_table_path):
        delta_table_creator.create_unity_delta_table(spark, derivative_table_path, partition_columns, schema)

# ================== Delete From Delta Table ==================
if unity == "True":
    derivative_table_path = f"`{env}-cdh`.derivativeinternal.{table_name}"
    if spark.catalog.tableExists(derivative_table_path):
        transform.delete_from_unity_table(
            spark,
            derivative_table_path,
            run_date,
        )

# COMMAND ----------

# DBTITLE 1,Exposures
dexposure = spark.sql(f"""
SELECT distinct de.PortfolioName,
    CASE 
    WHEN LEFT(ExposureKey, 3) =Prefix 
            then ExposureKey 
        else 
            concat(Prefix,ExposureKey)
    end as ExposureKeyWithPrefix,
    CASE 
    WHEN LEFT(ExposureKey, 3) =Prefix 
            then substring(ExposureKey,4,100)
        else 
            exposurekey
    END AS ExposureKeyWithoutPrefix
FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date('{run_date}') de
join `{env}-cdh`.primary.portfolio p 
    on p.PortfolioName=de.portfolioname 
    and p.BusinessDate ='{run_date}'
""")

dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

# DBTITLE 1,Government Subsidy identification
# If exposure matches DEBIT_OPEN_ACCOUNT in subsidy table → 'Government Subsidy'
subsidy_exposures = spark.sql(f"""
    SELECT DISTINCT 
        ExposureKeyWithPrefix AS DEBIT_OPEN_ACCOUNT,
        ExposureKeyWithoutPrefix AS DEBIT_OPEN_ACCOUNTWithoutPrefix
    FROM (
        -- Pass 1: match with prefix
        SELECT de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix
        FROM `{env}-cdh`.landed.profits_lns_ext_subsidy_hd sbs
        JOIN dexposure de ON de.ExposureKeyWithPrefix = sbs.DEBIT_OPEN_ACCOUNT
        WHERE sbs.BusinessDate = '{run_date}'

        UNION ALL

        -- Pass 2: match without prefix
        SELECT de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix
        FROM `{env}-cdh`.landed.profits_lns_ext_subsidy_hd sbs
        JOIN dexposure de ON de.ExposureKeyWithoutPrefix = sbs.DEBIT_OPEN_ACCOUNT
        WHERE sbs.BusinessDate = '{run_date}'
    ) combined
""")

subsidy_exposures.createOrReplaceTempView("subsidy_exposures")

# COMMAND ----------

# DBTITLE 1,profits_params (code → descriptions)
profits_params = spark.sql(f"""
SELECT serial_num,
       description_orig AS PaymentTypeEng,
       CASE serial_num
           WHEN 0  THEN 'Είσπραξη από πελάτη'
           WHEN 1  THEN 'Είσπραξη από πελάτη, κατόπιν ρύθμισης που έγινε στη Cepal.'
           WHEN 2  THEN 'Μη αναμενόμενη είσπραξη από πελάτη'
           WHEN 3  THEN 'Είσπραξη μέσω Ν.3869'
           WHEN 4  THEN 'OTHER'
           WHEN 5  THEN 'Είσπραξη από εκπλειστηρίασμα'
           WHEN 48 THEN 'Ποσό κατάταξης από πλειστηριασμό ημών - ακινήτων_ΤΡΙΤΟΣ'
           WHEN 49 THEN 'Ποσό κατάταξης από πλειστηριασμό τρίτου - ακινήτων_ΤΡΙΤΟΣ'
           WHEN 52 THEN 'Ποσό κατάταξης από μη εμπραγμάτως εξασφαλισμένες οφειλές_ΤΡΙΤΟΣ'
           WHEN 54 THEN 'Είσπραξη από διανομή πτωχευτικής περιουσίας'
           WHEN 55 THEN 'Ποσό κατάταξης από πλειστηριασμό ημών - ακινήτων_REO'
           WHEN 56 THEN 'Ποσό κατάταξης από πλειστηριασμό τρίτου - ακινήτων_REO'
           WHEN 57 THEN 'Ποσό κατάταξης από μη εμπραγμάτως εξασφαλισμένες οφειλές_REO'
           WHEN 61 THEN 'LG COLLECTIONS/FORECLOSURE-AUCTION SALE'
           WHEN 64 THEN 'GREEK STATE GUARANTEE FORFEITURE PAYMENT'
           WHEN 65 THEN 'ΣΥΜΨΗΦΙΣΜΟΣ ΔΙΚΑΣΤΙΚΗΣ ΔΑΠΑΝΗΣ'
           WHEN 67 THEN 'COLLECTION VIA 3RD PARTY CONFISCATION'
           WHEN 72 THEN 'COLLATERAL LIQUIDATION/ΡΕΥΣΤΟΠ ΕΝΕΧΥΡΟΥ'
           WHEN 73 THEN 'ΑΝΤΙΛΟΓΙΣΜΟΣ ΠΛΗΡΩΜΗΣ ΟΦΕΙΛΕΤΗ'
           WHEN 75 THEN 'Είσπραξη από Πλειστηριασμό Ακινήτων'
           WHEN 77 THEN 'Είσπραξη Μη Εμπραγμάτως Εξασφαλισμένες Οφειλές'
           WHEN 10 THEN 'Είσπραξη από πλειστηριασμό πριν μεταβίβαση'
           WHEN 16 THEN 'Gross Proceeds from Foreclosures'
           WHEN 50 THEN 'Non-RE Collateral Auction Proceeds - AO'
           WHEN 51 THEN 'Non-RE Collateral Auction Proceeds - Third Party'
           WHEN 53 THEN 'Είσπραξη από εκποίηση κινητής περιουσίας'
           WHEN 4211 THEN 'REQUEST FULL PAYMENT'
           ELSE NULL
       END AS PaymentTypeGr
FROM `{env}-cdh`.landed.profits_params
WHERE param_source = 'GD' AND param_type = 'EXTYP' AND BusinessDate = '{run_date}'
""")

profits_params.createOrReplaceTempView("profits_params")

# COMMAND ----------

# DBTITLE 1,Attica transactions
attica_transactions = spark.sql(f"""
    SELECT id, Transaction_Code, Transaction_Description, IsPaymentFlag
    FROM `{env}-cdh`.landed.process_collection_attica_transactions_cloud
    WHERE BusinessDate = '{run_date}'
""")

attica_transactions.createOrReplaceTempView("attica_transactions")

# COMMAND ----------

# DBTITLE 1,Collection (full perimeter — LEFT JOIN to exposure map)
collection_exposure_map = spark.sql(f"""
    -- Pass 1: ExposureKey matches with prefix
    SELECT c.ExposureKey, c.PortfolioName AS col_PortfolioName,
        de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix
    FROM `{env}-cdh`.primary.collection c
    JOIN dexposure de ON de.ExposureKeyWithPrefix = c.ExposureKey
        AND c.PortfolioName = de.PortfolioName
    WHERE c.BusinessDate = '{run_date}'

    UNION ALL

    -- Pass 2: ExposureKey matches without prefix (exclude already matched)
    SELECT c.ExposureKey, c.PortfolioName AS col_PortfolioName,
        de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix
    FROM `{env}-cdh`.primary.collection c
    JOIN dexposure de ON de.ExposureKeyWithoutPrefix = c.ExposureKey
        AND c.PortfolioName = de.PortfolioName
    WHERE c.BusinessDate = '{run_date}'
      AND NOT EXISTS (
          SELECT 1 FROM dexposure d2
          WHERE d2.ExposureKeyWithPrefix = c.ExposureKey
            AND d2.PortfolioName = c.PortfolioName
      )
""")
collection_exposure_map.createOrReplaceTempView("collection_exposure_map")

collection = spark.sql(f"""
    SELECT c.CollectionKey,
        c.CollectionSourceKey,
        cem.ExposureKeyWithPrefix,
        cem.ExposureKeyWithoutPrefix,
        c.loanbankcode,
        c.ActualDate,
        c.MatchedDate,
        c.FinalReason,
        c.DiasNumber,
        c.PortfolioKey,
        c.PortfolioName
    FROM `{env}-cdh`.primary.collection c
    LEFT JOIN collection_exposure_map cem
        ON cem.ExposureKey = c.ExposureKey
        AND cem.col_PortfolioName = c.PortfolioName
    WHERE c.BusinessDate = '{run_date}'
""")

collection.createOrReplaceTempView("collection")

# COMMAND ----------

# DBTITLE 1,TransactionTypeCode from Accounttransaction
# For matched records: find accounttransaction (ALL TransactionCodes) within ±7 days of ActualDate.
# MAX(COALESCE(TransactionTypeCode, 1)) per ExposureKey + ValeurDate.

accounttransaction = spark.sql(f"""
    SELECT ExposureKeyWithPrefix,
          ExposureKeyWithoutPrefix,
          ValeurDate,
          MAX(COALESCE(TransactionTypeCode, 1)) AS TransactionTypeCode
    FROM (
        -- Pass 1: match with prefix
        SELECT de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix,
               act.ValeurDate, act.TransactionTypeCode
        FROM `{env}-cdh`.primary.accounttransaction act
        JOIN dexposure de ON de.ExposureKeyWithPrefix = act.ExposureKey
            AND act.PortfolioName = de.PortfolioName
        WHERE act.EntryStatusCode = '1'
          AND act.BusinessDate BETWEEN date_sub('{run_date}', 365) AND date_add('{run_date}', 365)

        UNION ALL

        -- Pass 2: match without prefix
        SELECT de.ExposureKeyWithPrefix, de.ExposureKeyWithoutPrefix,
               act.ValeurDate, act.TransactionTypeCode
        FROM `{env}-cdh`.primary.accounttransaction act
        JOIN dexposure de ON de.ExposureKeyWithoutPrefix = act.ExposureKey
            AND act.PortfolioName = de.PortfolioName
        WHERE act.EntryStatusCode = '1'
          AND act.BusinessDate BETWEEN date_sub('{run_date}', 365) AND date_add('{run_date}', 365)
          AND NOT EXISTS (
              SELECT 1 FROM dexposure d2
              WHERE d2.ExposureKeyWithPrefix = act.ExposureKey
                AND d2.PortfolioName = act.PortfolioName
          )
    ) combined
    GROUP BY ExposureKeyWithPrefix, ExposureKeyWithoutPrefix, ValeurDate
""")
accounttransaction.createOrReplaceTempView("accounttransaction")

# COMMAND ----------

# DBTITLE 1,Match accounttransaction to Collection
collection_accounttransaction = spark.sql(f"""
WITH src AS (
    SELECT
        c.CollectionKey,
        pt.TransactionTypeCode,
        ROW_NUMBER() OVER (
            PARTITION BY c.CollectionKey
            ORDER BY pt.ValeurDate ASC
        ) AS rnk
    FROM collection c
    INNER JOIN accounttransaction pt ON pt.ExposureKeyWithPrefix = c.ExposureKeyWithPrefix
       AND pt.ValeurDate >= c.ActualDate
       AND ABS(DATEDIFF(pt.ValeurDate, c.ActualDate)) <= 7
    WHERE LOWER(c.FinalReason) = 'matched'
)
SELECT CollectionKey, TransactionTypeCode AS ProfitsPaymentTypeCode
FROM src
WHERE rnk = 1
""")

collection_accounttransaction.createOrReplaceTempView("collection_accounttransaction")

# COMMAND ----------

# DBTITLE 1,Second Pass: Auction-only ±60d (for unmatched Cash records)
# ============================================================================
# SECOND PASS: Auction-Only 
# For collections that got NO match in the ±7d window (still defaulting to Cash),
# check specifically for ALL auction codes.
# This catches auction proceeds where booking is delayed due to legal process.
# ============================================================================

collection_accounttransaction_auction = spark.sql(f"""
WITH unmatched AS (
    SELECT c.CollectionKey, c.ExposureKeyWithPrefix, c.ActualDate
    FROM Collection c
    LEFT ANTI JOIN collection_accounttransaction ce ON ce.CollectionKey = c.CollectionKey
    WHERE LOWER(c.FinalReason) = 'matched'
      AND c.ExposureKeyWithPrefix IS NOT NULL
),
src AS (
    SELECT
        u.CollectionKey,
        pt.TransactionTypeCode,
        ROW_NUMBER() OVER (
            PARTITION BY u.CollectionKey
            ORDER BY DATEDIFF(pt.ValeurDate, u.ActualDate) ASC
        ) AS rnk
    FROM unmatched u
    INNER JOIN accounttransaction pt
        ON pt.ExposureKeyWithPrefix = u.ExposureKeyWithPrefix
        AND pt.ValeurDate >= u.ActualDate
        AND pt.TransactionTypeCode IN (5, 10, 16, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 61, 72, 75, 77)
)
SELECT CollectionKey, TransactionTypeCode AS ProfitsPaymentTypeCode
FROM src
WHERE rnk = 1
""")

collection_accounttransaction_auction.createOrReplaceTempView("collection_accounttransaction_auction")

# COMMAND ----------

# DBTITLE 1,Previous collectionpaymenttype (carry-forward)
prev_collectionpaymenttype = spark.sql(f"""
    SELECT CollectionKey,
           PaymentTypeCode AS prev_PaymentTypeCode,
           PaymentType AS prev_PaymentType,
           PaymentTypeEng AS prev_PaymentTypeEng,
           PaymentTypeGroup AS prev_PaymentTypeGroup
    FROM `{env}-cdh`.derivativeinternal.{table_name}
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate)
        FROM `{env}-cdh`.derivativeinternal.{table_name}
        WHERE BusinessDate < '{run_date}'
    )
""")

prev_collectionpaymenttype.createOrReplaceTempView("prev_collectionpaymenttype")

# COMMAND ----------

# DBTITLE 1,Final calculation - all payment type fields
result_df = spark.sql(f"""
SELECT DISTINCT
    c.CollectionKey,
    c.PortfolioKey,
    c.PortfolioName,
    -- PaymentTypeCode: attica > special cases > accounttransaction > carry-forward > default for matched
    COALESCE(
        at.Transaction_Code,
        CASE WHEN c.loanbankcode IN ('EKΔOΣHENTOΛHΣ', 'EΞOΔAENTOΛHΣ') THEN NULL END,
        ce.ProfitsPaymentTypeCode,
        auc.ProfitsPaymentTypeCode,
        prev.prev_PaymentTypeCode,
        CASE WHEN LOWER(c.FinalReason) = 'matched' THEN 1 ELSE NULL END
    ) AS PaymentTypeCode,
    -- PaymentType (Greek): attica > special cases > profits_params > carry-forward > default
    COALESCE(
        at.Transaction_Description,
        CASE c.loanbankcode
            WHEN 'EKΔOΣHENTOΛHΣ' THEN 'Μεταφορά από λογαριασμό'
            WHEN 'EΞOΔAENTOΛHΣ' THEN 'Έξοδα μεταφοράς'
        END,
        params.PaymentTypeGr,
        auc_params.PaymentTypeGr,
        prev.prev_PaymentType,
        'Άλλο'
    ) AS PaymentType,
    -- PaymentTypeEng: attica > special cases > profits_params > carry-forward > default
    COALESCE(
        at.Transaction_Description,
        CASE c.loanbankcode
            WHEN 'EKΔOΣHENTOΛHΣ' THEN 'Cash Sweep'
            WHEN 'EΞOΔAENTOΛHΣ' THEN 'Cash Sweep Fees'
        END,
        params.PaymentTypeEng,
        auc_params.PaymentTypeEng,
        prev.prev_PaymentTypeEng,
        'Other'
    ) AS PaymentTypeEng,
    -- PaymentTypeGroup: subsidy > code-based CASE > carry-forward > null
    CASE
        WHEN sub.DEBIT_OPEN_ACCOUNT IS NOT NULL THEN 'Government Subsidy'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) IN (5, 10, 16, 48, 49, 50, 51, 52, 53, 54, 61, 72, 75, 77)
            THEN 'RE Collateral Liquidation - 3rd Party'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) IN (55, 56, 57)
            THEN 'RE Collateral Liquidation - Repossession'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) IN (64, 65, 67)
            THEN 'Other'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) IN (0, 1, 2, 3, 4211, 11, 12, 17, 18, 19, 20, 21, 22, 23, 27, 30, 33, 43, 44, 47)
            THEN 'Cash'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) = 4
            THEN 'Other'
        WHEN COALESCE(ce.ProfitsPaymentTypeCode, auc.ProfitsPaymentTypeCode, prev.prev_PaymentTypeCode) IS NOT NULL
            THEN 'Cash'
        WHEN prev.prev_PaymentTypeGroup IS NOT NULL
            THEN prev.prev_PaymentTypeGroup
        ELSE NULL
    END AS PaymentTypeGroup,
    COALESCE(
        at.IsPaymentFlag,
        CASE WHEN LOWER(c.FinalReason) = 'matched' OR c.DiasNumber IS NOT NULL THEN
            CASE WHEN c.loanbankcode IN ('EKΔOΣHENTOΛHΣ', 'EΞOΔAENTOΛHΣ') THEN False
                 ELSE True END
        ELSE False END
    ) AS IsPaymentFlag,
    CAST('{run_date}' AS DATE) AS BusinessDate

FROM collection c
LEFT JOIN collection_accounttransaction ce ON ce.CollectionKey = c.CollectionKey
LEFT JOIN subsidy_exposures sub ON (sub.DEBIT_OPEN_ACCOUNT = c.exposurekeywithprefix or sub.DEBIT_OPEN_ACCOUNTWithoutPrefix = c.exposurekeywithoutprefix)
LEFT JOIN profits_params params ON params.serial_num = COALESCE(
    ce.ProfitsPaymentTypeCode,
    CASE WHEN LOWER(c.FinalReason) = 'matched' THEN 1 ELSE NULL END
)
LEFT JOIN collection_accounttransaction_auction auc ON auc.CollectionKey = c.CollectionKey
LEFT JOIN profits_params auc_params ON auc_params.serial_num = auc.ProfitsPaymentTypeCode
LEFT JOIN prev_collectionpaymenttype prev ON prev.CollectionKey = c.CollectionKey
LEFT JOIN attica_transactions at ON at.id = c.CollectionSourceKey
""")

# COMMAND ----------

# DBTITLE 1,Write to derivativeinternal
if unity == "True":
    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path)
