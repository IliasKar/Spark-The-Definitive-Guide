# Databricks notebook source
import boto3
from botocore.exceptions import NoCredentialsError
import traceback

"""
Accessing the Hive metastore requires specific considerations:

1. Hive metastore access is not possible via the instance profile if the cluster is on shared access mode.
   The cluster must be in shared access mode to interact with the Unity catalog.

2. To access both Unity catalog and AWS services, an access token from Amazon is required.

3. To retrieve the token secret key, access the secrets scope 'aws-databricks,' where the access key is stored per environment.
    https://docs.databricks.com/en/security/secrets/index.html

4. Once AWS credentials are obtained, clients for services like S3 and SSM can be created.

5. After you authenticate successfully querying the Hive metastore is also working.
"""

try:
    mode = dbutils.widgets.get("mode")
except Exception as e:
    mode = "shared" 

# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
    
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        # if aws_secret_key is not None:
        #     print("AWS secret key retrieved successfully.")
        # else:
        #     print("AWS secret key is None. Please check Databricks secrets configuration.")

        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None
    
# Function to get AWS credentials from Databricks secrets
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration

        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)    
else:
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')                           


# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

# COMMAND ----------

# DBTITLE 1,Config — reporting table target
mv_reporting_table = f"`{env_prefix}-cdh-delivery`.`tia`.`update_mv_current_auction`"

# COMMAND ----------

# DBTITLE 1,v_cte_005 — Base TIA data + matched join
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_cte_005_tia_data_base AS
WITH tia_data_base AS (
    SELECT
        A.c_auction_id            AS tia_auction,
        A.AuctionObJID            AS tia_ObjKey,
        A.Source                  AS tia_Source,
        case
            when A.initiator_cepal_others = 'Cepal' then '01_Cepal'
            when A.initiator_cepal_others = 'doValue' then '02_doValue'
            when A.initiator_cepal_others = 'Intrum' then '03_Intrum'
            when A.initiator_cepal_others = 'QQuant' then '04_QQuant'
            when A.initiator_cepal_others = 'ETE' then '05_ETE'
            when A.initiator_cepal_others = 'Special Financial Solutions' then '06_Special Financial Solutions'
            when A.initiator_cepal_others = 'B2Kapital' then '07_B2Kapital'
            when A.initiator_cepal_others = 'Bank' then '08_Bank'
            when A.initiator_cepal_others = 'PQH' then '09_PQH'
            when A.initiator_cepal_others = 'ΑΑΔΕ' then '10_ΑΑΔΕ'
            when A.initiator_cepal_others = 'Other' then '11_Other'
            else '12_Undefined'
        end as initiator_cepal_others,
        case when A.`Κατασχετήρια_Ημερομηνία` is null then 'na' else A.`Κατασχετήρια_Ημερομηνία` end as katasx_date,
        A.UniqueCode, A.AuctionDate, A.FirstOfferPrice, A.FinalPrice,
        A.result_blended, A.Tags, A.full_ownership,
        A.AuctionRound_property_id, A.AuctionRound_property,
        A.Today
    FROM `{env_prefix}-cdh-delivery`.tia.tia AS A
),
tia_matched_data AS (
    SELECT DISTINCT
        AuctionObJID, c_auction_id, Cepal_Key_Final,
        UIMatchingAuctionStatus, AuctionRound_property_id, AuctionRound_property
    FROM `{env_prefix}-cdh-delivery`.tia.tia
    WHERE UIMatchingAuctionStatus = 'validated-matched'
)
SELECT DISTINCT
    tia_data_base.*,
    CASE
        WHEN tia_matched_data.Cepal_Key_Final IS NOT NULL THEN tia_matched_data.Cepal_Key_Final
        ELSE CONCAT('Unmatched_', ROW_NUMBER() OVER (ORDER BY tia_matched_data.AuctionObJID, tia_matched_data.c_auction_id))
    END AS tia_CepalKeyFinal,
    CASE
        WHEN tia_matched_data.UIMatchingAuctionStatus = 'validated-matched' THEN 'validated-matched'
        ELSE 'Unmatched'
    END AS tia_UIMatchingAuctionStatus
FROM tia_data_base
LEFT JOIN tia_matched_data
    ON tia_matched_data.AuctionObJID = tia_data_base.tia_ObjKey
-- where tia_data_base.tia_auction = 'sHGWC1f3Rl'
""")

# COMMAND ----------

# DBTITLE 1,v_cte_010 — Key split / explode logic
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_010_tia_data AS
WITH base_dedup_validated_matched_data AS (
    SELECT
        tia_auction, tia_ObjKey, tia_CepalKeyFinal, tia_Source,
        UniqueCode, AuctionDate, FirstOfferPrice, FinalPrice,
        result_blended, Tags, full_ownership, katasx_date,
        AuctionRound_property_id, AuctionRound_property, initiator_cepal_others,
        tia_UIMatchingAuctionStatus, Today,
        case
            when tia_UIMatchingAuctionStatus = 'validated-matched'
            and Tags is null and full_ownership is null
            and tia_Source = 'eAuction' then 1 else 0
        end as perimeter_valid
    FROM v_cte_005_tia_data_base
    WHERE tia_UIMatchingAuctionStatus = 'validated-matched'
),
-- 1) array split keys (με τη μεθοδολογία replace + split)
obj AS (
    SELECT bd.*,
        FILTER(TRANSFORM(SPLIT(REPLACE(COALESCE(tia_CepalKeyFinal, ''), '||', ';'), ';'), x -> TRIM(x)), x -> x IS NOT NULL AND x <> '') AS keys_arr,
        ROW_NUMBER() OVER (PARTITION BY tia_auction ORDER BY tia_ObjKey) AS obj_rn,
        COUNT(*) OVER (PARTITION BY tia_auction) AS obj_cnt
    FROM base_dedup_validated_matched_data bd
),
-- 2) keys_cnt ανά auction (χωρίς window πάνω σε size)
keys_cnt_by_auction AS (
    SELECT tia_auction, MAX(SIZE(keys_arr)) AS keys_cnt FROM obj GROUP BY tia_auction
),
-- 3) posexplode keys ανά auction (auction-level keys list)
auction_keys AS (
    SELECT o.tia_auction, pe.pos AS key_pos, pe.col AS key_val
    FROM (SELECT DISTINCT tia_auction, FIRST_VALUE(keys_arr) OVER (PARTITION BY tia_auction ORDER BY tia_ObjKey) AS keys_arr FROM obj) o
    LATERAL VIEW posexplode(o.keys_arr) pe AS pos, col
),
-- 4) Branch A: ΔΔΔ + 1-1 (obj_cnt = keys_cnt) => row-to-row mapping
ddd_one_to_one AS (
    SELECT o.*, ak.key_val AS tia_CepalKeyFinal_splited
    FROM obj o
    JOIN keys_cnt_by_auction kc ON kc.tia_auction = o.tia_auction
    JOIN auction_keys ak ON ak.tia_auction = o.tia_auction AND ak.key_pos = o.obj_rn - 1
    WHERE o.tia_Source = 'Δελτίο Δικαστικών Δημοσιεύσεων' AND o.obj_cnt = kc.keys_cnt
),
-- 5a) other cases for eAuction -> explode per ObjKey (όπως το έχεις εφαρμόσει)
other_eauction AS (
    SELECT o.*, ex.key_val AS tia_CepalKeyFinal_splited
    FROM obj o LATERAL VIEW explode(o.keys_arr) ex AS key_val
    WHERE o.tia_Source = 'eAuction'
),
-- 5b) other cases for NON-eAuction -> auction-level explode (ίδια λογική)
other_non_eauction AS (
    SELECT o.*, ak.key_val AS tia_CepalKeyFinal_splited
    FROM obj o
    JOIN keys_cnt_by_auction kc ON kc.tia_auction = o.tia_auction
    JOIN auction_keys ak ON ak.tia_auction = o.tia_auction
    WHERE o.tia_Source <> 'eAuction'
      AND NOT (o.tia_Source = 'Δελτίο Δικαστικών Δημοσιεύσεων' AND o.obj_cnt = kc.keys_cnt)
),
cte_union_data AS (
    SELECT * FROM ddd_one_to_one
    UNION ALL SELECT * FROM other_eauction
    UNION ALL SELECT * FROM other_non_eauction
),
-- STEP 1: auction-level distinct counts πάνω στο exploded dataset
temp_auction_level_counts AS (
    SELECT DISTINCT tia_auction, tia_ObjKey, tia_CepalKeyFinal_splited FROM cte_union_data
),
cte_dc_objkey_over_auction AS (
    SELECT tia_auction, count(tia_ObjKey) over(partition by tia_auction) as dc_objkey_over_auction
    FROM (SELECT DISTINCT tia_auction, tia_ObjKey FROM temp_auction_level_counts)
),
cte_dc_splited_over_auction AS (
    SELECT tia_auction, count(tia_CepalKeyFinal_splited) over(partition by tia_auction) as dc_splited_over_auction
    FROM (SELECT DISTINCT tia_auction, tia_CepalKeyFinal_splited FROM temp_auction_level_counts)
),
cte_dc_tia_ObjKey_over_tia_auction AS (
    SELECT tia_auction, count(tia_ObjKey) over(partition by tia_auction) as dc_tia_ObjKey_over_tia_auction
    FROM (SELECT DISTINCT tia_ObjKey, tia_auction FROM temp_auction_level_counts)
),
auction_level_counts AS (
    SELECT DISTINCT t.*,
        o.dc_objkey_over_auction, s.dc_splited_over_auction
    FROM temp_auction_level_counts t
    LEFT JOIN cte_dc_objkey_over_auction o ON o.tia_auction = t.tia_auction
    LEFT JOIN cte_dc_splited_over_auction s ON s.tia_auction = t.tia_auction
    LEFT JOIN cte_dc_tia_ObjKey_over_tia_auction d ON d.tia_auction = t.tia_auction
),
cte_010_final_data AS (
    SELECT DISTINCT u.*, alc.dc_objkey_over_auction, alc.dc_splited_over_auction
    FROM cte_union_data u
    LEFT JOIN auction_level_counts alc ON alc.tia_auction = u.tia_auction
),
-- STEP 2: tia_CepalKeyFinal_splited - level distinct counts πάνω στο exploded dataset
temp_tia_CepalKeyFinal_splited_counts AS (SELECT DISTINCT * FROM cte_010_final_data),
cte_dc_tia_CepalKeyFinal_splited_over_tia_ObjKey AS (
    SELECT tia_ObjKey,
        count(tia_CepalKeyFinal_splited) over (partition by tia_ObjKey, tia_CepalKeyFinal, tia_auction) as dc_tia_CepalKeyFinal_splited_over_tia_ObjKey
    FROM (SELECT DISTINCT tia_auction, tia_ObjKey, tia_CepalKeyFinal_splited, dc_objkey_over_auction, dc_splited_over_auction, tia_CepalKeyFinal FROM temp_tia_CepalKeyFinal_splited_counts)
),
cte_rn_tia_ObjKey_over_tia_auction AS (
    SELECT tia_ObjKey,
        ROW_NUMBER() OVER (PARTITION BY tia_auction ORDER BY tia_ObjKey) AS rn_tia_ObjKey_over_tia_auction
    FROM (SELECT DISTINCT tia_auction, tia_ObjKey FROM temp_tia_CepalKeyFinal_splited_counts)
),
tia_CepalKeyFinal_splited_counts AS (
    SELECT DISTINCT t.*,
        rn.rn_tia_ObjKey_over_tia_auction,
        dc.dc_tia_CepalKeyFinal_splited_over_tia_ObjKey
    FROM temp_tia_CepalKeyFinal_splited_counts t
    LEFT JOIN cte_dc_tia_CepalKeyFinal_splited_over_tia_ObjKey dc ON dc.tia_ObjKey = t.tia_ObjKey
    LEFT JOIN cte_rn_tia_ObjKey_over_tia_auction rn ON rn.tia_ObjKey = t.tia_ObjKey
)
SELECT DISTINCT * FROM tia_CepalKeyFinal_splited_counts
""")

# COMMAND ----------

# DBTITLE 1,v_cte_020 — split_mode_step classification
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_020_final_data AS
SELECT DISTINCT
    v_cte_010_tia_data.*,
    case
        when dc_objkey_over_auction = dc_splited_over_auction then 'single_pid_explode'
        when dc_objkey_over_auction < dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey = 1 then 'single_pid_explode'
        when dc_objkey_over_auction = dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey > 1 then 'cartesian_pid_explode'
        when dc_objkey_over_auction > dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey = 1 then 'cartesian_pid_explode_not_full_match'
        when dc_objkey_over_auction < dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey > 1 then 'cartesian_pid_explode'
        when dc_objkey_over_auction > dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey > 1 and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey < dc_objkey_over_auction then 'cartesian_pid_explode_not_full_match'
        when dc_objkey_over_auction > dc_splited_over_auction and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey > 1 and dc_splited_over_auction = 1 then 'single_pid_explode_not_full_match'
    end as split_mode_step
FROM v_cte_010_tia_data
""")

# COMMAND ----------

# DBTITLE 1,v_cte_030 — Assembly: base fields + exploded keys
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_030_temp_union_data_A AS
SELECT DISTINCT
    A.tia_auction, A.tia_ObjKey, A.tia_CepalKeyFinal, A.tia_Source,
    A.UniqueCode, A.AuctionDate, A.FirstOfferPrice, A.FinalPrice,
    A.result_blended, A.Tags, A.full_ownership, A.katasx_date,
    A.AuctionRound_property_id, A.AuctionRound_property,
    A.initiator_cepal_others, A.tia_UIMatchingAuctionStatus, A.Today,
    B.tia_CepalKeyFinal_splited,
    B.rn_tia_ObjKey_over_tia_auction,
    B.dc_objkey_over_auction,
    B.dc_splited_over_auction,
    B.dc_tia_CepalKeyFinal_splited_over_tia_ObjKey,
    B.split_mode_step,
    B.perimeter_valid
FROM v_cte_005_tia_data_base AS A
LEFT JOIN v_cte_020_final_data AS B ON A.tia_ObjKey = B.tia_ObjKey
""")

# COMMAND ----------

# DBTITLE 1,v_cte_035 — Real estate lookup (drealestate latest snapshot)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_cte_035_drealestate_data AS
SELECT
    RealEstateKey                AS dRE_REid,
    RealEstateCepalKey           AS dRE_RECepalKey,
    SourceSystem                 AS dRE_System,
    RealEstateActive             AS dRE_REActive,
    PropertyStatus               AS dRE_PropertyStatus,
    RealEstateMarketValue        AS dRE_MarketValue,
    RealEstateEvaluationDate     AS dRE_MarketValueDate,
    RealEstateEvaluationTypeCode AS dRE_MarketValueType,
    BusinessDate                 AS dRE_BusinessDate
FROM `{env_prefix}-cdh`.derivative.drealestate
WHERE SourceSystem = 'thesis'
  AND BusinessDate = (SELECT MAX(BusinessDate) FROM `{env_prefix}-cdh`.derivative.drealestate)
  AND RealEstateCepalKey IS NOT NULL
  --and RealEstateActive = 'Active'
""")

# COMMAND ----------

# DBTITLE 1,v_cte_040 — FOP increase flag per auction round
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_040_fop_increase_flag AS
WITH ranked AS (
    SELECT
        AuctionRound_property_id,
        AuctionDate,
        CAST(FirstOfferPrice AS DECIMAL(18,2)) AS FirstOfferPrice,
        ROW_NUMBER() OVER (PARTITION BY AuctionRound_property_id ORDER BY AuctionDate DESC, tia_auction DESC) AS rn_desc
    FROM v_cte_005_tia_data_base
    WHERE AuctionRound_property_id IS NOT NULL
      AND AuctionDate IS NOT NULL
      AND FirstOfferPrice IS NOT NULL
),
last_prev AS (
    SELECT
        AuctionRound_property_id,
        MAX(CASE WHEN rn_desc = 1 THEN FirstOfferPrice END) AS fop_last,
        MAX(CASE WHEN rn_desc = 2 THEN FirstOfferPrice END) AS fop_prev
    FROM ranked WHERE rn_desc IN (1, 2)
    GROUP BY AuctionRound_property_id
)
SELECT
    AuctionRound_property_id,
    fop_last,
    fop_prev,
    CASE
        WHEN fop_prev IS NULL THEN 1   -- δεν υπάρχει προηγούμενο round => δεν ανιχνεύεται αύξηση
        WHEN fop_last > fop_prev THEN 0 -- αύξηση στο τελευταίο vs προηγούμενο => exclude
        ELSE 1
    END AS increasedFOP_val_flag

FROM last_prev
""")

# COMMAND ----------

# DBTITLE 1,v_cte_050 — TIA thesis presence check
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_cte_050_tia_thesis_data AS
SELECT DISTINCT AuctionObJID AS tia_thesis_ObJID
FROM v_tia_thesis_new
""")

# COMMAND ----------

# DBTITLE 1,v_cte_060 — Full enrichment: DRE + FOP + thesis flag
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_060_temp_union_data_B AS
SELECT
    A.*,
    D.dRE_REActive,
    D.dRE_MarketValue,
    D.dRE_MarketValueType,
    D.dRE_MarketValueDate,
    CASE WHEN T.tia_thesis_ObJID = A.tia_ObjKey THEN 1 ELSE 0 END AS case_in_tia_thesis_flag,
    F.increasedFOP_val_flag
FROM v_cte_030_temp_union_data_A AS A
LEFT JOIN v_cte_035_drealestate_data AS D
    ON D.dRE_RECepalKey = A.tia_CepalKeyFinal_splited
LEFT JOIN v_cte_050_tia_thesis_data AS T
    ON T.tia_thesis_ObJID = A.tia_ObjKey
LEFT JOIN v_cte_040_fop_increase_flag AS F
    ON F.AuctionRound_property_id = A.AuctionRound_property_id
""")

# COMMAND ----------

# DBTITLE 1,v_cte_065 — Final reporting dataset with do_not_update_mv flag
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_cte_065_temp_union_data_C AS
SELECT DISTINCT
    tia_auction, tia_ObjKey, tia_CepalKeyFinal, tia_Source,
    UniqueCode, AuctionDate,
    CAST(FirstOfferPrice AS DECIMAL(24,2)) AS FirstOfferPrice,
    try_divide(CAST(FirstOfferPrice AS DECIMAL(24,2)), CAST(1000000 AS DECIMAL(24,2))) AS FirstOfferPrice_m,
    CAST(null AS DECIMAL(24,2))  as FirstOfferPrice2,
    FinalPrice, result_blended, Tags, full_ownership, katasx_date,
    AuctionRound_property_id, AuctionRound_property, initiator_cepal_others,
    tia_UIMatchingAuctionStatus, tia_CepalKeyFinal_splited,
    rn_tia_ObjKey_over_tia_auction, dc_objkey_over_auction, dc_splited_over_auction,
    dc_tia_CepalKeyFinal_splited_over_tia_ObjKey, split_mode_step, perimeter_valid,
    case when dRE_REActive is not null then dRE_REActive else '0' end as dRE_REActive,
    case when dRE_MarketValue is not null then CAST(dRE_MarketValue AS DECIMAL(24,2)) else 0 end as dRE_MarketValue,
    case when dRE_MarketValue is not null then try_divide(CAST(dRE_MarketValue AS DECIMAL(24,2)), CAST(1000000 AS DECIMAL(24,2))) else 0 end as dRE_MarketValue_m,
    case when dRE_REActive = 'Active' and dRE_MarketValueType is null then 'Undefined' else dRE_MarketValueType end as dRE_MarketValueType,
    case when dRE_MarketValueDate is not null then dRE_MarketValueDate else '1900-01-01' end as dRE_MarketValueDate,
    case_in_tia_thesis_flag,
    increasedFOP_val_flag, -- otan 1 δεν υπάρχει προηγούμενο round => δεν ανιχνεύεται αύξηση
    CASE
        WHEN tia_UIMatchingAuctionStatus = 'validated-matched'
             AND tia_CepalKeyFinal = LEAD(tia_CepalKeyFinal) OVER (ORDER BY tia_CepalKeyFinal, AuctionDate ASC)
        THEN 0
        WHEN tia_UIMatchingAuctionStatus = 'Unmatched' THEN 0
        ELSE 1
    END AS unique_asset,
    CAST(
    case
        when perimeter_valid = 1
        and tia_UIMatchingAuctionStatus =  'validated-matched'
        and split_mode_step  = 'single_pid_explode'
        and dc_tia_CepalKeyFinal_splited_over_tia_ObjKey = 1
        and increasedFOP_val_flag  =   1
        then 0
        else 1
    end AS INT) as do_not_update_mv,
    Today AS Businessdate,
    CURRENT_DATE() AS Rundate
FROM v_cte_060_temp_union_data_B
""")

# COMMAND ----------

# DBTITLE 1,Write reporting table + REPAIR
mv_reporting_df = spark.sql("SELECT DISTINCT * FROM v_cte_065_temp_union_data_C")

mv_reporting_df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(mv_reporting_table)

spark.sql(f"REPAIR TABLE {mv_reporting_table} SYNC METADATA;")