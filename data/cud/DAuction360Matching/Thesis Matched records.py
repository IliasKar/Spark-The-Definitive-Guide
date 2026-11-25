# Databricks notebook source
# DBTITLE 1,Env Variables
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

# Function to get AWS credentials from Databricks secrets.
def get_aws_credentials(secrets_scope, key_name):
    try:
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None
    
# Function to get AWS credentials from Databricks secrets.
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration.
        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets.
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)                           
else:
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')                           


# Function to connect to AWS and access SSM parameter.
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path.
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

# COMMAND ----------

from pyspark.sql import DataFrame
import datetime
import time
from pyspark.sql.functions import col, udf, lit, concat, concat_ws, collect_list, countDistinct, sum, when, count
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType,  TimestampType,\
    StructType, StructField, FloatType
from pyspark.sql.functions import current_timestamp


script_timestamp = current_timestamp()
matching_algorithm_version = 4.0
matching_algorithm_version_date = datetime.datetime(2023, 11, 6)
last_updated_by = "MatchingAlgorithm"
target_table = f"`{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching`"
party_table = f"`{env_prefix}-cdh`.primary.party"
realestateinfo_table = f"`{env_prefix}-cdh`.primary.RealEstateInfo"
realestatebuilding_table =  f"`{env_prefix}-cdh`.primary.RealEstateBuilding"
auctionrealestate_table = f"`{env_prefix}-cdh`.derivative.DAuctionRealEstate"
dauction_table = f"`{env_prefix}-cdh`.`derivativeactive`.`dauction`"
exposure_table =  f"`{env_prefix}-cdh`.primary.exposure"


# COMMAND ----------

# DBTITLE 1,Source df
source_query = f"""
select distinct A.sourcesystem as SourceSystem,  A.BusinessDate, A.TaxRegistrationNumber as SourceOwnerTaxRegistrationNumber, A.FirstNameGR as SourceOwnerFirstName, A.LastNameGR as SourceOwnerLastName,
        CASE WHEN A.sourcesystem ="arotron" THEN LTRIM(RTRIM(C.realestatekey)) ELSE LTRIM(RTRIM(C.RealEstateCepalKey)) END as SourceRealEstateKey, 
        C.RealEstateStatusCode, C.RealEstateAddress1 as SourceRealEstateAddress1,
        C.RealEstateAddress2 as SourceRealEstateAddress2, C.RealEstateArea,
        C.RealEstateBuildingNumber, C.RealEstateSubCategoryCode as SourceRealEstateSubCategoryCode, C.RealEstateCategoryCode as SourceRealEstateCategoryCode,
        C.RealEstateBuildingNetArea as SourceRealEstateBuildingNetArea, C.RealEstateStatus AS SourceRealEstateStatus,
        C.RealEstateCity as SourceRealEstateCity, C.RealEstateDescription as SourceRealEstateDescription, LTRIM(RTRIM(C.RealEstateLandRegistryCode)) as SourceKAEK, C.RealEstateLocationLambda,
        C.RealEstateLocationPhi, C.RealEstateNetArea as SourceRealEstateNetArea, C.RealEstateCounty, C.RealEstateMunicipality as SourceRealEstateMunicipality,
        C.RealEstateDistrict, C.RealEstateDivision, C.RealEstateActive as SourceRealEstateActive, C.RealEstateUsageCodeType as SourceRealEstateUsageCodeType,
        C.RealEstateConstructionYear, C.RealEstatePostalCode as SourceRealEstatePostalCode, C.RealEstateBuildingFloorRawValue as SourceRealEstateBuildingFloorRawValue, 
        C.RealEstateBuildingNumber as SourceRealEstateBuildingNumber, B.OwnershipStatusCode, B.OwnershipPercentage as SourceOwnershipPercentage, B.OwnershipTypeCode as SourceOwnershipTypeCode,  LTRIM(RTRIM(C.RealEstateAuctionCode)) as SourceRealEstateAuctionCode, 
        C.RealEstateBuildingParkingArea AS SourceRealEstateBuildingParkingArea, 
        C.RealEstateBuildingOtherAuxiliaryArea AS SourceRealEstateBuildingOtherAuxiliaryArea, 
        C.RealEstateBuildingStorageArea AS SourceRealEstateBuildingStorageArea
        from {party_table} as A
        inner join (  select distinct B.realestatekey,B.OwnershipStatusCode, B.OwnershipPercentage, B.OwnershipTypeCode, B.BusinessDate, B.OwnerPartyKey,b.SourceSystem 
                    from {realestateinfo_table} B) B
        on A.PartyKey =  B.OwnerPartyKey  
        and a.BusinessDate=b.BusinessDate
        and a.SourceSystem=b.SourceSystem
        inner join  (                    
            Select distinct C.BusinessDate, C.realestatekey, C.RealEstateCepalKey, C.RealEstateStatus, C.RealEstateStatusCode, C.RealEstateAddress1, C.RealEstateAddress2, C.RealEstateArea,
            C.RealEstateBuildingNumber, C.RealEstateSubCategoryCode, C.RealEstateCategoryCode,C.RealEstateBuildingNetArea, C.RealEstateBuildingStorageArea, 
            C.RealEstateCity, C.RealEstateDescription, C.RealEstateLandRegistryCode, C.RealEstateLocationLambda,
            C.RealEstateLocationPhi, C.RealEstateNetArea, C.RealEstateCounty, C.RealEstateMunicipality,
            C.RealEstateDistrict, C.RealEstateDivision, C.RealEstateActive, C.RealEstateUsageCodeType, C.RealEstateBuildingFloorRawValue, C.RealEstateBuildingNumber,c.SourceSystem,
            C.RealEstateConstructionYear, C.RealEstatePostalCode ,  C.RealEstateAuctionCode, 
            C.RealEstateBuildingOtherAuxiliaryArea, C.RealEstateBuildingParkingarea,  C.RealEstateBuildingStorageArea
            from {realestatebuilding_table} C 
            where (coalesce(RealEstateActive,'Active') = "Active"
                    AND NOT (C.SourceSystem = 'thesis' AND RealEstateStatus = 'REOco')) 
              or (C.SourceSystem = 'thesis' and 
                    coalesce(RealEstateActive,'Inactive') = "Inactive" and 
                     RealEstateStatus not in ('DT enhancement', 'Πωληθέν', 'Replacement','Double', 'Split')
                    )
        ) C
        on C.realestatekey =  B.realestatekey  
        and c.BusinessDate = b.BusinessDate
        and c.SourceSystem = b.SourceSystem
        where a.BusinessDate = (  select max(BusinessDate) BusinessDate
                                from {realestateinfo_table}) 
                """

source_df = spark.sql(source_query).filter(
  (col("SourceSystem") == 'thesis') & col("SourceRealEstateAuctionCode").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC \s matches whitespace (space, tab, newline, etc.)
# MAGIC
# MAGIC \u00A0 is non-breaking space (common when copying from HTML/PDF)
# MAGIC
# MAGIC \u200B is zero-width space
# MAGIC
# MAGIC trim() removes leading/trailing spaces (even after cleanup)

# COMMAND ----------

# DBTITLE 1,SourceRealEstateAuctionCode Preprocessing
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import regexp_replace, trim, col

source_df = source_df.withColumn("SourceRealEstateAuctionCode", regexp_replace(col("SourceRealEstateAuctionCode"), "\n", ""))

source_df = source_df.withColumn(
    "SourceRealEstateAuctionCode",
    trim(regexp_replace(col("SourceRealEstateAuctionCode"), r"[\s\u00A0\u200B]+", ""))
)


# COMMAND ----------

# DBTITLE 1,Exploding comma delimited values
from pyspark.sql.functions import split, explode, col

# Step 1: Split the comma-separated string into an array
source_df = source_df.withColumn(
    "AuctionCodeArray",
    split(col("SourceRealEstateAuctionCode"), ",")
)

# Step 2: Explode the array into multiple rows
source_df = source_df.withColumn(
    "SourceRealEstateAuctionCode",
    explode(col("AuctionCodeArray"))
).drop("AuctionCodeArray")


# COMMAND ----------

# DBTITLE 1,DAuction360Matching_df

DAuction360Matching_query = f"""
    SELECT a.*
    FROM {target_table} a
    JOIN (
    SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
    FROM {target_table}
    GROUP BY AuctionMatching360ID
    ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
        AND a.ingestiontimestamp = b.max_timestamp
        AND ( a.UIMatchingAuctionStatus != 'Deprecated' OR a.UIMatchingAuctionStatus IS NULL )
"""

DAuction360Matching_df = spark.sql(DAuction360Matching_query)

# COMMAND ----------

# DBTITLE 1,Thesis matched data
thesis_matched = source_df \
    .select("SourceRealEstateKey", "SourceRealEstateAuctionCode") \
    .join(
        DAuction360Matching_df.select("IncomingRealEstateKey", "IncomingRealEstateGroupCode"),
        DAuction360Matching_df.IncomingRealEstateKey == source_df.SourceRealEstateAuctionCode,
        "inner"
    ) \
    .distinct()

thesis_matched_nogroupcode = thesis_matched.filter(col("IncomingRealEstateGroupCode").isNull())

for c in thesis_matched_nogroupcode.columns:
    thesis_matched_nogroupcode = thesis_matched_nogroupcode.withColumnRenamed(c, c + "_u")

thesis_matched_withgroupcode = thesis_matched.filter(col("IncomingRealEstateGroupCode").isNotNull())

for c in thesis_matched_withgroupcode.columns:
    thesis_matched_withgroupcode = thesis_matched_withgroupcode.withColumnRenamed(c, c + "_u")

# COMMAND ----------

# DBTITLE 1,Records already matched in thesis  withOUT GroupCode  A360-570
last_updated_by_df_null = DAuction360Matching_df.filter(
  ( col("SourceRealEstateKey").isNotNull() ) & (col("SourceRealEstateKey") != '') & (col("SourceSystem") == 'thesis') 
).join(thesis_matched_nogroupcode.select("SourceRealEstateAuctionCode_u", "SourceRealEstateKey_u"), 
                      (DAuction360Matching_df.IncomingRealEstateKey == thesis_matched_nogroupcode.SourceRealEstateAuctionCode_u) &
                      (DAuction360Matching_df.SourceRealEstateKey == thesis_matched_nogroupcode.SourceRealEstateKey_u)
                      ).withColumn("LastUpdatedBy", lit("system-matched")
                      ).withColumn("UIMatchingAuctionStatus", lit("validated-matched")).drop(
                         "SourceRealEstateAuctionCode_u", "SourceRealEstateKey_u", "IncomingRealEstateKey_u", "IncomingRealEstateGroupCode_u")

# COMMAND ----------

# DBTITLE 1,Records already matched in thesis with GroupCode A360-517
last_updated_by_df = DAuction360Matching_df.filter(
( col("SourceRealEstateKey").isNotNull() ) & (col("SourceRealEstateKey") != '') & (col("SourceSystem") == 'thesis') 
).join(thesis_matched_withgroupcode.select("IncomingRealEstateGroupCode_u", "SourceRealEstateKey_u"), 
                      (DAuction360Matching_df.IncomingRealEstateGroupCode == thesis_matched_withgroupcode.IncomingRealEstateGroupCode_u) &
                      (DAuction360Matching_df.SourceRealEstateKey == thesis_matched_withgroupcode.SourceRealEstateKey_u)
                      ).withColumn("LastUpdatedBy", lit("system-matched")
                      ).withColumn("UIMatchingAuctionStatus", lit("validated-matched")).drop(
                         "SourceRealEstateAuctionCode_u", "SourceRealEstateKey_u", "IncomingRealEstateKey_u", "IncomingRealEstateGroupCode_u")



# COMMAND ----------

# DBTITLE 1,Total and raise the IngestionTimestamp
total_thesis_matched = last_updated_by_df.union(last_updated_by_df_null)
total_thesis_matched = total_thesis_matched.withColumn("IngestionTimestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Exclude Thesis Matched that are already matched in A360 UI
filtered_total_thesis_matched = total_thesis_matched.join(
    DAuction360Matching_df.filter(col("UIMatchingAuctionStatus") == "validated-matched").select("AuctionMatching360ID"),
    on="AuctionMatching360ID",
    how="left_anti"
)

display(filtered_total_thesis_matched)

# COMMAND ----------

print(target_table)

# COMMAND ----------

# DBTITLE 1,Write data
filtered_total_thesis_matched.distinct().write.format("delta").mode("append").saveAsTable(f"{target_table}")
spark.sql(f"REPAIR TABLE {target_table} SYNC METADATA")
