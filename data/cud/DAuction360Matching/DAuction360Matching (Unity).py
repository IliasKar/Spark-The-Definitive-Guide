# Databricks notebook source
# MAGIC %md
# MAGIC #### Authentication cell

# COMMAND ----------

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

# MAGIC %md
# MAGIC #### Get the application specific SSM values

# COMMAND ----------

s3_path = connect_to_aws_and_access_ssm('/cfn/a360/matching/conf_s3_path')

# ATTENTION! Change the cibnf param on your env SSM to enable / disable pre-release mode
pre_release = connect_to_aws_and_access_ssm('/cfn/a360/matching/pre_release')

# Initialize the S3 client
s3_client = boto3.client("s3", region_name="eu-central-1")

# Specify the S3 bucket and folder
bucket_name = s3_path.split("/a360")[0].replace("s3://","")
s3_path_short = s3_path.split(bucket_name)[-1]

run_date = dbutils.widgets.get("run_date")
export = dbutils.widgets.get("export")

print(f"run_date={run_date}")
print(f"export={export}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MAIN SCRIPT

# COMMAND ----------

# DBTITLE 1,START
import datetime
from functools import reduce
import time
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (col,
                                   udf,
                                   lit,
                                   concat,
                                   concat_ws,
                                   collect_list,
                                   countDistinct,
                                   sum,
                                   when,
                                   count,
                                   length,
                                   trim,
                                   current_timestamp)
from pyspark.sql.types import (StringType, 
                               IntegerType,
                               DoubleType,
                               BooleanType,
                               TimestampType,
                               StructType,
                               StructField,
                               FloatType)
from utility_notebook.utils import sum_net_area, unifying_address

#Parameters
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

# Start the timer
start_time = time.time()

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

DAuction360Matching_ui_query = f"""
        SELECT distinct a.*
        FROM {target_table} a
        JOIN (
        SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
        FROM {target_table}
        GROUP BY AuctionMatching360ID
        ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
            AND a.ingestiontimestamp = b.max_timestamp
            WHERE 
            (a.UIAssignee IS NOT NULL AND a.UIAssignee != '')
            OR (a.UIApprovalFlag IS NOT NULL)
            OR (TRY_CAST(a.UIApprovalDate AS TIMESTAMP) IS NOT NULL)
            OR (a.UIMatchingAuctionStatus NOT IN ('toProcess', 'toValidate', 'Deprecated') AND a.UIMatchingAuctionStatus IS NOT NULL AND a.UIMatchingAuctionStatus != '')
"""


#this DF contains the UI touched records only (separate treatment)
DAuction360Matching_ui_df = spark.sql(DAuction360Matching_ui_query)

#clean the UI touched records from the base query
DAuction360Matching_df = DAuction360Matching_df.subtract(DAuction360Matching_ui_df)

# create an empty data frame with the same schema as the DAuction360Matching_df to temprary store the matching results.
DAuction360Matching_new_df = spark.createDataFrame([], DAuction360Matching_df.schema)

# COMMAND ----------

# DBTITLE 1,Source DF
source_query = f"""
select distinct A.sourcesystem as SourceSystem,
        A.BusinessDate,
        A.TaxRegistrationNumber as SourceOwnerTaxRegistrationNumber,
        A.FirstNameGR as SourceOwnerFirstName,
        A.LastNameGR as SourceOwnerLastName,
        CASE WHEN A.sourcesystem ="arotron" THEN LTRIM(RTRIM(C.realestatekey)) ELSE LTRIM(RTRIM(C.RealEstateCepalKey)) END as SourceRealEstateKey, 
        C.RealEstateStatusCode,
        C.RealEstateAddress1 as SourceRealEstateAddress1,
        C.RealEstateAddress2 as SourceRealEstateAddress2,
        C.RealEstateArea,
        C.RealEstateBuildingNumber,
        C.RealEstateSubCategoryCode as SourceRealEstateSubCategoryCode,
        C.RealEstateCategoryCode as SourceRealEstateCategoryCode,
        C.RealEstateBuildingNetArea as SourceRealEstateBuildingNetArea,
        C.RealEstateStatus AS SourceRealEstateStatus,
        C.RealEstateCity as SourceRealEstateCity,
        C.RealEstateDescription as SourceRealEstateDescription,
        LTRIM(RTRIM(C.RealEstateLandRegistryCode)) as SourceKAEK,
        C.RealEstateLocationLambda,
        C.RealEstateLocationPhi,
        C.RealEstateLandArea as SourceRealEstateLandArea,
        C.RealEstateNetArea as SourceRealEstateNetArea,
        C.RealEstateCounty,
        C.RealEstateMunicipality as SourceRealEstateMunicipality,
        C.RealEstateDistrict,
        C.RealEstateDivision,
        C.RealEstateActive as SourceRealEstateActive,
        C.RealEstateUsageCodeType as SourceRealEstateUsageCodeType,
        C.RealEstateConstructionYear,
        C.RealEstatePostalCode as SourceRealEstatePostalCode,
        C.RealEstateBuildingFloorRawValue as SourceRealEstateBuildingFloorRawValue, 
        C.RealEstateBuildingFloorRawValueDescription AS SourceRealEstateBuildingFloorRawValueDescription,
        C.RealEstateBuildingNumber as SourceRealEstateBuildingNumber,
        B.OwnershipStatusCode,
        B.OwnershipPercentage as SourceOwnershipPercentage,
        B.OwnershipTypeCode as SourceOwnershipTypeCode,
        LTRIM(RTRIM(C.RealEstateAuctionCode)) as SourceRealEstateAuctionCode, 
        C.RealEstateBuildingParkingArea AS SourceRealEstateBuildingParkingArea, 
        C.RealEstateBuildingOtherAuxiliaryArea AS SourceRealEstateBuildingOtherAuxiliaryArea, 
        C.RealEstateBuildingStorageArea AS SourceRealEstateBuildingStorageArea
        from {party_table} AS A
        inner join (select distinct realestatekey,
                                      OwnershipStatusCode,
                                      OwnershipPercentage,
                                      OwnershipTypeCode,
                                      BusinessDate,
                                      OwnerPartyKey,
                                      SourceSystem 
                    from {realestateinfo_table}
                    ) AS B
        on A.PartyKey =  B.OwnerPartyKey  
        and A.BusinessDate=B.BusinessDate
        and A.SourceSystem=B.SourceSystem
        inner join  (                    
            Select distinct BusinessDate,
                            realestatekey,
                            RealEstateCepalKey,
                            RealEstateStatus,
                            RealEstateStatusCode,
                            RealEstateAddress1,
                            RealEstateAddress2,
                            RealEstateArea,
                            RealEstateBuildingNumber,
                            RealEstateSubCategoryCode,
                            RealEstateCategoryCode,
                            RealEstateBuildingNetArea,
                            RealEstateBuildingStorageArea, 
                            RealEstateCity,
                            RealEstateDescription,
                            RealEstateLandRegistryCode,
                            RealEstateLocationLambda,
                            RealEstateLocationPhi,
                            RealEstateLandArea,
                            RealEstateNetArea,
                            RealEstateCounty,
                            RealEstateMunicipality,
                            RealEstateDistrict,
                            RealEstateDivision,
                            RealEstateActive,
                            RealEstateUsageCodeType,
                            RealEstateBuildingFloorRawValue,
                            RealEstateBuildingFloorRawValueDescription,
                            RealEstateBuildingNumber,
                            SourceSystem,
                            RealEstateConstructionYear,
                            RealEstatePostalCode,
                            RealEstateAuctionCode, 
                            RealEstateBuildingOtherAuxiliaryArea,
                            RealEstateBuildingParkingarea,
                            RealEstateBuildingStorageArea
            from {realestatebuilding_table} 
            where (coalesce(CASE WHEN RealEstateActive = 'true' THEN NULL ELSE RealEstateActive END, 'Active') = "Active"
                    AND NOT (SourceSystem = 'thesis' AND RealEstateStatus = 'REOco')) 
              or (SourceSystem = 'thesis' and coalesce(RealEstateActive,'Inactive') = "Inactive" and 
                  RealEstateStatus not in ('DT enhancement', 'Πωληθέν', 'Replacement','Double', 'Split'))
        ) AS C
        on C.realestatekey =  B.realestatekey  
        and C.BusinessDate = B.BusinessDate
        and C.SourceSystem = B.SourceSystem
        where A.BusinessDate = (select max(BusinessDate) BusinessDate
                                from {realestateinfo_table}) 
        """

source_df = spark.sql(source_query)

unifying_address_udf = udf(unifying_address, StringType())
sum_net_area_udf = udf(sum_net_area, DoubleType())

source_df = source_df.withColumn(
    "SourceRealEstateNetArea",
    F.coalesce(col("SourceRealEstateNetArea"), col("SourceRealEstateBuildingNetArea"))
)

source_df = source_df.withColumn(
    "SourceRealEstateBuildingNetArea_sum",
    sum_net_area_udf(
        source_df.SourceRealEstateBuildingNetArea, 
        source_df.SourceRealEstateBuildingStorageArea, 
        source_df.SourceRealEstateBuildingParkingArea)
)

source_df = source_df.withColumn(
    "SourceRealEstateAddress1",
    unifying_address_udf(
        source_df.SourceRealEstateAddress1,
        source_df.SourceRealEstateAddress2)
)


# COMMAND ----------

# DBTITLE 1,SourceAggregatedRealEstateBuildingFloorRawValue A360-523
source_df_to_aggregate = source_df

aggregated_df = source_df_to_aggregate.groupBy("SourceRealEstateKey") \
    .agg(concat_ws(",", collect_list("SourceRealEstateBuildingFloorRawValue")).alias("SourceAggregatedRealEstateBuildingFloorRawValue"))

# Step 2: Join the aggregated result back to the original DataFrame
source_df = source_df_to_aggregate.join(aggregated_df, on="SourceRealEstateKey", how="left")

del source_df_to_aggregate

source_df.cache()

# COMMAND ----------

# DBTITLE 1,Newsphone DF
if run_date == 'all':
    filedate_max_filter = ''
else:
    filedate_max_filter = f"AND are.filedate = {run_date}"

newsphone_query = f"""
SELECT
    CASE 
        WHEN are_perimeter.OriginSourceSystem = 'eAuction' THEN 
            CASE 
                WHEN are_perimeter.TNMergeCode IS NOT NULL AND are_perimeter.TNMergeCode != 0 THEN are_perimeter.TNMergeCode
                ELSE are_perimeter.AuctionRealEstateCode
            END
        WHEN are_perimeter.OriginSourceSystem = 'Δελτίο Δικαστικών Δημοσιεύσεων' AND NOT EXISTS (
            SELECT 1 
            FROM {auctionrealestate_table} eauction_rows 
            WHERE eauction_rows.AuctionKey = are_perimeter.AuctionKey 
                AND eauction_rows.OriginSourceSystem = 'eAuction'
                AND eauction_rows.ActiveFlag = True
                AND eauction_rows.BusinessDate = (
                        SELECT MAX(BusinessDate) 
                        FROM {auctionrealestate_table}
                    )
        ) THEN are_perimeter.AuctionObjKey
        ELSE NULL
    END AS IncomingCalculatedAuctionObjKey,
    are_perimeter.TNMergeCode AS IncomingTNMergeCode,
    LTRIM(RTRIM(are_perimeter.LandRegistration)) AS IncomingKAEK,
    are_perimeter.OriginSourceSystem AS IncomingOriginSourceSystem,
    are_perimeter.AuctionObjKey AS IncomingAuctionObjKey,  
    are_perimeter.AuctionKey AS IncomingAuctionKey,
    da.OriginAuctionKey AS IncomingOriginAuctionKey,
    are_perimeter.AuctionRealEstateCode AS IncomingRealEstateKey,
    are_perimeter.RealEstateGroupCode AS IncomingRealEstateGroupCode,
    are_perimeter.DebtorTaxRegistrationNumber AS IncomingOwnerTaxRegistrationNumber,
    are_perimeter.OriginDebtorTaxRegistrationNumber AS IncomingOriginOwnerTaxRegistrationNumber,
    are_perimeter.DebtorDescription AS IncomingOwnerDescription,
    are_perimeter.PostalCode AS IncomingRealEstatePostalCode,
    are_perimeter.auctionstatus AS IncomingAuctionStatus,
    are_perimeter.Address AS IncomingRealEstateAddress,
    are_perimeter.SurfaceSqm AS IncomingRealEstateSurfaceSqm,
    are_perimeter.FloorRawValue AS IncomingRealEstateFloorRawValue,
    are_perimeter.TitleGR AS IncomingRealEstateTitle,
    are_perimeter.Category AS IncomingRealEstateCategory,
    are_perimeter.SubCategory AS IncomingRealEstateSubCategory,
    are_perimeter.Municipality AS IncomingRealEstateMunicipality,
    are_perimeter.Area AS IncomingRealEstateArea,
    are_perimeter.Tags AS IncomingRealEstateTags,
    are_perimeter.TagValues AS IncomingRealEstateTagValues,
    are_perimeter.SeizureReportNumber AS IncomingSeizureReportNumber,
    COALESCE(da.OriginHastener, are_perimeter.Hastener) AS IncomingHastenerDescr,
    da.Initiator AS IncomingInitiator,
    da.Hastener AS IncomingHastener,
    da.InitiatorCepalAB AS IncomingInitiatorCepalAB, 
    da.PortfolioCategory AS IncomingPortfolioCategory,
    da.IsHastenerServicer AS IncomingIsHastenerServicer,
    are_perimeter.AuctionDate AS IncomingAuctionDate
FROM (
    SELECT filtered_are.*
    FROM (
        SELECT *
        FROM {auctionrealestate_table}
        WHERE OriginSourceSystem = 'eAuction' 
            AND activeFlag = TRUE 
            AND BusinessDate = (
                SELECT MAX(BusinessDate) 
                FROM {auctionrealestate_table}
            )
        UNION
        SELECT *
        FROM {auctionrealestate_table} ddd_rows
        WHERE OriginSourceSystem = 'Δελτίο Δικαστικών Δημοσιεύσεων'
            AND activeFlag = TRUE 
            AND BusinessDate = (
                SELECT MAX(BusinessDate) 
                FROM {auctionrealestate_table}
            )
            AND NOT EXISTS (
                SELECT 1
                FROM {auctionrealestate_table} eauction_rows
                WHERE eauction_rows.AuctionKey = ddd_rows.AuctionKey
                    AND eauction_rows.OriginSourceSystem = 'eAuction' 
                    AND eauction_rows.activeFlag = TRUE 
                    AND eauction_rows.BusinessDate = (
                        SELECT MAX(BusinessDate) 
                        FROM {auctionrealestate_table}
                    )
            )
            AND SourceSystem = 'extfile'
    ) AS filtered_are
    JOIN (
        SELECT AuctionRealEstateId, MAX(filedate) AS filedate, MAX(businessdate) AS businessdate
        FROM {auctionrealestate_table}
        WHERE activeFlag = TRUE
        GROUP BY AuctionRealEstateId
    ) AS max_are
    ON filtered_are.AuctionRealEstateId = max_are.AuctionRealEstateId
        AND filtered_are.filedate = max_are.filedate
        AND filtered_are.businessdate = max_are.businessdate
) AS are_perimeter
INNER JOIN (
    SELECT da.*
    FROM {dauction_table} AS da
    JOIN (
        SELECT AuctionID, MAX(ReferenceDate) AS ReferenceDate 
        FROM {dauction_table}
        GROUP BY AuctionID
    ) AS da_max
    ON da.AuctionID = da_max.AuctionID 
        AND da.ReferenceDate = da_max.ReferenceDate
) AS da
ON are_perimeter.AuctionKey = da.AuctionKey
    AND are_perimeter.SourceSystem = da.SourceSystem
WHERE are_perimeter.SourceSystem = 'extfile'            
{filedate_max_filter}

"""

newsphone_df_full = spark.sql(newsphone_query)
newsphone_df_full = newsphone_df_full.withColumn("SourceSystem", lit("newsphone"))

newsphone_df_full.cache()


# COMMAND ----------

newsphone_df_full.createOrReplaceTempView("newsphone_df_view")

# COMMAND ----------

# DBTITLE 1,A360-542 additional fix for IncomingisLastAuction
updated_keys_df = spark.sql( f""" WITH LatestDates AS (
    SELECT 
        IncomingOriginAuctionKey,
        MAX(IncomingAuctionDate) AS LatestAuctionDate
    FROM newsphone_df_view
    GROUP BY IncomingOriginAuctionKey
)
SELECT 
    t.IncomingAuctionKey,
    t.IncomingOriginAuctionKey,
    t.IncomingAuctionDate,
    CASE 
        WHEN t.IncomingAuctionDate = ld.LatestAuctionDate THEN 'yes'
        ELSE NULL
    END AS IncomingisLastAuction
FROM newsphone_df_view t
LEFT JOIN LatestDates ld
    ON t.IncomingOriginAuctionKey = ld.IncomingOriginAuctionKey;
""").distinct().withColumnsRenamed(
    {
        "IncomingAuctionKey":"IncomingAuctionKey_u", 
        "IncomingOriginAuctionKey":"IncomingOriginAuctionKey_u", 
        "IncomingAuctionDate": "IncomingAuctionDate_u"
    }
)

newsphone_df_full_to_join = newsphone_df_full

newsphone_df_full = newsphone_df_full_to_join.join(
    updated_keys_df,
    newsphone_df_full_to_join["IncomingAuctionKey"] == updated_keys_df["IncomingAuctionKey_u"],
    "left"
).drop("IncomingAuctionKey_u", "IncomingOriginAuctionKey_u","IncomingAuctionDate_u")

#A360-520
newsphone_df_full = newsphone_df_full.withColumn(
    "IncomingisLastAuction",
    when(col("IncomingAuctionStatus") == "eAuction - Ολοκληρωμένος - Κατακύρωση", "yes").otherwise(
        col("IncomingisLastAuction")
        )
)
        

# COMMAND ----------

# DBTITLE 1,IncomingOwnerTaxRegistrationNumberIsCustomer Calculation
IncomingOwnerTaxRegistrationNumberIsCustomer_df = spark.sql(
    f'''SELECT DebtorTaxRegistrationNumber,
       CASE WHEN COALESCE(onbalance, 0) > 0.01 AND DebtorTaxRegistrationNumber IS NOT NULL THEN 'Y' ELSE 'N' END IsCepalFlag
FROM (
    SELECT DebtorTaxRegistrationNumber,
           SUM(onbalance) AS onbalance
    FROM (
        SELECT REPLACE(AuctRE.DebtorTaxRegistrationNumber, '-', '') AS DebtorTaxRegistrationNumber,
               exp.AgreementKey,
               Prt.Portfolioname AS Portfolioname,
               SUM(COALESCE(exp.onbalance, 0) + COALESCE(exp.DueAmount, 0)) AS onbalance
        FROM (
            SELECT DebtorTaxRegistrationNumber, MAX(FileDate) AS FileDate
            FROM {auctionrealestate_table}
            GROUP BY DebtorTaxRegistrationNumber
        ) AuctRE
        INNER JOIN (
            SELECT *
            FROM {party_table}
            WHERE businessdate = (
                SELECT MAX(businessdate) AS businessdate
                FROM {party_table}
            )
        ) Prt ON COALESCE(AuctRE.DebtorTaxRegistrationNumber, 'NA') = COALESCE(prt.TaxRegistrationNumber, 'NA')
        --AND CAST(AuctRE.FileDate AS DATE) = Prt.BusinessDate
        INNER JOIN {exposure_table} Exp ON COALESCE(prt.PartyKey, 'NA') = COALESCE(Exp.Partykey, 'N/A')
        AND prt.SourceSystem = Exp.SourceSystem
        AND Exp.SourceSystem IN ('profits', 'qca')
        AND Exp.BusinessDate = Prt.BusinessDate
        WHERE REPLACE(AuctRE.DebtorTaxRegistrationNumber, '-', '') NOT IN ('0', '000', '00')
        AND REPLACE(AuctRE.DebtorTaxRegistrationNumber, '-', '') NOT LIKE ('00%000')
        GROUP BY REPLACE(AuctRE.DebtorTaxRegistrationNumber, '-', ''),
                 exp.AgreementKey,
                 Prt.Portfolioname
    ) a
    GROUP BY DebtorTaxRegistrationNumber
    ) d;'''
    )  

cepal_afms = set(
    [ row["DebtorTaxRegistrationNumber"] for row in  
     IncomingOwnerTaxRegistrationNumberIsCustomer_df.filter(col("IsCepalFlag") == "Y").select("DebtorTaxRegistrationNumber").collect()  
     if len(row["DebtorTaxRegistrationNumber"]) > 4 ]
    )

def calc_OwnerTaxRegistrationNumberIsCustomer(afm):

    if afm in cepal_afms:
        return "Y"
    else:
        return "N"

calc_OwnerTaxRegistrationNumberIsCustomer_udf = udf(calc_OwnerTaxRegistrationNumberIsCustomer, StringType())

newsphone_df_full = newsphone_df_full.withColumn(
    "IncomingOwnerTaxRegistrationNumberIsCustomer",
    calc_OwnerTaxRegistrationNumberIsCustomer_udf(newsphone_df_full.IncomingOwnerTaxRegistrationNumber)
)


# COMMAND ----------

# DBTITLE 1,IncomingRealEstateGroupCodeStatus Calculation
AuctionRealEstateCode_query = f'''
SELECT AuctionObjKey, AuctionRealEstateCode, COUNT(DISTINCT RealEstateGroupCode) AS CountRealEstateGroupCode
FROM (
    SELECT DISTINCT AuctionObjKey, AuctionRealEstateCode, RealEstateGroupCode
    FROM {auctionrealestate_table}
    WHERE SourceSystem = 'extfile' )
    AS Subquery
GROUP BY AuctionObjKey, AuctionRealEstateCode
HAVING COUNT(DISTINCT RealEstateGroupCode) > 1; '''

AuctionRealEstateCode_df = spark.sql(AuctionRealEstateCode_query)

AuctionRealEstateCodes_set = set(
    [ row["AuctionRealEstateCode"] for row in  
     AuctionRealEstateCode_df.select("AuctionRealEstateCode").collect()   ]
    )

def calc_IncomingRealEstateGroupCodeStatus(IncomingRealEstateGroupCode):
    if IncomingRealEstateGroupCode is None:
        return False
    elif IncomingRealEstateGroupCode in AuctionRealEstateCodes_set:
        return False
    else:
        return True

calc_IncomingRealEstateGroupCodeStatus_udf = udf(calc_IncomingRealEstateGroupCodeStatus, BooleanType())

newsphone_df_full = newsphone_df_full.withColumn(
    "IncomingRealEstateGroupCodeStatus",
    calc_IncomingRealEstateGroupCodeStatus_udf(newsphone_df_full.IncomingRealEstateGroupCode)
)

# COMMAND ----------

# DBTITLE 1,Preprocessing functions
#PREPROCESSING PIPELINES
import unicodedata
import re 

# decimals pre-processing
def round_and_convert_to_string(decimal_number):

    if not decimal_number:
        return ""

    rounded_number = round(decimal_number)
    string_number = str(rounded_number)

    return string_number


def strip_greek_accents(input_string):
    # Normalize the string to decompose composite characters
    normalized = unicodedata.normalize("NFD", input_string)

    # Use a filter to remove diacritic marks and join characters back together
    stripped_string = "".join(
        [char for char in normalized if unicodedata.category(char) != "Mn"]
    )

    return stripped_string


def remove_non_letters(input_string):
    return "".join([char for char in input_string if char.isalpha()])


def remove_substrings(s: str, 
                      to_remove: list = ["nan", "none", "null", "\n"]) -> str:

    for substring in to_remove:
        s = s.replace(substring, "")

    return s


def remove_spaces(s: str) -> str:
    return re.sub(r"\s+", "", s)


def string_cast_normalize(s: int | float | str) -> str:

    s = str(s)
    s = s.lower()
    s = strip_greek_accents(s)
    s = remove_spaces(s)
    s = remove_substrings(s)

    return s

def is_empty_or_none(s):
    if not s:
        return True
    else:
        return False

def jaccard_similarity(x: str, y: str) -> float:

    intersection_cardinality: int = 0
    union_cardinality: int = 0
    similarity: float = 0.0

    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    similarity = intersection_cardinality / float(union_cardinality)

    return similarity


def jaro_winkler_similarity(str1: str, str2: str, 
                            p: float = 0.1, max_prefix: int = 0) -> float:
    
    def get_matched_characters(_str1: str, _str2: str) -> str:
        matched = []
        limit = min(len(_str1), len(_str2)) // 2
        for i, l in enumerate(_str1):
            left = int(max(0, i - limit))
            right = int(min(i + limit + 1, len(_str2)))
            if l in _str2[left:right]:
                matched.append(l)
                _str2 = f"{_str2[0:_str2.index(l)]} {_str2[_str2.index(l) + 1:]}"
 
        return "".join(matched)
 
    # matching characters
    matching_1 = get_matched_characters(str1, str2)
    matching_2 = get_matched_characters(str2, str1)
    match_count = len(matching_1)
 
    # transposition
    transpositions = (
        len([(c1, c2) for c1, c2 in zip(matching_1, matching_2) if c1 != c2]) // 2
    )
 
    if not match_count:
        jaro = 0.0
    else:
        jaro = (
            1
            / 3
            * (
                match_count / len(str1)
                + match_count / len(str2)
                + (match_count - transpositions) / match_count
            )
        )
 
    # common prefix up to max_prefix characters
    prefix_len = 0
    for c1, c2 in zip(str1[:max_prefix], str2[:max_prefix]):
        if c1 == c2:
            prefix_len += 1
        else:
            break

    return jaro + p * prefix_len * (1 - jaro)


def string_comparison(x, y):

    conditions = [
        not is_empty_or_none(x), 
        not is_empty_or_none(y)
        ]

    if all(conditions):
        return jaccard_similarity(x, y)
    else:
        return 0.0

def compare_positions(a_string, b_string):

    found = 0

    for a, b in zip(a_string, b_string):
        if a == b:
            found +=1

    score = found / len(a_string)

    return score

def string_comparison_ordered(x, y):

    conditions = [
        not is_empty_or_none(x), 
        not is_empty_or_none(y)
        ]

    if all(conditions):
        if len(x) == len(y):
            return compare_positions(x, y)
        return jaro_winkler_similarity(x, y)
    else:
        return 0.0

def change(x, y):
    # Handle the edge case where both x and y are 0 to avoid division by zero
    if x == 0 and y == 0:
        return 0  # No change if both are zero
    
    C = (abs(max(x,y) - min(x,y)) / ((abs(x) + abs(y)) / 2))

    return C

def numeric_comparison(x, y):

    if not x:
        x = 0.0
    if not y:
        y = 0.0
    
    c = change(x, y)

    if c > 1: # y > 3x
        return 0
    else:
        return 1- c

# COMMAND ----------

# DBTITLE 1,Mapping for floor values
#Mapping dict for floor values between source and newsphone data
import csv

# Specify the CSV file path
csv_file_path = 'floor_mapping.csv'

# Read the CSV file into a dictionary
floor_mapping_dict = {}
with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    header = next(csv_reader)  # Skip the header row
    for row in csv_reader:
        floor, mapping = row
        floor_mapping_dict[float(floor)] = mapping if mapping != '' else None

newsphone_none_list = [-99.0, 0.7, 95.0, 96.0, 97.0, 99.0 ]

def map_newphone_floor(newsphone_val):
    if not newsphone_val:
        return ""
    else:
        return newsphone_val

def map_source_floor(source_val):
    if source_val in newsphone_none_list:
        return ""
    elif source_val is None:
        return ""
    else:
        return floor_mapping_dict[source_val]


# COMMAND ----------

# DBTITLE 1,Intermediate results - A360-604
newsphone_df_full = newsphone_df_full.drop("SourceSystem")

#Source intermediate DF 
intermediate_result_df = newsphone_df_full.join(source_df, 
                                                newsphone_df_full.IncomingOwnerTaxRegistrationNumber == \
                                                source_df.SourceOwnerTaxRegistrationNumber)

key_cols_eauction = ["IncomingOriginAuctionKey", "IncomingAuctionKey", "IncomingRealEstateKey",
            "IncomingOwnerTaxRegistrationNumber", "SourceSystem", "SourceRealEstateKey"]

key_cols_ddd = ["IncomingOriginAuctionKey", "IncomingAuctionKey", "IncomingAuctionObjKey", 
            "IncomingOwnerTaxRegistrationNumber", "SourceSystem", "SourceRealEstateKey"]

def make_cud_key(*cols):
    final_cols = []
    for c in cols:
        if not c:
            final_cols.append("")
        else:
            final_cols.append(c)
    return "|".join(final_cols)

make_cud_key_udf = udf(make_cud_key, StringType())

intermediate_result_df = intermediate_result_df.withColumn(
    "AuctionMatching360ID",
    when(
        intermediate_result_df["IncomingOriginSourceSystem"] == "eAuction",
        make_cud_key_udf(*key_cols_eauction)
    ).when(
        intermediate_result_df["IncomingOriginSourceSystem"] == "Δελτίο Δικαστικών Δημοσιεύσεων",
        make_cud_key_udf(*key_cols_ddd)
    )
).cache()

# COMMAND ----------

# DBTITLE 1,Matching algorithm
#The main matching function!!!
def matching(thesis_afm_col, thesis_zip_col,
    thesis_address_1_col,
    thesis_netarea_col, thesis_floor_value_col,
    newsphone_afm_col, newsphone_zip_col,
    newsphone_address_1_col, newsphone_netarea_col, 
    newsphone_floor_value_col):
    
    #zip col comparison
    thesis_zip_col = str(thesis_zip_col)
    newsphone_zip_col = str(newsphone_zip_col)
    zip_similarity = string_comparison_ordered(thesis_zip_col, newsphone_zip_col)

    #address col comparison
    thesis_adddress_col = str(thesis_address_1_col)
    newsphone_address_1_col = str(newsphone_address_1_col)
    thesis_adddress_col = string_cast_normalize(thesis_adddress_col)
    newsphone_address_1_col = string_cast_normalize(newsphone_address_1_col)
    address_similarity = string_comparison(thesis_adddress_col, newsphone_address_1_col)

    #net area col comparison
    if not thesis_netarea_col:
        thesis_netarea_col = 0.0
    if not newsphone_netarea_col:
        newsphone_netarea_col = 0.0
    net_area_similarity = numeric_comparison(thesis_netarea_col, newsphone_netarea_col)

    #floor value comparison
    thesis_floor_value_col = str(map_source_floor(thesis_floor_value_col))
    newsphone_floor_value_col = str(map_newphone_floor(newsphone_floor_value_col))
    floor_similarity = string_comparison_ordered(thesis_floor_value_col, newsphone_floor_value_col)

    #averaging out for the final similarity
    total_similarity = (zip_similarity + 
                        address_similarity + 
                        net_area_similarity + 
                        floor_similarity) / 4
    
    return total_similarity

    
def set_similarity100_sameKAEK(kaek1, kaek2, similarity):

    if not kaek1 and not kaek2:
        return similarity
    elif kaek1 == kaek2:
        return 1.0
    else:
        return similarity

matching_udf = udf(matching, FloatType())
set_similarity100_sameKAEK_udf= udf(set_similarity100_sameKAEK, FloatType())


result_df = intermediate_result_df.withColumn("MatchingPercentage", 
                                 matching_udf(col("SourceOwnerTaxRegistrationNumber"), 
                                                col("SourceRealEstatePostalCode"),
                                                col("SourceRealEstateAddress1"), 
                                                col("SourceRealEstateBuildingNetArea_sum"), 
                                                col("SourceRealEstateBuildingFloorRawValue"), 
                                                col("IncomingOwnerTaxRegistrationNumber"), 
                                                col("IncomingRealEstatePostalCode"),
                                                col("IncomingRealEstateAddress"),
                                                col("IncomingRealEstateSurfaceSqm"), 
                                                col("IncomingRealEstateFloorRawValue"))
                                            )
                                                    
result_df = result_df.withColumn(
    "MatchingPercentage",
    set_similarity100_sameKAEK_udf(result_df.IncomingKAEK, result_df.SourceKAEK, result_df.MatchingPercentage)
)

#Add the MatchingAlgorithmVersion
result_df = result_df.withColumn("MatchingAlgorithmVersion", lit(matching_algorithm_version))

#Add the MatchingAlgorithmVersionDate
result_df = result_df.withColumn("MatchingAlgorithmVersionDate", lit(matching_algorithm_version_date))

#Add the MatchingMissingFlag
result_df = result_df.withColumn("MatchingMissingFlag",lit(False))


# COMMAND ----------

# DBTITLE 1,ddd_df = IncomingAuctionKeys that have only DDD flag
ddd_df = spark.sql(f"""
WITH max_tmp AS (
  SELECT a.*
  FROM {target_table} a
  JOIN (
    SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
    FROM {target_table}
    GROUP BY AuctionMatching360ID
  ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
    AND a.ingestiontimestamp = b.max_timestamp
    AND a.UIMatchingAuctionStatus != 'Deprecated'
), exclusive_flags AS (
  SELECT 
    IncomingAuctionKey,
    COUNT(DISTINCT CASE WHEN IncomingOriginSourceSystem != 'Δελτίο Δικαστικών Δημοσιεύσεων' THEN IncomingOriginSourceSystem END) AS other_flags_count
  FROM max_tmp
  GROUP BY IncomingAuctionKey
)
SELECT max_tmp.*
FROM max_tmp
JOIN exclusive_flags ON max_tmp.IncomingAuctionKey = exclusive_flags.IncomingAuctionKey
WHERE exclusive_flags.other_flags_count = 0
AND max_tmp.IncomingOriginSourceSystem = 'Δελτίο Δικαστικών Δημοσιεύσεων'
AND max_tmp.SourceRealEstateKey <> '';
""")

#eAuction_df = IncomingAuctionKeys that have only eAuction flag
eAuction_df = spark.sql(f"""
                        WITH max_tmp AS (
  SELECT a.*
  FROM {target_table} a
  JOIN (
    SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
    FROM {target_table}
    GROUP BY AuctionMatching360ID
  ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
    AND a.ingestiontimestamp = b.max_timestamp
    AND a.UIMatchingAuctionStatus != 'Deprecated'
), exclusive_flags AS (
  SELECT 
    IncomingAuctionKey,
    COUNT(DISTINCT CASE WHEN IncomingOriginSourceSystem != 'eAuction' THEN IncomingOriginSourceSystem END) AS other_flags_count
  FROM max_tmp
  GROUP BY IncomingAuctionKey
)
SELECT max_tmp.*
FROM max_tmp
JOIN exclusive_flags ON max_tmp.IncomingAuctionKey = exclusive_flags.IncomingAuctionKey
WHERE exclusive_flags.other_flags_count = 0
AND max_tmp.IncomingOriginSourceSystem = 'eAuction'
AND max_tmp.SourceRealEstateKey <> '';
""")

# COMMAND ----------

# DBTITLE 1,result_df_eAuction = new  IncomingAuctionKeys that have only eAuction flag

# Group by IncomingAuctionKey and aggregate
aggregated_df = result_df.groupBy("IncomingAuctionKey").agg(
    countDistinct("IncomingOriginSourceSystem").alias("unique_source_systems"),
    sum(when(col("IncomingOriginSourceSystem") == "eAuction", 1).otherwise(0)).alias("eAuction_count"),
    count("*").alias("total_count")  # Count the total number of rows per key
)

# Filter to keep only records where all rows for that key are "eAuction"
filtered_df = aggregated_df.filter(
    (col("unique_source_systems") == 1) & (col("eAuction_count") == col("total_count"))
)

# Join back with the original dataframe to get the desired rows
result_df_eAuction = result_df.join(
    filtered_df.select("IncomingAuctionKey"),
    on="IncomingAuctionKey",
    how="inner"
).filter(
    (col("SourceSystem") != "") & col("SourceSystem").isNotNull() &
    (col("IncomingCalculatedAuctionObjKey") != "") & col("IncomingCalculatedAuctionObjKey").isNotNull() &
    (col("IncomingOwnerTaxRegistrationNumber") != "") & col("IncomingOwnerTaxRegistrationNumber").isNotNull() &
    (col("SourceRealEstateKey") != "") & col("SourceRealEstateKey").isNotNull()
)

#Make sure eAuction records do not already exist in dauction360matching
result_df_eAuction = result_df_eAuction.join(
    eAuction_df,
    on='IncomingAuctionKey',
    how='left_anti'
)

# COMMAND ----------

# DBTITLE 1,Inherit Info based on IncomingCalculatedAuctionObjKey
ddd_df = ddd_df.withColumnsRenamed(
    {
    "SourceSystem": "SourceSystem_u",
    "IncomingCalculatedAuctionObjKey": "IncomingCalculatedAuctionObjKey_u",
    "IncomingOwnerTaxRegistrationNumber": "IncomingOwnerTaxRegistrationNumber_u",
    "SourceRealEstateKey": "SourceRealEstateKey_u", 
    "AuctionMatching360ID": "AuctionMatching360ID_u", 
    "IncomingOriginSourceSystem":"IncomingOriginSourceSystem_u"
}
).select("AuctionMatching360ID_u", "UIMatchingAuctionStatus",
    "UIAssignee",
    "UIApprovalFlag",
    "UIApprovalDate",
    "LastUpdatedDate",
    "Comment",
    "LastUpdatedBy", 
    "SourceSystem_u", 
    "IncomingCalculatedAuctionObjKey_u", 
    "IncomingOwnerTaxRegistrationNumber_u", 
    "SourceRealEstateKey_u", 
    "IncomingOriginSourceSystem_u").filter(
    (col("SourceSystem_u") != "") & col("SourceSystem_u").isNotNull() &
    (col("IncomingCalculatedAuctionObjKey_u") != "") & col("IncomingCalculatedAuctionObjKey_u").isNotNull() &
    (col("IncomingOwnerTaxRegistrationNumber_u") != "") & col("IncomingOwnerTaxRegistrationNumber_u").isNotNull() &
    (col("SourceRealEstateKey_u") != "") & col("SourceRealEstateKey_u").isNotNull()
)

ddd_to_eauction = result_df_eAuction.join(ddd_df,
                    (result_df_eAuction["SourceSystem"] == ddd_df["SourceSystem_u"]) &
                    (result_df_eAuction["IncomingCalculatedAuctionObjKey"] == ddd_df["IncomingCalculatedAuctionObjKey_u"]) &
                    (result_df_eAuction["IncomingOwnerTaxRegistrationNumber"] == ddd_df["IncomingOwnerTaxRegistrationNumber_u"]) &
                    (result_df_eAuction["SourceRealEstateKey"] == ddd_df["SourceRealEstateKey_u"] ), 
                                       how="inner").withColumn("IngestionTimestamp", script_timestamp)

for c in ddd_to_eauction.columns:
    if "_u" in c:
        ddd_to_eauction = ddd_to_eauction.drop(c)

# COMMAND ----------

# DBTITLE 1,Create missing newsphone records from the sources
def construct_missing_found_df(target_df, source_system):

    target_df = target_df.filter(col("SourceSystem") == source_system)

    #Missing df
    missing_source_df = newsphone_df_full.join(target_df,  
                                        newsphone_df_full.IncomingOwnerTaxRegistrationNumber == \
                                        target_df.SourceOwnerTaxRegistrationNumber,
                                        "left_anti")

    matching_algorithm_version = 0.0
    matching_algorithm_version_date = datetime.datetime(1, 1, 1)

    for c in source_df.columns:
        missing_source_df = missing_source_df.withColumn(c, lit(None))

    missing_source_df = missing_source_df.withColumn("SourceSystem", lit(source_system))
    missing_source_df = missing_source_df.withColumn("MatchingPercentage", lit(0.0))
    missing_source_df = missing_source_df.withColumn("MatchingAlgorithmVersion", lit(matching_algorithm_version))
    missing_source_df = missing_source_df.withColumn("MatchingAlgorithmVersionDate", lit(matching_algorithm_version_date))
    missing_source_df = missing_source_df.withColumn("MatchingMissingFlag", lit(True))


    #Found df
    found_source_df = newsphone_df_full.join(target_df,  
                                        newsphone_df_full.IncomingOwnerTaxRegistrationNumber == \
                                        target_df.SourceOwnerTaxRegistrationNumber)

    for c in source_df.columns:
        found_source_df = found_source_df.withColumn(c, lit(None))

    found_source_df = found_source_df.withColumn("SourceSystem", lit(source_system))
    found_source_df = found_source_df.withColumn("MatchingPercentage", lit(0.0))
    found_source_df = found_source_df.withColumn("MatchingAlgorithmVersion", lit(matching_algorithm_version))
    found_source_df = found_source_df.withColumn("MatchingAlgorithmVersionDate", lit(matching_algorithm_version_date))
    found_source_df = found_source_df.withColumn("MatchingMissingFlag", lit(False))

    #Merge Missing and Found dfs
    total_df = found_source_df.union(missing_source_df)

    total_df = total_df.withColumn(
    "AuctionMatching360ID",
    when(
        total_df["IncomingOriginSourceSystem"] == "eAuction",
        make_cud_key_udf(*key_cols_eauction)
    ).when(
        total_df["IncomingOriginSourceSystem"] == "Δελτίο Δικαστικών Δημοσιεύσεων",
        make_cud_key_udf(*key_cols_ddd)
    )
    ).cache()
        
    return total_df


unique_source_systems =  source_df.select("SourceSystem").distinct().collect()
unique_source_systems_list = [row['SourceSystem'] for row in unique_source_systems]

for i, s_system in enumerate(unique_source_systems_list):
    temp_df = construct_missing_found_df(source_df, s_system)
    #print(f"Records reporting for {s_system}, count is {temp_df.count()}")
    print(f"Records reporting for {s_system}")
    if i == 0:
        newsphone_level_df_noSourceREkey_df = temp_df
    else:
        newsphone_level_df_noSourceREkey_df = newsphone_level_df_noSourceREkey_df.union(temp_df)

# COMMAND ----------

# DBTITLE 1,OUTPUT data
# List of columns from DAuction360Matching_df that are missing in final_dataframe

DAuction360Matching_df_lite = DAuction360Matching_df

result_df_toProcess = result_df
#result_df_toProcess.cache()

extra_columns = set(result_df_toProcess.columns) - set(DAuction360Matching_df_lite.columns)
# fix additional cols
print(extra_columns)

# Add the missing columns to final_dataframe with null values
for column_name in extra_columns:
    if(column_name not in ["MatchingPercentage", "AuctionMatching360ID"]):
        result_df_toProcess = result_df_toProcess.drop(column_name)

result_df_toProcess = result_df_toProcess.withColumn("UIMatchingAuctionStatus",lit(""))
newsphone_level_df_noSourceREkey_df_toProcess = newsphone_level_df_noSourceREkey_df

newsphone_level_df_noSourceREkey_df_toProcess = newsphone_level_df_noSourceREkey_df_toProcess.withColumn("UIMatchingAuctionStatus",lit(""))

# Handle column differences in the fields of the non-matching dfs
extra_missing_source = set(newsphone_level_df_noSourceREkey_df_toProcess.columns) - set(DAuction360Matching_df_lite.columns)

for tcol in extra_missing_source:
    if(tcol not in ["MatchingPercentage", "AuctionMatching360ID"]):
        newsphone_level_df_noSourceREkey_df_toProcess = newsphone_level_df_noSourceREkey_df_toProcess.drop(tcol)

#Concatenate the dfs to the final result_df_toProcess
newsphone_level_df_noSourceREkey_df_toProcess = newsphone_level_df_noSourceREkey_df_toProcess.select(result_df_toProcess.columns)

result_df_toProcess = result_df_toProcess.union(newsphone_level_df_noSourceREkey_df_toProcess)

# List of columns from DAuction360Matching_df_lite that are missing in final_dataframe
extra_columns_from_source = set(DAuction360Matching_df_lite.columns) - set(result_df_toProcess.columns)
# fix additional cols

# Remove ay extra columns from CUD data DF DAuction360Matching_df_lite in order to subtract it from result_df_toProcess
for column_name in extra_columns_from_source:
    DAuction360Matching_df_lite = DAuction360Matching_df_lite.drop(column_name)
    print(f'Column name dropped is:{column_name}')


# COMMAND ----------

# DBTITLE 1,UIMatchingAuctionStatus set
# Get the column names in the desired order from DAuction360Matching_df_lite
column_order = DAuction360Matching_df_lite.columns
	  		  
# Reorder the columns of result_df
result_df_reordered = result_df_toProcess.select(column_order)
#result_df_reordered.cache()

#Add UIMatchingAuctionStatus column
result_df_reordered_without_guides = result_df_reordered.filter(col("SourceRealEstateKey").isNotNull() & (col("SourceRealEstateKey") != ""))
result_df_reordered_guides = result_df_reordered.filter(col("SourceRealEstateKey").isNull() | (length(trim(col("SourceRealEstateKey"))) == 0))

# Define a window specification to partition the data by the specified columns
window_spec = Window.partitionBy("IncomingAuctionKey", "IncomingRealEstateKey", "SourceSystem")

# Add an extra column "RowCount" to count the number of rows in each group
counted_df = result_df_reordered_without_guides.withColumn("RowCount", count("*").over(window_spec))

# Update the "UIMatchingAuctionStatus" column based on the conditions
result_df_reordered_without_guides = counted_df.withColumn(
    "UIMatchingAuctionStatus",
    when((col("UIMatchingAuctionStatus")=="") & ((col("RowCount") > 1) | ((col("RowCount") == 1) & (col("MatchingPercentage") < 1))), "toProcess")
    .otherwise(when((col("UIMatchingAuctionStatus")=="") & ((col("RowCount") == 1) & (col("MatchingPercentage") == 1)), "toValidate").otherwise(col("UIMatchingAuctionStatus")))
)

result_df_reordered_without_guides = result_df_reordered_without_guides.drop("RowCount")

result_df_reordered = result_df_reordered_without_guides.union(result_df_reordered_guides)

source_df.unpersist()
newsphone_df_full.unpersist()
result_df_reordered.cache()

# COMMAND ----------

# DBTITLE 1,Inheriting UI Information
new_records_df = result_df_reordered.subtract(DAuction360Matching_df.select(result_df_reordered.columns))

#TODO add the missing columns to new_records_df (from the DAuction360Matching_ui_df)
DAuction360Matching_ui_dt = DAuction360Matching_ui_df.select(
    col("AuctionMatching360ID").alias("Selected_AuctionMatching360ID"),
    col("UIMatchingAuctionStatus").alias("original_UIMatchingAuctionStatus"),
    "UIApprovalDate",
    "UIApprovalFlag",
    "UIAssignee", 
    "LastUpdatedBy", 
    "Comment"
)

#TODO update the new_records_df records from the DAuction360Matching_ui_df but only the UI columns
new_records_df = new_records_df.join(
    DAuction360Matching_ui_dt,
    new_records_df["AuctionMatching360ID"] == DAuction360Matching_ui_dt["Selected_AuctionMatching360ID"],
    "left_outer"
).drop("Selected_AuctionMatching360ID")

new_records_df = new_records_df.withColumn(
    "UIMatchingAuctionStatus",
    when(col("original_UIMatchingAuctionStatus").isNotNull(), col("original_UIMatchingAuctionStatus"))
    .otherwise(col("UIMatchingAuctionStatus"))
).drop("original_UIMatchingAuctionStatus")


### DEBUG SUBTRACT ###
#It changes some data types!!!!!

#Now do appropriate re-casting
new_records_df = new_records_df.withColumn("SourceRealEstateNetArea", col("SourceRealEstateNetArea").cast(DoubleType()))
new_records_df = new_records_df.withColumn("SourceRealEstateLandArea", col("SourceRealEstateLandArea").cast(DoubleType()))
new_records_df = new_records_df.withColumn("SourceOwnershipPercentage", col("SourceOwnershipPercentage").cast(DoubleType()))    
new_records_df = new_records_df.withColumn("MatchingPercentage", col("MatchingPercentage").cast(FloatType()))


# COMMAND ----------

# DBTITLE 1,DDD to eAuction handover
#Excluding Already Thesis Matched Records
new_records_df = new_records_df.join(
    ddd_to_eauction,
    on='AuctionMatching360ID',
    how='left_anti'
)
#Union with the inherited records so we write the data to the table
new_records_df = new_records_df.union(ddd_to_eauction.select(new_records_df.columns))

#Subtract so that we do not write the same recrods over and over
new_records_df = new_records_df.subtract(DAuction360Matching_df.select(new_records_df.columns))

# COMMAND ----------

# DBTITLE 1,ASK-4408

from pyspark.sql.functions import coalesce, every, lit, max

new_records_df = new_records_df.withColumn(
    "real_estate_key_split",
    coalesce(col("IncomingRealEstateKey").rlike(".*-[0-9]{3}$"), lit(False))
)

split_auctions_with_original_real_estate_key_df = (
    new_records_df
    .groupBy("IncomingAuctionKey")
    .agg((max("real_estate_key_split") & ~every("real_estate_key_split")).alias("incorrect_splitting"))
    .where("incorrect_splitting")
    .select("IncomingAuctionKey")
)

new_records_df = new_records_df.join(
    split_auctions_with_original_real_estate_key_df.alias("errors"),
    on=["IncomingAuctionKey"],
    how="left"
).filter(
    col("errors.IncomingAuctionKey").isNull() |
    (~col("IncomingRealEstateKey").rlike(".*-[0-9]{3}$"))
).drop("errors.IncomingAuctionKey", "real_estate_key_split")

# COMMAND ----------

# DBTITLE 1,END
new_records_df = new_records_df.distinct().withColumn("IngestionTimestamp", script_timestamp)

if export == 'true':
    # Stop the timer
    before_write_toS3_time = time.time()

    # Calculate the elapsed time
    elapsed_time = before_write_toS3_time - start_time

    print("Before Write to S3 time:", elapsed_time / 60, "minutes")
    
    # Write data to Unity Catalog table as a Delta table
    new_records_df.write.format("delta").mode("append").saveAsTable(f"{target_table}")

    # Stop the timer
    end_time = time.time()

    # Calculate the elapsed timeok?

    elapsed_time = end_time - start_time

    print("Elapsed total time:", elapsed_time / 60, "minutes")

