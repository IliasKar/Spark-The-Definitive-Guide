# Databricks notebook source
# DBTITLE 1,Environment Variables
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
    print("shared")
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

try:
    IncomingRealEstateGroupCode_filter = dbutils.widgets.get("IncomingRealEstateGroupCode_filter")
except Exception as e:
    IncomingRealEstateGroupCode_filter = "all" 

try:
    target_table = dbutils.widgets.get("target_table")
except Exception as e:
    target_table = f"`{env_prefix}-cdh`.derivativeactive.dauction360matching" 

# COMMAND ----------

IncomingRealEstateGroupCode_filter_query = ''
IncomingRealEstateGroupCode_filter_noalias_query = ''

if IncomingRealEstateGroupCode_filter != "all":
    # Select columns for final output
    IncomingRealEstateGroupCode_filter_query = f""" AND a.IncomingRealEstateGroupCode = '{IncomingRealEstateGroupCode_filter}'"""
    IncomingRealEstateGroupCode_filter_noalias_query = f""" AND IncomingRealEstateGroupCode = '{IncomingRealEstateGroupCode_filter}'"""

sql_query = f"""WITH max_tmp AS (
            SELECT
                AuctionMatching360ID,
                MAX(ingestiontimestamp) AS max_timestamp
            FROM
            {target_table}
            WHERE
                IncomingRealEstateGroupCode IS NOT NULL                
                {IncomingRealEstateGroupCode_filter_noalias_query}
            GROUP BY
                AuctionMatching360ID
        ),
        guides AS (
            SELECT
                a.*
            FROM
                {target_table} a
            JOIN
                max_tmp b
            ON
                a.AuctionMatching360ID = b.AuctionMatching360ID
                AND a.ingestiontimestamp = b.max_timestamp
            WHERE
                a.IncomingRealEstateGroupCode IS NOT NULL
                AND a.UIMatchingAuctionStatus = 'locked'
                {IncomingRealEstateGroupCode_filter_query}
        ),
        all_records AS (    
            SELECT
                a.*
            FROM
                {target_table} a
            JOIN
                max_tmp b
            ON
                a.AuctionMatching360ID = b.AuctionMatching360ID
                AND a.ingestiontimestamp = b.max_timestamp
            WHERE
                a.SourceRealEstateKey IS NOT NULL
                AND a.SourceRealEstateKey <> ''
                AND a.IncomingRealEstateGroupCode IS NOT NULL
                and (a.UIMatchingAuctionStatus != 'Deprecated' or a.UIMatchingAuctionStatus is null)
        ),
        pc AS (
            SELECT
                all_records.*, guides.AuctionMatching360ID as AuctionMatching360ID_g
            FROM
                all_records
            JOIN
                guides
            ON
                guides.IncomingRealEstateKey = all_records.IncomingRealEstateKey
                AND guides.IncomingOwnerTaxRegistrationNumber = all_records.IncomingOwnerTaxRegistrationNumber
            WHERE
                all_records.AuctionMatching360ID = CONCAT(guides.AuctionMatching360ID, all_records.SourceRealEstateKey)
        ),
        all_cands AS (    
            SELECT
                a.*
            FROM
                {target_table} a
            JOIN
                max_tmp b
            ON
                a.AuctionMatching360ID = b.AuctionMatching360ID
                AND a.ingestiontimestamp = b.max_timestamp
            WHERE
                a.SourceRealEstateKey IS NOT NULL
                AND a.SourceRealEstateKey <> ''
                AND a.IncomingRealEstateGroupCode IS NOT NULL
                AND a.UIMatchingAuctionStatus != 'Deprecated'
                AND a.AuctionMatching360ID not in (select distinct CONCAT(pc.AuctionMatching360ID_g, pc.SourceRealEstateKey) from pc )
                AND a.IncomingRealEstateGroupCode  in (
                SELECT DISTINCT guides.IncomingRealEstateGroupCode from guides)

        ), 
        candidates as (
        SELECT
            oc.AuctionMatching360ID,oc.IncomingCalculatedAuctionObjKey,
            oc.IncomingTNMergeCode,oc.IncomingKAEK,
            oc.IncomingOriginSourceSystem,oc.IncomingAuctionObjKey,
            oc.IncomingAuctionKey,oc.IncomingOriginAuctionKey,
            oc.IncomingRealEstateKey,oc.IncomingRealEstateGroupCode,
            oc.IncomingOwnerTaxRegistrationNumber,oc.IncomingOriginOwnerTaxRegistrationNumber,
            oc.IncomingOwnerDescription,oc.IncomingRealEstatePostalCode,
            oc.IncomingAuctionStatus,oc.IncomingRealEstateAddress,
            oc.IncomingRealEstateSurfaceSqm,oc.IncomingRealEstateFloorRawValue,
            oc.IncomingRealEstateTitle,oc.IncomingRealEstateCategory,
            oc.IncomingRealEstateSubCategory,oc.IncomingRealEstateMunicipality,
            oc.IncomingRealEstateArea,oc.IncomingRealEstateTags,
            oc.IncomingRealEstateTagValues,oc.IncomingSeizureReportNumber,
            oc.IncomingHastenerDescr,oc.IncomingInitiator,
            oc.IncomingHastener,oc.IncomingInitiatorCepalAB,
            oc.IncomingPortfolioCategory,oc.IncomingIsHastenerServicer,
            oc.IncomingAuctionDate,oc.IncomingisLastAuction,
            oc.IncomingOwnerTaxRegistrationNumberIsCustomer,oc.IncomingRealEstateGroupCodeStatus,
            oc.SourceSystem,oc.SourceOwnerTaxRegistrationNumber,
            oc.SourceOwnerFirstName,oc.SourceOwnerLastName,
            oc.SourceRealEstateKey,oc.SourceRealEstateAddress1,
            oc.SourceRealEstateAddress2,oc.SourceRealEstateSubCategoryCode,
            oc.SourceRealEstateCategoryCode,oc.SourceRealEstateStatus,
            oc.SourceRealEstateCity,oc.SourceRealEstateDescription,
            oc.SourceKAEK,oc.SourceRealEstateLandArea,oc.SourceRealEstateNetArea,
            oc.SourceRealEstateMunicipality,oc.SourceRealEstateActive,
            oc.SourceRealEstateUsageCodeType,oc.SourceRealEstatePostalCode,
            oc.SourceRealEstateBuildingFloorRawValue,
            oc.SourceRealEstateBuildingOtherAuxiliaryArea, oc.SourceRealEstateBuildingStorageArea, 
            oc.SourceRealEstateBuildingParkingArea,
            oc.SourceRealEstateBuildingNetArea, oc.SourceRealEstateBuildingNetArea_sum, 
            oc.SourceAggregatedRealEstateBuildingFloorRawValue,
            oc.SourceRealEstateBuildingNumber,
            oc.SourceOwnershipPercentage,oc.SourceOwnershipTypeCode,
            oc.MatchingPercentage,oc.MatchingAlgorithmVersion,
            oc.MatchingAlgorithmVersionDate,oc.MatchingMissingFlag,
            current_timestamp() as IngestionTimestamp,
            pc.UIMatchingAuctionStatus,
            oc.SourceRealEstateBuildingFloorRawValueDescription,
            pc.UIAssignee,
            pc.UIApprovalFlag,
            pc.UIApprovalDate,
            'A360user (system inheritance)' as LastUpdatedBy, 
            pc.LastUpdatedDate,
            pc.Comment
        FROM
            all_cands oc
        LEFT JOIN 
            pc
        ON
            pc.SourceSystem = oc.SourceSystem
            AND oc.IncomingRealEstateGroupCode = pc.IncomingRealEstateGroupCode
            AND pc.SourceRealEstateKey = oc.SourceRealEstateKey

        WHERE
            pc.UIMatchingAuctionStatus is NOT NULL 
            and oc.IncomingRealEstateGroupCode in (
                SELECT DISTINCT guides.IncomingRealEstateGroupCode from guides
            )
        ), 
            guides_formatted as(
            select    AuctionMatching360ID, IncomingCalculatedAuctionObjKey,
            IncomingTNMergeCode,IncomingKAEK,
            IncomingOriginSourceSystem,IncomingAuctionObjKey,
            IncomingAuctionKey,IncomingOriginAuctionKey,
            IncomingRealEstateKey,IncomingRealEstateGroupCode,
            IncomingOwnerTaxRegistrationNumber,IncomingOriginOwnerTaxRegistrationNumber,
            IncomingOwnerDescription,IncomingRealEstatePostalCode,
            IncomingAuctionStatus,IncomingRealEstateAddress,
            IncomingRealEstateSurfaceSqm,IncomingRealEstateFloorRawValue,
            IncomingRealEstateTitle,IncomingRealEstateCategory,
            IncomingRealEstateSubCategory,IncomingRealEstateMunicipality,
            IncomingRealEstateArea,IncomingRealEstateTags,
            IncomingRealEstateTagValues,IncomingSeizureReportNumber,
            IncomingHastenerDescr,IncomingInitiator,
            IncomingHastener,IncomingInitiatorCepalAB,
            IncomingPortfolioCategory,IncomingIsHastenerServicer,
            IncomingAuctionDate,IncomingisLastAuction,
            IncomingOwnerTaxRegistrationNumberIsCustomer,IncomingRealEstateGroupCodeStatus,
            SourceSystem,SourceOwnerTaxRegistrationNumber,
            SourceOwnerFirstName,SourceOwnerLastName,
            SourceRealEstateKey,SourceRealEstateAddress1,
            SourceRealEstateAddress2,SourceRealEstateSubCategoryCode,
            SourceRealEstateCategoryCode,SourceRealEstateStatus,
            SourceRealEstateCity,SourceRealEstateDescription,
            SourceKAEK,SourceRealEstateLandArea,SourceRealEstateNetArea,
            SourceRealEstateMunicipality,SourceRealEstateActive,
            SourceRealEstateUsageCodeType,SourceRealEstatePostalCode,
            SourceRealEstateBuildingFloorRawValue, 
            SourceRealEstateBuildingOtherAuxiliaryArea, SourceRealEstateBuildingStorageArea, 
            SourceRealEstateBuildingParkingArea,
            SourceRealEstateBuildingNetArea, SourceRealEstateBuildingNetArea_sum, 
            SourceAggregatedRealEstateBuildingFloorRawValue,
            SourceRealEstateBuildingNumber,
            SourceOwnershipPercentage,SourceOwnershipTypeCode,
            MatchingPercentage,MatchingAlgorithmVersion,
            MatchingAlgorithmVersionDate,MatchingMissingFlag,
            current_timestamp() as IngestionTimestamp,
            '' as UIMatchingAuctionStatus,
            SourceRealEstateBuildingFloorRawValueDescription,
            UIAssignee,
            UIApprovalFlag,
            UIApprovalDate,
            'system' as LastUpdatedBy, 
            LastUpdatedDate,
            Comment   from guides 
            ),
        final as (
            select * from candidates 

            union 

            select * from guides_formatted

                )
            INSERT INTO  {target_table}          
            SELECT * FROM final;
"""

'''
UPDATE  {target_table} a
set a.UIMatchingAuctionStatus = 'locked'
where
a.ingestiontimestamp = (select max(ingestiontimestamp) from {target_table} WHERE  AuctionMatching360ID =  'JrwIToRomn|456003|JrwIToRomn-004|068232718|thesis|')
and  a.AuctionMatching360ID =  'JrwIToRomn|456003|JrwIToRomn-004|068232718|thesis|'
'''



# COMMAND ----------

out_df = spark.sql(sql_query)

# COMMAND ----------

display(out_df)
