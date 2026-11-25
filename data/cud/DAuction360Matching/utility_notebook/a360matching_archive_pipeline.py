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

'''
truncate table `qa-de-sandbox`.a360.dauction360matching_freshdata;
truncate table `qa-cdh`.derivativearchive.dauction360matching;


drop table `qa-cdh`.derivativeactive.dauction360matching;
create table `qa-cdh`.derivativeactive.dauction360matching as select * from `p-cdh`.derivativeactive.dauction360matching;
select count(*) from `qa-cdh`.derivativeactive.dauction360matching



RUN THIS ON SQL EDITOR FOR THE DEPLOYMENT


%sql
create table `qa-cdh`.derivativearchive.dauction360matching as select * from `qa-cdh`.derivativeactive.dauction360matching limit 1;
create table `qa-de-sandbox`.a360.dauction360matching_freshdata as select * from `qa-cdh`.derivativeactive.dauction360matching limit 1;
truncate table `qa-de-sandbox`.a360.dauction360matching_freshdata;
truncate table `qa-cdh`.derivativearchive.dauction360matching;
repair table `qa-de-sandbox`.a360.dauction360matching_freshdata sync metadata;
repair table `qa-cdh`.derivativearchive.dauction360matching sync metadata;


'''



# COMMAND ----------

# append all records to the archive table - match by col name instead of select * 
spark.sql(f"""
INSERT INTO `{env_prefix}-cdh`.derivativearchive.dauction360matching (
  AuctionMatching360ID,
  IncomingCalculatedAuctionObjKey,
  IncomingTNMergeCode,
  IncomingKAEK,
  IncomingOriginSourceSystem,
  IncomingAuctionObjKey,
  IncomingAuctionKey,
  IncomingOriginAuctionKey,
  IncomingRealEstateKey,
  IncomingRealEstateGroupCode,
  IncomingOwnerTaxRegistrationNumber,
  IncomingOriginOwnerTaxRegistrationNumber,
  IncomingOwnerDescription,
  IncomingRealEstatePostalCode,
  IncomingAuctionStatus,
  IncomingRealEstateAddress,
  IncomingRealEstateSurfaceSqm,
  IncomingRealEstateFloorRawValue,
  IncomingRealEstateTitle,
  IncomingRealEstateCategory,
  IncomingRealEstateSubCategory,
  IncomingRealEstateMunicipality,
  IncomingRealEstateArea,
  IncomingRealEstateTags,
  IncomingRealEstateTagValues,
  IncomingSeizureReportNumber,
  IncomingHastenerDescr,
  IncomingInitiator,
  IncomingHastener,
  IncomingInitiatorCepalAB,
  IncomingPortfolioCategory,
  IncomingIsHastenerServicer,
  IncomingAuctionDate,
  IncomingisLastAuction,
  IncomingOwnerTaxRegistrationNumberIsCustomer,
  IncomingRealEstateGroupCodeStatus,
  SourceSystem,
  SourceOwnerTaxRegistrationNumber,
  SourceOwnerFirstName,
  SourceOwnerLastName,
  SourceRealEstateKey,
  SourceRealEstateAddress1,
  SourceRealEstateAddress2,
  SourceRealEstateSubCategoryCode,
  SourceRealEstateCategoryCode,
  SourceRealEstateStatus,
  SourceRealEstateCity,
  SourceRealEstateDescription,
  SourceKAEK,
  SourceRealEstateLandArea,
  SourceRealEstateNetArea,
  SourceRealEstateMunicipality,
  SourceRealEstateActive,
  SourceRealEstateUsageCodeType,
  SourceRealEstatePostalCode,
  SourceRealEstateBuildingFloorRawValue,
  SourceRealEstateBuildingOtherAuxiliaryArea,
  SourceRealEstateBuildingStorageArea,
  SourceRealEstateBuildingParkingArea,
  SourceRealEstateBuildingNetArea,
  SourceRealEstateBuildingNetArea_sum,
  SourceAggregatedRealEstateBuildingFloorRawValue,
  SourceRealEstateBuildingNumber,
  SourceOwnershipPercentage,
  SourceOwnershipTypeCode,
  MatchingPercentage,
  MatchingAlgorithmVersion,
  MatchingAlgorithmVersionDate,
  MatchingMissingFlag,
  IngestionTimestamp,
  UIMatchingAuctionStatus,
  UIAssignee,
  UIApprovalFlag,
  UIApprovalDate,
  LastUpdatedBy,
  LastUpdatedDate,
  Comment,
  SourceRealEstateBuildingFloorRawValueDescription
)
SELECT
  AuctionMatching360ID,
  IncomingCalculatedAuctionObjKey,
  IncomingTNMergeCode,
  IncomingKAEK,
  IncomingOriginSourceSystem,
  IncomingAuctionObjKey,
  IncomingAuctionKey,
  IncomingOriginAuctionKey,
  IncomingRealEstateKey,
  IncomingRealEstateGroupCode,
  IncomingOwnerTaxRegistrationNumber,
  IncomingOriginOwnerTaxRegistrationNumber,
  IncomingOwnerDescription,
  IncomingRealEstatePostalCode,
  IncomingAuctionStatus,
  IncomingRealEstateAddress,
  IncomingRealEstateSurfaceSqm,
  IncomingRealEstateFloorRawValue,
  IncomingRealEstateTitle,
  IncomingRealEstateCategory,
  IncomingRealEstateSubCategory,
  IncomingRealEstateMunicipality,
  IncomingRealEstateArea,
  IncomingRealEstateTags,
  IncomingRealEstateTagValues,
  IncomingSeizureReportNumber,
  IncomingHastenerDescr,
  IncomingInitiator,
  IncomingHastener,
  IncomingInitiatorCepalAB,
  IncomingPortfolioCategory,
  IncomingIsHastenerServicer,
  IncomingAuctionDate,
  IncomingisLastAuction,
  IncomingOwnerTaxRegistrationNumberIsCustomer,
  IncomingRealEstateGroupCodeStatus,
  SourceSystem,
  SourceOwnerTaxRegistrationNumber,
  SourceOwnerFirstName,
  SourceOwnerLastName,
  SourceRealEstateKey,
  SourceRealEstateAddress1,
  SourceRealEstateAddress2,
  SourceRealEstateSubCategoryCode,
  SourceRealEstateCategoryCode,
  SourceRealEstateStatus,
  SourceRealEstateCity,
  SourceRealEstateDescription,
  SourceKAEK,
  SourceRealEstateLandArea,
  SourceRealEstateNetArea,
  SourceRealEstateMunicipality,
  SourceRealEstateActive,
  SourceRealEstateUsageCodeType,
  SourceRealEstatePostalCode,
  SourceRealEstateBuildingFloorRawValue,
  SourceRealEstateBuildingOtherAuxiliaryArea,
  SourceRealEstateBuildingStorageArea,
  SourceRealEstateBuildingParkingArea,
  SourceRealEstateBuildingNetArea,
  SourceRealEstateBuildingNetArea_sum,
  SourceAggregatedRealEstateBuildingFloorRawValue,
  SourceRealEstateBuildingNumber,
  SourceOwnershipPercentage,
  SourceOwnershipTypeCode,
  MatchingPercentage,
  MatchingAlgorithmVersion,
  MatchingAlgorithmVersionDate,
  MatchingMissingFlag,
  IngestionTimestamp,
  UIMatchingAuctionStatus,
  UIAssignee,
  UIApprovalFlag,
  UIApprovalDate,
  LastUpdatedBy,
  LastUpdatedDate,
  Comment,
  SourceRealEstateBuildingFloorRawValueDescription
FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching
""")

# COMMAND ----------

spark.sql(f"""truncate table `{env_prefix}-de-sandbox`.a360.dauction360matching_freshdata;""")

spark.sql(f"""insert into `{env_prefix}-de-sandbox`.a360.dauction360matching_freshdata
SELECT a.*
FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching a
JOIN (
  SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
  FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching
  GROUP BY AuctionMatching360ID
) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
    AND a.ingestiontimestamp = b.max_timestamp;""")

spark.sql(f"""truncate table `{env_prefix}-cdh`.derivativeactive.dauction360matching;""")

spark.sql(f"""insert into `{env_prefix}-cdh`.derivativeactive.dauction360matching 
          select * from `{env_prefix}-de-sandbox`.a360.dauction360matching_freshdata;""")

# COMMAND ----------

spark.sql(f"""
DELETE FROM `{env_prefix}-cdh`.derivativeactive.dauction360matching
WHERE UIMatchingAuctionStatus = 'Deprecated'
          """)
