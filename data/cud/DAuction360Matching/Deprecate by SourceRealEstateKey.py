# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### Authentication cell

# COMMAND ----------

# DBTITLE 1,Connect to CDH
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

# DBTITLE 1,set the target table to edit
target_table = f"`{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching`"
deprecation_report_table = f"`{env_prefix}-cdh`.`derivativeactive`.`DAuction360Matching_deprecation_report`"
DAuction360Matching_df = spark.read.table(target_table)

# COMMAND ----------

# DBTITLE 1,Deprecation Query
deprecate_query = f"""
WITH depr_s AS (
    SELECT DISTINCT 
        C.sourcesystem AS SourceSystem,  
        CASE 
            WHEN C.sourcesystem = 'arotron' THEN LTRIM(RTRIM(C.realestatekey)) 
            ELSE LTRIM(RTRIM(C.RealEstateCepalKey)) 
        END AS SourceRealEstateKey
    FROM `{env_prefix}-cdh`.primary.realestatebuilding C 
    WHERE C.SourceSystem = 'thesis'
      AND (
          (COALESCE(RealEstateActive, 'Inactive') = 'Inactive' 
           AND RealEstateStatus IN ('DT enhancement', 'Πωληθέν', 'Replacement', 'Double', 'Split'))
          OR
          (COALESCE(CASE WHEN RealEstateActive = 'true' THEN NULL ELSE RealEstateActive END, 'Active') = 'Active'
           AND RealEstateStatus = 'REOco')
      )
      AND C.BusinessDate = (
          SELECT MAX(BusinessDate) 
          FROM `{env_prefix}-cdh`.primary.realestatebuilding
      )
),
current_records AS (
    SELECT a.*
    FROM {target_table} a
    JOIN (
        SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
        FROM {target_table}
        GROUP BY AuctionMatching360ID
    ) b 
    ON a.AuctionMatching360ID = b.AuctionMatching360ID 
       AND a.ingestiontimestamp = b.max_timestamp
       AND (a.UIMatchingAuctionStatus != 'Deprecated' OR a.UIMatchingAuctionStatus IS NULL)
)
SELECT * 
FROM current_records 
WHERE LTRIM(RTRIM(SourceRealEstateKey)) IN (
    SELECT SourceRealEstateKey 
    FROM depr_s
)
"""


deprecate_df = spark.sql(deprecate_query)
display(deprecate_df)
display(deprecate_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, date_format, lit

deprecate_df_report = deprecate_df.withColumnRenamed("UIMatchingAuctionStatus", "UIMatchingAuctionStatus_old"
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("BusinessDate", date_format(current_timestamp(), "yyyy-MM-dd")
                            ).withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system deprecation script")
                            ).withColumn("Comment", lit("not found in thesis perimeter"))


if not spark.catalog.tableExists(deprecation_report_table):
    deprecate_df_report.write.format("delta").mode("overwrite").saveAsTable(deprecation_report_table)
else:
    deprecate_df_report.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(deprecation_report_table)

spark.sql(f"REPAIR TABLE {deprecation_report_table} SYNC METADATA")

# COMMAND ----------

# DBTITLE 1,Marking SourceRealEstateKey deprecated data
deprecate_df = deprecate_df.withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system deprecation script")
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("Comment", lit("System"))

# COMMAND ----------

# DBTITLE 1,Vanishing records
vanished_query = f"""
WITH latest_realestate AS (
    SELECT distinct RealEstateCepalKey, SourceSystem
    FROM `{env_prefix}-cdh`.primary.realestatebuilding
    WHERE BusinessDate = (
        SELECT MAX(BusinessDate) 
        FROM `{env_prefix}-cdh`.primary.realestatebuilding
    )
)
SELECT t.*
FROM {target_table} t
LEFT ANTI JOIN latest_realestate l
ON t.SourceRealEstateKey = l.RealEstateCepalKey
AND t.SourceSystem = l.SourceSystem
WHERE t.SourceRealEstateKey IS NOT NULL
"""
vanished_df = spark.sql(vanished_query)
display(vanished_df)
display(vanished_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, date_format

vanished_df_report = vanished_df.withColumnRenamed("UIMatchingAuctionStatus", "UIMatchingAuctionStatus_old"
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("BusinessDate", date_format(current_timestamp(), "yyyy-MM-dd")
                            ).withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system deprecation script")
                            ).withColumn("Comment", lit("deleted from thesis"))

if not spark.catalog.tableExists(deprecation_report_table):
    vanished_df_report.write.format("delta").mode("overwrite").saveAsTable(deprecation_report_table)
else:
    vanished_df_report.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(deprecation_report_table)

spark.sql(f"REPAIR TABLE {deprecation_report_table} SYNC METADATA")

# COMMAND ----------

# DBTITLE 1,Marking vanishing records
from pyspark.sql.functions import lit, current_timestamp

vanished_df = vanished_df.withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system deprecation script")
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("Comment", lit("System"))

# COMMAND ----------

# DBTITLE 1,Deprecate DDD  AuctionKeys that have also exist in eAuction
deprecate_DDD_df = spark.sql(
    f"""WITH problem_records AS (
        WITH max_tmp AS (
            SELECT a.*
            FROM {target_table} a
            JOIN (
                SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
                FROM {target_table}
                GROUP BY AuctionMatching360ID
            ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
                AND a.ingestiontimestamp = b.max_timestamp
                AND ( a.UIMatchingAuctionStatus != 'Deprecated' OR a.UIMatchingAuctionStatus IS NULL )
        ),
        filtered_tmp AS (
            SELECT *,
                SUM(CASE WHEN IncomingOriginSourceSystem = 'Δελτίο Δικαστικών Δημοσιεύσεων' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY IncomingAuctionKey) AS count_deltio,
                SUM(CASE WHEN IncomingOriginSourceSystem = 'eAuction' THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY IncomingAuctionKey) AS count_eauction
            FROM max_tmp
        )
        SELECT *
        FROM filtered_tmp
        WHERE count_deltio > 0 AND count_eauction > 0
        )
        select * from problem_records 
        where problem_records.IncomingOriginSourceSystem = 'Δελτίο Δικαστικών Δημοσιεύσεων' """
)

display(deprecate_DDD_df)
display(deprecate_DDD_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, date_format

deprecate_DDD_df_report = deprecate_DDD_df.withColumnRenamed("UIMatchingAuctionStatus", "UIMatchingAuctionStatus_old"
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("BusinessDate", date_format(current_timestamp(), "yyyy-MM-dd")
                            ).withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system deprecation script")
                            ).withColumn("Comment", lit("DDD to eAuction"))


if not spark.catalog.tableExists(deprecation_report_table):
    deprecate_DDD_df_report.write.format("delta").mode("overwrite").saveAsTable(deprecation_report_table)
else:
    deprecate_DDD_df_report.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(deprecation_report_table)

spark.sql(f"REPAIR TABLE {deprecation_report_table} SYNC METADATA")

# COMMAND ----------

# DBTITLE 1,Mark DDD problem records as Deprecated
deprecate_DDD_df = deprecate_DDD_df.withColumn("UIMatchingAuctionStatus", lit("Deprecated")
                            ).withColumn("LastUpdatedBy", lit("system DDD deprecation script")
                            ).withColumn("IngestionTimestamp", current_timestamp()
                            ).withColumn("Comment", lit("System")).drop(
                                "count_deltio", "count_eauction"
                            )

# COMMAND ----------

print(target_table)

# COMMAND ----------

# DBTITLE 1,Write deprecated SourceRealEstateKey  data to table
deprecate_df_count = deprecate_df.count()

if deprecate_df_count > 0:
    print(f"Deprecating {deprecate_df_count} records")
    deprecate_df.write.format("delta").mode("append").saveAsTable(f"{target_table}")
    spark.sql(f"REPAIR TABLE {target_table} SYNC METADATA")
else:
    print("No records to deprecate")


# COMMAND ----------

# DBTITLE 1,Write deprecated DDD records
deprecate_DDD_df_count = deprecate_DDD_df.count()

if deprecate_DDD_df_count > 0:
    print(f"Deprecating {deprecate_DDD_df} DDD records")
    deprecate_DDD_df.write.format("delta").mode("append").saveAsTable(f"{target_table}")
    spark.sql(f"REPAIR TABLE {target_table} SYNC METADATA")
else:
    print("No DDD records to deprecate")

# COMMAND ----------

# DBTITLE 1,Write deprecated vanished records
vanished_df_count = vanished_df.count()

if vanished_df_count > 0:
    print(f"Deprecating {vanished_df_count} vanished records")
    vanished_df.write.format("delta").mode("append").saveAsTable(f"{target_table}")
    spark.sql(f"REPAIR TABLE {target_table} SYNC METADATA")
else:
    print("No vanished records to deprecate")
