# Databricks notebook source
# DBTITLE 1,Get environment and redis link
import boto3
from botocore.exceptions import NoCredentialsError
import traceback
import os

from pyspark.sql.functions import col, when, lit, month, year, concat, left, expr

from typing import Any
 
from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    col, when, lit, substring, concat_ws, coalesce, length,max,concat,format_string,expr,regexp_replace,cast, date_format
)

from typing import Optional
 
import delta  # type: ignore
from pyspark.sql import SparkSession
 
from io import BytesIO
import pandas as pd
import json
import sys
 
from datetime import datetime
from pytz import timezone
 
 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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
    print("mode is shared")
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
redis_link = connect_to_aws_and_access_ssm('/cfn/redis/primary/address')
print(f"env_prefix : {env_prefix}")
print(f"redis_link : {redis_link}")

# COMMAND ----------

# MAGIC %pip install redis
# MAGIC import redis 
# MAGIC r = redis.Redis(host=f"{redis_link}")
# MAGIC print(r)

# COMMAND ----------

# DBTITLE 1,Import from redis to DB
import urllib.parse

data = []
for key in r.scan_iter("reports_orchestrator:reports:*payment*"):
    decoded_key = urllib.parse.unquote(key.decode('utf-8'))
    json_data = r.json().get(decoded_key)
    data.append((decoded_key, str(json_data)))
    print(decoded_key)

df = spark.createDataFrame(data, ["redis_key", "redis_json"])
display(df)
df.createOrReplaceTempView("temp_redis_data")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{env_prefix}-cdh`.redis.reports (redis_key STRING, report_name STRING, redis_deployed INT, redis_json STRING);
""")

existing_keys = spark.sql(f"""
    WITH existing_keys AS (
        SELECT redis_key FROM `{env_prefix}-cdh`.redis.reports
    )
    SELECT 
        redis_key
    FROM temp_redis_data
    WHERE redis_key IN (SELECT redis_key FROM existing_keys);
    """)

if existing_keys.count() > 0:
    raise ValueError("Redis_key already exist found in `qa-cdh`.redis.reports")

spark.sql(f"""
    INSERT INTO `{env_prefix}-cdh`.redis.reports (redis_key, report_name, isdeployed, redis_json)
    SELECT redis_key,
    SUBSTRING(
      redis_json,
      CHARINDEX('report_name', redis_json) + 15,
      CHARINDEX('report_job_name', redis_json) - (CHARINDEX('report_name', redis_json) + 19)),
    1,
    redis_json 
    FROM temp_redis_data
""")
