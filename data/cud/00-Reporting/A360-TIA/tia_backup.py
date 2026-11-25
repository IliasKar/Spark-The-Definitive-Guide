# Databricks notebook source
# DBTITLE 1,Env Variables
from pyspark.sql.functions import collect_set, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs, create_map
from pyspark.sql import Window, Row                                                                                                                                                                                                                                                                                                             
from pyspark.sql.types import StringType, BooleanType, NullType, IntegerType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import traceback


# we have an issue with the broadcast values in cmd16
# spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

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
s3_output_path = connect_to_aws_and_access_ssm('/cfn/a360/reports_output_path')

# Specify the S3 bucket and folder
bucket_name = s3_output_path.split("/a360")[0].replace("s3://","")

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y%m%d")

folder_name = f"{s3_output_path}10-TIA/output/{curr_date}"

debug = False
emulatefe = False
reducerecordsA360 = False
emulatebusinessdate = False
 

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False



# COMMAND ----------

# DBTITLE 1,Query
backup_tia = spark.sql(
    f""" select * from 
    `{env_prefix}-cdh`.derivativeactive.tia_historical
    where BusinessDate = (
        select max(BusinessDate) from 
        `{env_prefix}-cdh`.derivativeactive.tia_historical
    ) """
).drop("BusinessDate")

# COMMAND ----------

# DBTITLE 1,Write data
backup_tia.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"`{env_prefix}-cdh-delivery`.tia.tia")
