# Databricks notebook source
from pyspark.sql.functions import collect_set,explode_outer, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs
from pyspark.sql import Window,Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, NullType, IntegerType, ArrayType, LongType, TimestampType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import traceback
import requests
import json
import random


# we have an issue with the broadcast values in cmd16
spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env_prefix = os.environ["S3PREFIX"] (not working always.. deprecated. Moved to SSM)

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y-%m-%d")


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
    
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

# COMMAND ----------


# Import the required module
import os

# Define the file path
file_path = '/dbfs/tmp/offset.txt'

# Check if the file exists
if os.path.exists(file_path):
    # Delete the file
    os.remove(file_path)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import StructType, StructField, StringType
import concurrent.futures

# Define schema for the DataFrame
schema = StructType([
    StructField("VAT_Number", StringType())
])

# Load VAT numbers from your query
vat_query = f"""
select distinct trim(afm) as VAT_Number from `{env_prefix}-de-sandbox`.gemi_aristea.gemi_kad kd
where trim(kd.afm) <> '000000000'
"""

print(vat_query)

vat_numbers_df = spark.sql(vat_query)

print(vat_numbers_df)

# Read offset from file
try:
    with open("/dbfs/tmp/offset.txt", "r") as f:
        offset = int(f.read())        
except FileNotFoundError:
    offset = 0

print("offset:",offset)

# Define function to process VAT numbers
def process_vat_number(vat_number, gemi_number):
    # Run notebook for each VAT number
    notebook_path = "./GEMH-VAT-rest_api"
    dbutils.notebook.run(notebook_path, 3600, {"VAT": f"{vat_number}", "clean_data": "false", "export": "true", "retention_d":"100"})

# Process VAT numbers in parallel using ThreadPoolExecutor
batch_size = 100
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)  # Adjust max_workers as needed

while offset < vat_numbers_df.count():
    # Get next batch of VAT numbers
    batch_df = vat_numbers_df.select("VAT_Number").orderBy("VAT_Number").na.drop().withColumn("row_num", row_number().over(Window.orderBy("VAT_Number")))

    print(batch_df)

    # Filter the batch DataFrame based on the offset and batch size
    batch_df = batch_df.filter((col("row_num") > offset) & (col("row_num") <= offset + batch_size)).drop("row_num")

    print(batch_df)

    # Submit tasks for processing VAT numbers in parallel
    futures = []
    for vat_number_row in batch_df.collect():
        vat_number = vat_number_row["VAT_Number"]
        print("vat_number:",vat_number)
        futures.append(executor.submit(process_vat_number, vat_number, None))

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)

    # Update offset
    offset += batch_size

    # Write offset to a file (to be used for restoring the run)
    with open("/dbfs/tmp/offset.txt", "w") as f:
        f.write(str(offset))
        print("offset written:",offset)

