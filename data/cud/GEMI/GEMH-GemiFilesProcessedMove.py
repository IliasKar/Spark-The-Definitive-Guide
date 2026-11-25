# Databricks notebook source
# MAGIC %md
# MAGIC ## Initial authorization cell

# COMMAND ----------

from pyspark.sql.functions import collect_set,explode_outer, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs
from pyspark.sql import Window,Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, NullType, IntegerType, ArrayType, LongType, TimestampType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import random


#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env_prefix = os.environ["S3PREFIX"] (not working always.. deprecated. Moved to SSM)

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y-%m-%d")


# Connect to AWS using boto3
ssm_client = boto3.client('ssm', region_name='eu-central-1')

s3_client = boto3.client('s3', region_name='eu-central-1')                           

# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

    
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move files to processed

# COMMAND ----------

import os
import shutil
import pandas as pd

# Get the json files source directory path
source_directory = dbutils.widgets.get("json_data_path")  

# After parsing the json file move the file to the processed destination directory path
processed_destination_directory = dbutils.widgets.get("json_processed_path")  

# Get the minimum reference date based on which we are going to decide if the geminumber will be refreshed or not. If the gemi number is already stored with reference date > min_reference_date then it will be skipped.
try:
    min_reference_date = dbutils.widgets.get("min_reference_date")
except Exception as e:
    min_reference_date = "" 

# Get list of files in the source directory
files_to_move = os.listdir(source_directory)

# Convert files_to_move to a DataFrame
files_df = pd.DataFrame(files_to_move, columns=['filename'])

# Registered geminumbers query returning filenames
registered_gemi_numbers_query = """
    SELECT DISTINCT CONCAT(geminumber, '.json') AS filename 
    FROM `gemi`.default.gemi_company_info 
"""

if min_reference_date != "":
    registered_gemi_numbers_query += f" WHERE referencedate >= '{min_reference_date}'"

print(registered_gemi_numbers_query)

# Get the distinct filenames (geminumber.json)
already_registered_gemi_numbers_df = spark.sql(registered_gemi_numbers_query).cache()

# Convert Spark DataFrame to Pandas DataFrame for filtering
already_registered_gemi_numbers_pd_df = already_registered_gemi_numbers_df.toPandas()

display(already_registered_gemi_numbers_pd_df)

# Filter files_df to keep only the filenames present in already_registered_gemi_numbers_pd_df
filtered_files_df = files_df[files_df['filename'].isin(already_registered_gemi_numbers_pd_df['filename'])]

display(filtered_files_df)


# Iterate over the filtered files and move them
for filename in filtered_files_df['filename']:
    source_file_path = os.path.join(source_directory, filename)
    destination_file_path = os.path.join(processed_destination_directory, filename)
    
    # Check if the file exists in the source directory
    if os.path.exists(source_file_path):
        # Move the file to the destination directory
        shutil.move(source_file_path, destination_file_path)
        print(f"Moved {filename} to {destination_file_path}")
    else:
        print(f"File {filename} not found in {source_directory}")

# COMMAND ----------

# Define the tables and execute repair statements
'''
for table in tables:
    spark.sql("REPAIR TABLE `gemi`.`default`.`{table}` SYNC METADATA".format(env_prefix=env_prefix, table=table))
'''
