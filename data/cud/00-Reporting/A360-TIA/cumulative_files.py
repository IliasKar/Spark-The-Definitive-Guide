# Databricks notebook source
# MAGIC %pip install fsspec
# MAGIC %pip install s3fs
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,env variables
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

# DBTITLE 1,Read to pandas from S3
import pandas as pd
from io import BytesIO
import urllib

def read_s3_csv_with_boto3(s3_path, s3_client=S3_client):
    # Parse the bucket and key
    parsed = urllib.parse.urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read()

    # Convert to DataFrame
    df = pd.read_csv(BytesIO(body))
    return df


# COMMAND ----------

# DBTITLE 1,Most recent Cumulative files from newsphone
BasicData_20250520_path = 's3://p-ec1-cepal-landing-zone/News_Phone_Migration/newsphone/BasicData_InitialFileUpTo20250520.csv'
auctions_20250522_path = 's3://p-ec1-cepal-landing-zone/News_Phone_Migration/newsphone/auctions_InitialFileUpTO20250522.csv'

BasicData_20250520_df = read_s3_csv_with_boto3(BasicData_20250520_path)

BasicData_20250520_df["file_name"] = 'BasicData_InitialFileUpTo20250520.csv'
BasicData_20250520_df["file_date"] = '2025-05-20'

auctions_20250522_df = read_s3_csv_with_boto3(auctions_20250522_path)
auctions_20250522_df["file_name"] = 'auctions_InitialFileUpTO20250522.csv'
auctions_20250522_df["file_date"] = '2025-05-22'

# COMMAND ----------

# DBTITLE 1,Building the dataframes
from datetime import datetime
import re
import pandas as pd

# Separate min/max dates for auction and basicdata files
auction_min_date = datetime.strptime("2025-05-20", "%Y-%m-%d").date()
auction_max_date = datetime.today().date()

basicdata_min_date = datetime.strptime("2025-05-22", "%Y-%m-%d").date()
basicdata_max_date = datetime.today().date()

# Directory path
all_files = dbutils.fs.ls("s3://p-ec1-cepal-landing-zone/News_Phone_Migration/newsphone/")

# Regex patterns
auction_pattern = re.compile(r'^auctions_(\d{8})\.csv$', re.IGNORECASE)
basicdata_pattern = re.compile(r'^BasicData_(\d{8})\.csv$', re.IGNORECASE)

# Collect and process files
auctions_dfs = [auctions_20250522_df]
basicdata_dfs = [BasicData_20250520_df]

for file_object in all_files:
    full_path = file_object.path
    file_name = file_object.name

    if file_object.size == 0:
        continue

    auction_match = auction_pattern.match(file_name)
    basicdata_match = basicdata_pattern.match(file_name)

    if auction_match:
        file_date = datetime.strptime(auction_match.group(1), "%Y%m%d").date()
        if file_date < auction_min_date or file_date > auction_max_date:
            continue
        try:
            df = read_s3_csv_with_boto3(full_path)
            if "GroupID" in df.columns.to_list():
                print(f"Skipping file {file_name} with GroupID column")
            else:
                df["file_date"] = file_date
                df["file_name"] = file_name
                auctions_dfs.append(df)
        except Exception as e:
            print(f"Error processing file {file_name}: {str(e)}")

    elif basicdata_match:
        file_date = datetime.strptime(basicdata_match.group(1), "%Y%m%d").date()
        if file_date < basicdata_min_date or file_date > basicdata_max_date:
            continue
        try:
            df = read_s3_csv_with_boto3(full_path)
            df["file_date"] = file_date
            df["file_name"] = file_name
            basicdata_dfs.append(df)
        except Exception as e:
            print(f"Error processing file {file_name}: {str(e)}")

# Concatenate all rows
df_auctions = pd.concat(auctions_dfs, ignore_index=True) if auctions_dfs else pd.DataFrame()
df_basicdata = pd.concat(basicdata_dfs, ignore_index=True) if basicdata_dfs else pd.DataFrame()

# Ensure file_date is in correct datetime format
df_auctions["file_date"] = pd.to_datetime(df_auctions["file_date"], errors='coerce')
df_basicdata["file_date"] = pd.to_datetime(df_basicdata["file_date"], errors='coerce')

# Filter for latest file per key
df_auctions_latest = (
    df_auctions.loc[df_auctions.groupby("AuctionObJID")["file_date"].idxmax()].reset_index(drop=True)
    if not df_auctions.empty and "AuctionObJID" in df_auctions.columns
    else pd.DataFrame()
)

if not df_auctions_latest.empty and "Active" in df_auctions_latest.columns:
    df_auctions_latest = df_auctions_latest[df_auctions_latest["Active"] == '"1"']

df_basicdata_latest = (
    df_basicdata.loc[df_basicdata.groupby("UniqueCode")["file_date"].idxmax()].reset_index(drop=True)
    if not df_basicdata.empty and "UniqueCode" in df_basicdata.columns
    else pd.DataFrame()
)

if not df_basicdata_latest.empty and "Active" in df_basicdata_latest.columns:
    df_basicdata_latest = df_basicdata_latest[df_basicdata_latest["Active"] == '"1"']

# Output shapes
print("All Auctions rows:", df_auctions.shape)
print("Latest Auctions per AuctionObjID:", df_auctions_latest.shape)
print("All BasicData rows:", df_basicdata.shape)
print("Latest BasicData per UniqueCode:", df_basicdata_latest.shape)


# COMMAND ----------

# DBTITLE 1,Delete files
import os
import shutil

# Directory path
directory_path = f"/Volumes/{env_prefix}-cdh/reference/tia/cumulative_files/"

# Delete all files in the directory
for filename in os.listdir(directory_path):
    file_path = os.path.join(directory_path, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except Exception as e:
        print(f"Failed to delete {file_path}. Reason: {e}")

# COMMAND ----------

# DBTITLE 1,Export files(to be ingested by our pipeline)
# build the list of columns to keep
auctions_cols_to_export = [
    col for col in df_auctions_latest.columns 
    if col not in ("file_name", "file_date")
]

# 1. Compute a clean date string
max_date = df_auctions['file_date'].max().strftime('%Y%m%d')

# 2. Build your file_object
file_object = f"cumulative_auctions_{max_date}.csv"

# 3. Export only the desired columns
df_auctions_latest[auctions_cols_to_export].to_csv(
    f"/Volumes/{env_prefix}-cdh/reference/tia/cumulative_files/{file_object}", index=False)

# build the list of columns to keep
basicdata_cols_to_export = [
    col for col in df_basicdata_latest.columns 
    if col not in ("file_name", "file_date")
]

# 1. Compute a clean date string
max_date = df_basicdata_latest['file_date'].max().strftime('%Y%m%d')

# 2. Build your file_object
file_object = f"cumulative_BasicData_{max_date}.csv"

# 3. Export only the desired columns
df_basicdata_latest[basicdata_cols_to_export].to_csv(
    f"/Volumes/{env_prefix}-cdh/reference/tia/cumulative_files/{file_object}", index=False)

