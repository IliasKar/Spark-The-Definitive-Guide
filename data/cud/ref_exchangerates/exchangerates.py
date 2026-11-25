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

from datetime import datetime

dbutils.widgets.text('datefrom', '', '01-datefrom')
dbutils.widgets.text('dateto', '', '01-dateto')

currentdatestr = datetime.now().strftime("%Y-%m-%d")

try:
    datefrom = dbutils.widgets.get("datefrom") 
except Exception as e:
    datefrom = currentdatestr
    dateto = currentdatestr

try:
    dateto = dbutils.widgets.get("dateto")    
except Exception as e:
    datefrom = currentdatestr
    dateto = currentdatestr

if datefrom == '':
    datefrom = currentdatestr
    dateto = currentdatestr

# COMMAND ----------

print(f"date: {datefrom}")
#print(f"dateto: {dateto}")

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, to_date, lit, current_date as spark_current_date, expr
import requests
from io import StringIO
import csv

# Function to fetch exchange rates from the ECB API
def fetch_exchange_rates(datefrom, dateto):
    url = f"https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?startPeriod={datefrom}&endPeriod={dateto}&format=csvdata"

    # example
    # url = f"https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?startPeriod=2019-05-10&endPeriod=2024-07-15&format=csvdata"

    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for non-2xx status codes

    # Parse the CSV data
    csv_data = response.content.decode("utf-8")
    reader = csv.DictReader(StringIO(csv_data))
    rates = []
    for row in reader:
        currency = row["CURRENCY"]
        rate = row["OBS_VALUE"]
        time_period = row["TIME_PERIOD"]
        rates.append((time_period, currency, rate))

    return rates

response_rates = fetch_exchange_rates(datefrom, dateto)



# Create the table if it doesn't exist
try:
    existing_rates_df = spark.table(f"`{env_prefix}-cdh`.reference.exchangerates")
except:
    spark.sql(
        f"CREATE TABLE `{env_prefix}-cdh`.reference.exchangerates (BusinessDate DATE, OriginalCurrency STRING, ExchangeRate DOUBLE, LocalCurrency STRING) USING DELTA"
    )

    spark.sql(f"ALTER TABLE `{env_prefix}-cdh`.reference.exchangerates CHANGE COLUMN BusinessDate COMMENT 'The Business Date the exchange rate was published by ECB'")
    spark.sql(f"ALTER TABLE `{env_prefix}-cdh`.reference.exchangerates CHANGE COLUMN OriginalCurrency COMMENT 'The Currency the exchange rate concerns.'")
    spark.sql(f"ALTER TABLE `{env_prefix}-cdh`.reference.exchangerates CHANGE COLUMN ExchangeRate COMMENT 'This is the exchange rate ammount according to localcurrency (EUR)'")
    spark.sql(f"ALTER TABLE `{env_prefix}-cdh`.reference.exchangerates CHANGE COLUMN LocalCurrency COMMENT 'The base currency the exchange rate is based on. (EUR)'")

    spark.sql(f"REPAIR TABLE `{env_prefix}-cdh`.reference.exchangerates SYNC METADATA")

    existing_rates_df = spark.createDataFrame(
        [],
        schema="BusinessDate DATE, OriginalCurrency STRING, ExchangeRate DOUBLE, LocalCurrency STRING",
    )

#if not empty do the following
if len(response_rates) > 0:
    print(f"The response rates count is {len(response_rates)}")

    # Create the DataFrame with current rates
    response_rates_df = spark.createDataFrame(
        response_rates, ["BusinessDate", "OriginalCurrency", "ExchangeRate"]
    )
    response_rates_df = response_rates_df.withColumn(
        "BusinessDate", to_date(col("BusinessDate"), "yyyy-MM-dd")
    )
    response_rates_df = response_rates_df.withColumn("ExchangeRate", expr("try_cast(ExchangeRate as double)"))
    response_rates_df = response_rates_df.withColumn("LocalCurrency", lit("EUR"))
    # Filter out rows with null ExchangeRate
    response_rates_df = response_rates_df.filter(col("ExchangeRate").isNotNull())

    from pyspark.sql.functions import col

    # Filter out the existing rates with the same values except for the ExchangeRate column
    filtered_existing_rates_df = existing_rates_df.join(
        response_rates_df.select("BusinessDate", "OriginalCurrency", "LocalCurrency"),
        on=["BusinessDate", "OriginalCurrency", "LocalCurrency"],
        how="left_anti"
    )

    # Union the filtered existing rates with the response rates
    combined_rates_df = filtered_existing_rates_df.unionByName(
        response_rates_df, allowMissingColumns=True
    )

    # Write the combined DataFrame back to the table
    combined_rates_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"`{env_prefix}-cdh`.reference.exchangerates")
else:
    raise Exception("ERROR: No exchange rates were fetched")
