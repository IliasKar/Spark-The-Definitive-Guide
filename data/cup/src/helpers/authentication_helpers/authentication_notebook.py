# Databricks notebook source
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

# DBTITLE 1,Load Parameters
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, when, udf, concat, year, month, format_string, current_date, last_day, date_sub, dayofweek, expr, to_date
import calendar

try:
    # Get the reporting_date parameter.
    input_date = dbutils.widgets.get("reporting_date")
    
    if input_date:
        # If reporting_date is provided, validate its format.
        datetime.strptime(input_date, '%Y-%m-%d')
        # If reporting_date is valid, treat it as the business_date.
        business_date = input_date
    else:
        # If reporting_date is not provided, use the current date.
        current_date = datetime.now().date()
        
        # Get ref_calendar from table to df
        database_name = f'`{env_prefix}-cdh`.reference.ref_calendar'
        query = f"SELECT PreviousMonthLastWorkingDate FROM {database_name} WHERE CalendarDate = '{current_date}'"
        df_ref_calendar = spark.sql(query)
        
        if df_ref_calendar.count() == 0:
            raise ValueError(f"No record found for the date: {current_date}")

        business_date = df_ref_calendar.collect()[0][0].strftime('%Y-%m-%d')

except ValueError as e:
    error_message = f"Error: {str(e)} - The reporting_date parameter is not in the correct format (YYYY-MM-DD)."
    print(error_message)
    dbutils.notebook.exit(error_message)
except Exception as e:
    error_message = f"Error: {str(e)} - The reporting_date parameter is not set correctly."
    print(error_message)
    dbutils.notebook.exit(error_message)

# COMMAND ----------

# DBTITLE 1,Parse the authentication data to dates
dbutils.jobs.taskValues.set(key = 'business_date', value = business_date)
dbutils.jobs.taskValues.set(key = "env_prefix", value = env_prefix)
