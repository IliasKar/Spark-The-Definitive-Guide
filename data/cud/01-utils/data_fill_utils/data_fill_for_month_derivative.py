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

'''
cases:

τρέχω μόνο τα EOM => dates_one_by_one_only = False, eom_only = True
τρέχω μόνο τα dates του μήνα => target_month = <ο μήνας>, eom_only = False, dates_one_by_one_only = True
'''

target_month = "07"

dates_one_by_one_only = False
eom_only = True

# COMMAND ----------

if not eom_only:
    sql_query = f"""select CalendarDate from `{env_prefix}-cdh`.reference.ref_calendar
                    where isbusinessday = 1 and CalendarDate like '2024-{target_month}-%' order by CalendarDate asc; """

    target_dates_df = spark.sql(sql_query) 
    # display(target_dates_df)

# COMMAND ----------

import requests
import traceback

# DParty, DUnderLaw, DExposure tables only

if not eom_only:
    job_id = "530802089604665"

    for row in target_dates_df.collect():
        calendar_date = row["CalendarDate"]
        arguments = {"job_id": job_id, "job_parameters": {"run_date": calendar_date}}
        response = requests.post(
            f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
            json=arguments,
            headers={"Authorization": "Bearer dapi8ec412165d5d95f2697bb02be096a24a"}
        )
        if response.status_code == 200:
            print(f"Job with ID {job_id} called successfully for CalendarDate {calendar_date}")
        else:
            print(f"Failed to call job with ID {job_id} for CalendarDate {calendar_date}")
            print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC do the same for tables dcollateral and drealestate using the controller

# COMMAND ----------

import requests
import traceback

tables_list = ["DCollateral", "DRealEstate", "DInvestments", "dlegalprocessstatusdebtor"]

if not eom_only:

    job_id = "346583397680646"

    for table_name in tables_list:
        for row in target_dates_df.collect():
            calendar_date = row["CalendarDate"]
            arguments = {"job_id": job_id, "job_parameters": {"run_date": calendar_date, "table_name": table_name}}
            response = requests.post(
                f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
                json=arguments,
                headers={"Authorization": "Bearer dapi8ec412165d5d95f2697bb02be096a24a"}
            )
            if response.status_code == 200:
                print(f"Job with ID {job_id} called successfully for CalendarDate {calendar_date} and Table {table_name}")
            else:
                print(f"Failed to call job with ID {job_id} for CalendarDate {calendar_date} and Table {table_name}")
                print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EOM ONLY

# COMMAND ----------

import datetime

# Get the end of the current month
current_date = datetime.date.today()
end_of_month = datetime.date(current_date.year, current_date.month, 1)

# Replace the date in the SQL query
eom_sql_query = f"""select case when isbusinessday = 0 then try_cast(PreviousBusinessDay as string) else CalendarDate end as CalendarDate from `{env_prefix}-cdh`.reference.ref_calendar where CalendarDate in (
select distinct try_cast(LastDateOfMonth as String) from `{env_prefix}-cdh`.reference.ref_calendar
where isbusinessday = 1 and CalendarDate like '2024-%'  and try_cast(CalendarDate as date) < '{end_of_month}') order by CalendarDate
"""

target_dates_df = spark.sql(eom_sql_query)

print(end_of_month)

# COMMAND ----------

import requests
import traceback

if not dates_one_by_one_only:
    job_id = "530802089604665"

    for row in target_dates_df.collect():
        calendar_date = row["CalendarDate"]
        arguments = {"job_id": job_id, "job_parameters": {"run_date": calendar_date}}
        response = requests.post(
            f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
            json=arguments,
            headers={"Authorization": "Bearer dapi8ec412165d5d95f2697bb02be096a24a"}
        )
        if response.status_code == 200:
            print(f"Job with ID {job_id} called successfully for CalendarDate {calendar_date}")
        else:
            print(f"Failed to call job with ID {job_id} for CalendarDate {calendar_date}")
            print(response.text)

# COMMAND ----------

import requests
import traceback

if not dates_one_by_one_only:

    job_id = "346583397680646"

    for table_name in tables_list:
        for row in target_dates_df.collect():
            calendar_date = row["CalendarDate"]
            arguments = {"job_id": job_id, "job_parameters": {"run_date": calendar_date, "table_name": table_name}}
            response = requests.post(
                f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
                json=arguments,
                headers={"Authorization": "Bearer dapi8ec412165d5d95f2697bb02be096a24a"}
            )
            if response.status_code == 200:
                print(f"Job with ID {job_id} called successfully for CalendarDate {calendar_date} and Table {table_name}")
            else:
                print(f"Failed to call job with ID {job_id} for CalendarDate {calendar_date} and Table {table_name}")
                print(response.text)
