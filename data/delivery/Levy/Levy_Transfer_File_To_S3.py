# Databricks notebook source
import boto3
from botocore.exceptions import NoCredentialsError
import traceback
from datetime import datetime
import os
from pyspark.dbutils import DBUtils
import re
import pandas as pd
from collections import defaultdict
from pyspark.sql import SparkSession
from io import BytesIO
import json
from typing import Optional, Dict, List

# COMMAND ----------

continue_run = dbutils.jobs.taskValues.get(taskKey = "Levy_Creation_Export", key = "continue_run")
if continue_run == "false":
    dbutils.notebook.exit("Skiped")

# COMMAND ----------

running_date = dbutils.jobs.taskValues.get(taskKey = "Levy_Creation_Export", key = "running_date")

# COMMAND ----------

try:
    delivery_args = dbutils.widgets.get("delivery_args")
    delivery_args = delivery_args.replace("\'", "\"")
except Exception as e:
    raise ValueError("Missing required input: delivery_args")

try:
    run_date = dbutils.widgets.get("run_date")
except Exception:
    if running_date != None:
        run_date = running_date
    else:
        run_date = datetime.now().strftime("%Y-%m-%d")

try:
    report_output = dbutils.widgets.get("report_output")
except Exception as e:
    raise ValueError("Missing required input: report_output")

try:
    report_name = dbutils.widgets.get("report_name")
except Exception as e:
    raise ValueError("Missing required input: report_name")

# COMMAND ----------

print(f'report_output: {report_output}')
print(f'run_date: {run_date}')
print(f'report_name: {report_name}')

# COMMAND ----------

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

# Get the Bucket prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

# COMMAND ----------

class S3ModuleArgumentsHandler:
    def __init__(
        self, run_date: str, 
        delivery_args: str, 
        report_output: str,
        report_name: str
        ) -> None:
        """
        Initializes the S3ModuleArgumentsHandler with the provided arguments.

        Args:
            run_date (str): The reference date (e.g., "2024-12-18").
            delivery_args (str): JSON string containing delivery arguments.
            report_output (str): Report output path.
            report_name (str): Report name.

        Raises:
            ValueError: If delivery_args is not valid JSON or is missing required keys.
            TypeError: If any input is not of the expected type.
        """
        self.run_date = run_date
        self.report_output = report_output
        self.report_name = report_name

        try:
            self.delivery_args = json.loads(delivery_args)
            if not isinstance(self.delivery_args, dict):
                raise ValueError("delivery_args should be a JSON object (dictionary).")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON provided for delivery_args: {e}") from e

        self.validate_inputs()

    def validate_inputs(self) -> None:
        """
        Validates the instance attributes to ensure they meet the required structure and types.

        Raises:
            TypeError: If any attribute is of the wrong type or is None.
            ValueError: If delivery_args is missing required keys or has invalid structure.
        """
        if self.run_date is None or not isinstance(self.run_date, str):
            raise TypeError("run_date must be a non-empty string.")
        
        if self.report_output is None or not isinstance(self.report_output, str):
            raise TypeError("report_output must be a non-empty string.")

        if self.report_name is None or not isinstance(self.report_output, str):
            raise TypeError("report_name must be a non-empty string.")
        
        if self.delivery_args is None or not isinstance(self.delivery_args, dict):
            raise ValueError("delivery_args must be a non-empty dictionary.")

        required_keys = ['method', 'sftp_args', 'sharepoint_args', 's3_args']
        for key in required_keys:
            if key not in self.delivery_args:
                raise ValueError(f"delivery_args must contain the key '{key}'.")

        if not isinstance(self.delivery_args.get('s3_args', {}), dict):
            raise ValueError("'s3_args' in delivery_args must be a dictionary.")

        if 'destination_path' not in self.delivery_args['s3_args']:
            raise ValueError("'destination_path' must be present in 's3_args'.")

    def get_s3_destination_path(self) -> str:
        """
        Retrieves the destination path from the s3_args.

        Returns:
            str: The destination path as a string.
        """
        return self.delivery_args['s3_args']['destination_path']

# COMMAND ----------

class volumeToS3FileTransferHandler(S3ModuleArgumentsHandler):
    """
    Handles the transfer of files from a volume to an S3 bucket.

    Attributes:
        run_date (str): The date used to create subfolder structures.
        delivery_args (str): Arguments related to the delivery configuration.
        report_output (str): The directory containing the report files.
        report_name (str): The name of the report being processed.
        dbutils (DBUtils): Utility object for file system operations.
    """

    def __init__(
        self, 
        run_date: str, 
        delivery_args: str, 
        report_output: str, 
        report_name: str
    ) -> None:
        """
        Initializes the handler with the provided parameters.

        Args:
            run_date (str): The date used to create subfolder structures.
            delivery_args (str): Arguments related to the delivery configuration.
            report_output (str): The directory containing the report files.
            report_name (str): The name of the report being processed.
        """
        self.run_date = run_date
        self.delivery_args = delivery_args
        self.report_output = report_output
        self.report_name = report_name
        S3ModuleArgumentsHandler.__init__(self, run_date, delivery_args, report_output, report_name)
        self.dbutils = DBUtils(spark)

    def create_s3_path(self) -> str:
        """
        Creates the S3 path where the files will be stored.

        Returns:
            str: The full S3 path including the subfolder.
        """
        subfolder = self.run_date.replace("-", "")
        s3_destination_path_with_subfolder = f"{self.get_s3_destination_path()}{subfolder}"

        return s3_destination_path_with_subfolder

    def report_output_location(self) -> str:
        """
        Determines the local path of the report output including subfolders.

        Returns:
            str: The full local path to the report output.
        """
        subfolder = self.run_date.replace("-", "")
        report_output_with_subfolder = f"{self.report_output}{subfolder}"
        print(f"Report location from which we will fetch it from: {report_output_with_subfolder}")

        return report_output_with_subfolder

    def move_file_from_vol_to_s3(self) -> None:
        """
        Moves files from a local volume to the specified S3 bucket.

        Prints details of the operation and handles any errors that occur.
        """
        report_output_with_subfolder = self.report_output_location()
        s3_destination_path_with_subfolder = self.create_s3_path()

        bucket = s3_destination_path_with_subfolder.split('/')[2]  # Extract bucket and prefix from destination path
        prefix = '/'.join(s3_destination_path_with_subfolder.split('/')[3:])

        print(f"Started moving files from {report_output_with_subfolder} to {s3_destination_path_with_subfolder}.")

        try:
            files_to_move = self.dbutils.fs.ls(report_output_with_subfolder)

            if not files_to_move:
                raise FileNotFoundError(f"No files found in the source directory: {report_output_with_subfolder}")

            S3_client.put_object(Bucket=bucket, Key=prefix + '/')
            for file in files_to_move:
                if not file.isDir():
                    file_content = open(os.path.join(report_output_with_subfolder, file.name), 'rb')
                    file_path = os.path.join(report_output_with_subfolder, file.name)
                    
                    file_size = os.path.getsize(file_path)

                    if file_size == 0:
                        raise ValueError(f"File {file.name} is empty (0 kilobytes). Aborting upload.")

                    s3_key =f"{prefix}{'/'}{file.name}"    
                    S3_client.put_object(
                        Bucket=bucket, 
                        Key=s3_key, 
                        Body=file_content
                    )

                    print(f"Copied {file.name} to bucket: {bucket}")
            print("All files copied successfully")
        except Exception as e:
            print(f"Error: {str(e)}")
            raise Exception(f"Error: {str(e)}")

# COMMAND ----------

vol_to_s3_transfer_obj = volumeToS3FileTransferHandler(run_date, delivery_args, report_output, report_name)

print(f"Report location from which we will fetch the file from: {vol_to_s3_transfer_obj.report_output_location()}")
print(f"s3 bucket location in which the file will be moved to: {vol_to_s3_transfer_obj.create_s3_path()}")

# Final file transfer
vol_to_s3_transfer_obj.move_file_from_vol_to_s3()
