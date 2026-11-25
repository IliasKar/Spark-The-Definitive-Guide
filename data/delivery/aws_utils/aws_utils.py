import boto3
from botocore.exceptions import NoCredentialsError

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

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

# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key, ssm_client, aws_secret_key):
 
    if aws_secret_key is None:
        print("AWS secret key not available.")
        return
 
    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )
 
    return response['Parameter']['Value']

# Function to get the environment prefix
def get_env_prefix():
    secrets_scope = "aws-databricks"
    aws_keys = get_scope_key_names(secrets_scope)
    aws_key = aws_keys[0] if aws_keys else None
    # Get AWS secret key from Databricks secrets
    aws_secret_key = get_aws_credentials(secrets_scope,aws_key)
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)
 
    S3_client = boto3.client('s3', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)  

    env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix', ssm_client, aws_secret_key)
    return env_prefix

# Function to get full environment name
def get_env_fullname():
    env_prefix = get_env_prefix()
    env_fullname = f"`{env_prefix}-cdh`"
    return env_fullname

def get_databricks_job_id(cfn_parameter_name):
    secrets_scope = "aws-databricks"
    aws_keys = get_scope_key_names(secrets_scope)
    aws_key = aws_keys[0] if aws_keys else None
    # Get AWS secret key from Databricks secrets
    aws_secret_key = get_aws_credentials(secrets_scope,aws_key)
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)
 
    S3_client = boto3.client('s3', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)  

    job_id = connect_to_aws_and_access_ssm(cfn_parameter_name, ssm_client, aws_secret_key)
    return job_id

