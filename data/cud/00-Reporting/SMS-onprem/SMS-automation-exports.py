# Databricks notebook source
# MAGIC %md
# MAGIC DATAMARTS 
# MAGIC ip: 10.240.2.231
# MAGIC user: "PBIAgent"
# MAGIC password: "PB1Ag3nt!"

# COMMAND ----------

from pyspark.sql.functions import coalesce, collect_set, sha2, collect_list, substring, regexp_replace, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs, DataFrame, trim,current_timestamp,from_utc_timestamp
from pyspark.sql import Window, Row
from pyspark.sql.types import StringType, BooleanType, NullType, IntegerType
from datetime import timedelta, datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
import delta_table_creator, transform
import delta  # type: ignore
from delta import DeltaTable  # type: ignore

# import pyspark.sql.functions as F

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y%m%d")

botosession = boto3.session.Session(region_name="eu-central-1")

# Initialize the SSM client
ssm_client = botosession.client("ssm", region_name="eu-central-1")

response = ssm_client.get_parameter(
    Name='/cfn/bucket_prefix',
    WithDecryption=False
)

env_prefix = response['Parameter']['Value']

# Initialize the S3 client
s3_client = botosession.client("s3", region_name="eu-central-1")

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

# COMMAND ----------

from pyspark.sql.functions import collect_set, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs
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
spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env_prefix = os.environ["S3PREFIX"] (not working always.. deprecated. Moved to SSM)

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y%m%d")
debug = False
emulatefe = False
reducerecordsA360 = False
emulatebusinessdate = False

#------------------------------------------

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


# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
    
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        if aws_secret_key is None:
            print("AWS secret key is None. Please check Databricks secrets configuration.")
        else:
            print("AWS secret key retrieved successfully.")

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

# Connect to AWS using boto3
ssm_client = boto3.client('ssm', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)

s3_client = boto3.client('s3', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)                           

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

# Get the Buckewt prefix from SSM to be sure
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')
    
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False


# COMMAND ----------

jdbc_url = "jdbc:sqlserver://10.240.2.230:1433;databaseName=Core;encrypt=false"
connection_properties = {
    "user": "DEUser",
    "password": "MM998fqLvS9H2",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Define the SQL query
sql_query = """
SELECT top 1 *
FROM output.Account --bi.assignment a where AgentID = '2204385'
"""

# Read data using the SQL query
result_df = spark.read.jdbc(url=jdbc_url, table=f"({sql_query}) as tmp", properties=connection_properties)

# Show the DataFrame
result_df.show()

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://10.240.2.231:1433;databaseName=Core;encrypt=false"
connection_properties = {
    "user": "PBIAgent",
    "password": "PB1Ag3nt!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Define the SQL query
sql_query = """
SELECT top(5) AccountNumber
FROM output.Account --bi.assignment a where AgentID = '2204385'
"""

# Read data using the SQL query
result_df = spark.read.jdbc(url=jdbc_url, table=f"({sql_query}) as tmp", properties=connection_properties)

# Show the DataFrame
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3rd party

# COMMAND ----------

sql_query = """
select AccountNumber, max(ReferenceDate) ref_date
into #max_d1
from bi.Loan 
group by AccountNumber

select CDI, max(ReferenceDate) ref_date
into #max_d2
from bi.Customer
group by CDI

select AccountNumber, max(ActivityDate) act_date
into #max_d3
from bi.ContactActivity
group by AccountNumber

select AccountNumber, max(ETLLoadingDatetime) etl_date
into #max_d4
from bi.LoanCustomerRelation
group by AccountNumber

select AccountNumber, max(ReferenceDate) ref_date
into #max_d5
from bi.Settlement
group by AccountNumber

----------------------------------------------------------

select distinct a.AccountNumber, AgreementNumber, IsActiveFlag, TaxNumber, Queue, PortfolioName, FinalBucket, SettlementBucket, SourceBucket, SourcePastDueDays, AssetSegment, ProductKey, ProductDesc, SubProductDesc, SPV, OnBalance, CustomerSegment, Channel, AssignmentProcess, ReferenceDate
into #loan
from bi.Loan a
inner join #max_d1 b
on a.AccountNumber=b.AccountNumber and a.ReferenceDate=b.ref_date

select distinct a.CDI, LegalForm, DeceasedFlag
into #customer
from bi.Customer a
inner join #max_d2 b
on a.CDI=b.CDI and a.ReferenceDate=b.ref_date

select distinct a.AccountNumber, CDI, ContactType, ActivityDate, ActivityName, PromiseStatus
into #cont_act
from bi.ContactActivity a
inner join #max_d3 b
on a.AccountNumber=b.AccountNumber and a.ActivityDate=b.act_date

select distinct a.AccountNumber, CDI, RelationType
into #loan_cust_rel
from bi.LoanCustomerRelation a
inner join #max_d4 b
on a.AccountNumber=b.AccountNumber and a.ETLLoadingDatetime=b.etl_date

select a.AccountNumber, SettlementStatus
into #settlement
from bi.Settlement a
inner join #max_d5 b
on a.AccountNumber=b.AccountNumber and a.ReferenceDate=b.ref_date
------------------------------------------------------------------





