# Databricks notebook source
# MAGIC %md
# MAGIC ## 📑 Core Paymnets MtD
# MAGIC  
# MAGIC #### 🔗 Links
# MAGIC - **[BRD link](https://cepalhellas.atlassian.net/wiki/x/AYDHGg)**  
# MAGIC   Comprehensive documentation outlining the business requirements for this report.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Authentication

# COMMAND ----------

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

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col,regexp_replace

import delta_table_creator, extract, transform
from delta import DeltaTable  # type: ignore

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("example").getOrCreate()

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import trim

# COMMAND ----------

import boto3
import os
 
#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"]
 
dbutils.widgets.text("run_date", '')
run_date = dbutils.widgets.get("run_date").strip()

if not run_date:
    run_date = spark.sql(f'''
                         select MAX(BusinessDate) BusinessDate from `{env_prefix}-cdh`.derivative.dexposure 
                         union
                         select MAX(BusinessDate) BusinessDate from `{env_prefix}-cdh`.landed.profits_loan_details
                         ''').collect()

    if len(run_date) != 1: raise ValueError(f"Incorrect run_date!")
    run_date = run_date[0]['BusinessDate']
    run_date = run_date.strftime('%Y-%m-%d') 
 
SOMreferenceDate = spark.sql(f'''
                    SELECT last_day(add_months('{run_date}', -1)) AS SOM
                         ''').collect()[0]['SOM']


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Schema of the table

# COMMAND ----------

# Define the schema name
schema_name = f"`{env_prefix}-cdh-delivery`.repoint"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

table_name = 'Core_PaymentsMtD' 

schema = (
"BusinessDate date," 
"AssetSegment string,"
"ExposureKey string,"
"AgreementKey string,"
"PaymentAmount decimal(25,2),"
"RefMonthYear string,"
"BalanceSOM decimal(19,2),"
"ModificationFlag int,"
"InsertTimeStamp timestamp"
)

# COMMAND ----------

partition_columns = ["BusinessDate"]
delivery_table_path = ""
 
# ================== Create Delta Table ==================
#delivery_table_path = f"`{env_prefix}-cdh-delivery`.repoint.{table_name}"  
#AWS Credentials not available thus env_prefix returns empty temporarily we manually set it to `qa`

delivery_table_path = f"`{env_prefix}-cdh-delivery`.repoint.{table_name}"
delta_table_creator.create_unity_delta_table(
    spark, delivery_table_path, partition_columns, schema
)
# if not DeltaTable.isDeltaTable(spark, delivery_table_path):
#     delta_table_creator.create_unity_delta_table(
#         spark, delivery_table_path, partition_columns, schema
#     )
  
# ================== Delete From Delta Table ==================
transform.delete_from_unity_table(
    spark,
    delivery_table_path,
    run_date
#### type: ignore
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Payments

# COMMAND ----------


Payments = spark.sql(
f'''
with maxBDate as (
  SELECT
  p.ExposureKey, MAX(p.BusinessDate) as BusinessDate
FROM `{env_prefix}-cdh`.derivative.dexposure p where IsActiveFlag =True 
and p.BusinessDate <= '{run_date}'
Group BY p.ExposureKey),

maxpDate as (SELECT
  p.ExposureKey, MAX(p.BusinessDate) as BusinessDate
FROM `{env_prefix}-cdh`.primary.payment p
where p.PaymentActualDate > last_day(add_months('{run_date}', -1))
AND p.PaymentActualDate <= '{run_date}'
Group BY p.ExposureKey)



SELECT 
CAST(p.ExposureKey AS STRING), p.PaymentAmount as PaymentAmount, p.BusinessDate, p.PaymentActualDate
FROM `{env_prefix}-cdh`.primary.payment as p
inner join maxBDate mbd on mbd.ExposureKey = p.ExposureKey 
inner join maxpDate mpd on mpd.ExposureKey = p.ExposureKey and mpd.BusinessDate = p.BusinessDate
WHERE 1=1
and p.PortfolioName like 'Core Retail'
and p.PaymentActualDate > last_day(add_months('{run_date}', -1))
AND p.PaymentActualDate <= '{run_date}'
and p.SourceSystem = 'qca'
'''
)   
Payments.createOrReplaceTempView("Payments")

# COMMAND ----------


Account1 = spark.sql(
f'''
Select AssetSegment, de.ExposureKey, AgreementKey,de.BusinessDate 
from `{env_prefix}-cdh`.derivative.dexposure de
inner join (select ExposureKey, MAX(BusinessDate) as BusinessDate 
            from `{env_prefix}-cdh`.derivative.dexposure 
            where last_day(BusinessDate) ='{run_date}' 
            group by ExposureKey) deb on de.ExposureKey = deb.ExposureKey and de.BusinessDate = deb.BusinessDate
'''
)   
Account1.createOrReplaceTempView("Account1")

# COMMAND ----------

Account2 = spark.sql(
f'''
Select AssetSegment, de.ExposureKey, AgreementKey,de.BusinessDate 
from `{env_prefix}-cdh`.derivative.dexposure de
inner join (select ExposureKey, MAX(BusinessDate) as BusinessDate 
            from `{env_prefix}-cdh`.derivative.dexposure 
            where last_day(BusinessDate) =last_day(add_months('{run_date}',-1 ))
            group by ExposureKey) deb on de.ExposureKey = deb.ExposureKey and de.BusinessDate = deb.BusinessDate
'''
)   
Account2.createOrReplaceTempView("Account2")

# COMMAND ----------


Payments2 = spark.sql(
f'''
SELECT
    case when dex2.AssetSegment is NULL THEN dex.AssetSegment ELSE dex2.AssetSegment END AS AssetSegment,
    p.ExposureKey, 
    case when dex2.AgreementKey is NULL THEN dex.AgreementKey ELSE dex2.AgreementKey END AS AgreementKey,
    SUM(p.PaymentAmount) AS PaymentAmount 
FROM Payments p 
left join Account2 dex 
    on p.ExposureKey = dex.ExposureKey
    and last_day(dex.BusinessDate) = '{SOMreferenceDate}'
left join Account1 dex2  
    on p.ExposureKey = dex2.ExposureKey
    and last_day(dex2.BusinessDate) = '{run_date}'

Group BY
    case when dex2.AssetSegment is NULL THEN dex.AssetSegment ELSE dex2.AssetSegment END,
    p.ExposureKey,
    case when dex2.AgreementKey is NULL THEN dex.AgreementKey ELSE dex2.AgreementKey END
'''
)   
Payments2.createOrReplaceTempView("Payments2")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### LPFXB364

# COMMAND ----------

LPFXB364 = spark.sql(
f'''
SELECT 
  *
FROM `{env_prefix}-cdh`.externalfiles.extlpfxb364 L
WHERE 1=1
and l.BusinessDate = '{run_date}' 
and l.IngestionTimeStamp = (select max(IngestionTimeStamp) from `{env_prefix}-cdh`.externalfiles.extlpfxb364 where BusinessDate = '{run_date}')
AND   date_format(to_date(L.RestructuringDate,'dd/MM/yyy'),'yyyy-MM-dd') >'{SOMreferenceDate}'
'''
)

LPFXB364.createOrReplaceTempView("LPFXB364")

# Check if the dataset is empty and print a message accordingly
if LPFXB364.count() == 0:
    print(f"The required external file extlpfxb364 dataset for date {run_date!r} is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LPFXB364_OLD

# COMMAND ----------

LPFXB364_OLD = spark.sql(
f'''
SELECT DISTINCT
  a.OldExposureKey
FROM LPFXB364 a
ORDER BY a.OldExposureKey desc
'''
)
LPFXB364_OLD.createOrReplaceTempView("LPFXB364_OLD")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LPFXB364_NEW

# COMMAND ----------

LPFXB364_NEW = spark.sql(
f'''
SELECT DISTINCT
  a.NewExposureKey
FROM LPFXB364 a
'''
)
LPFXB364_NEW.createOrReplaceTempView("LPFXB364_NEW")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### PaymentsFinal

# COMMAND ----------

PaymentsFinal = spark.sql(
f'''
SELECT 
   CAST('{run_date}' AS DATE)   AS BusinessDate
  ,p.AssetSegment               AS AssetSegment
  ,p.ExposureKey                AS ExposureKey
  ,p.AgreementKey               AS AgreementKey
  ,p.PaymentAmount              AS PaymentAmount
  ,CONCAT(SUBSTRING('{run_date}', 1, 4), SUBSTRING('{run_date}', 6, 2)) AS RefMonthYear
  ,cast(D1.CASE_OUTSTANDING_AMT as decimal(19,2))  AS BalanceSOM
  ,CASE WHEN L1.OldExposureKey IS NOT NULL OR L2.NewExposureKey IS NOT NULL THEN 1 ELSE 0 END AS ModificationFlag
FROM Payments2 p
  LEFT JOIN `{env_prefix}-cdh`.landed.repointqcab_daily_portfolio D1 ON D1.ACCOUNT_NUMBER = p.ExposureKey and D1.BusinessDate = '{SOMreferenceDate}' --CHECK p
  LEFT JOIN LPFXB364_OLD L1 ON p.ExposureKey = L1.OldExposureKey
  LEFT JOIN LPFXB364_NEW L2 ON p.ExposureKey = L2.NEwExposureKey

'''
)
PaymentsFinal.createOrReplaceTempView("PaymentsFinal")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### final_df

# COMMAND ----------

final_df = spark.sql(
f'''
SELECT 
  CAST(BusinessDate AS date) AS BusinessDate
, CAST(AssetSegment AS string) AS AssetSegment
, CAST(ExposureKey AS string) AS ExposureKey
, CAST(AgreementKey AS string) AS AgreementKey
, CAST(PaymentAmount AS decimal(25,2)) AS PaymentAmount
, CAST(RefMonthYear AS string) AS RefMonthYear
, CAST(BalanceSOM AS decimal(19,2)) AS BalanceSOM
, CAST(ModificationFlag AS int) AS ModificationFlag 
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM PaymentsFinal 
'''
)
final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

if final_df.rdd.isEmpty():
    raise ValueError(f"Core_PaymentsMtD: final DataFrame is empty - aborting write to {delivery_table_path} to avoid overwriting with no data")

final_df.write \
    .format("delta") \
    .partitionBy("BusinessDate") \
    .mode("append") \
    .saveAsTable(delivery_table_path) # in order to write in delta_lake

# COMMAND ----------

from pyspark.sql.functions import col, count, concat

def check_table(delivery_table_path, run_date, key_columns, duplicate_check_action="error", empty_check_action="error"):
    errors = []
    warnings = []

    if key_columns:
        # Create the composite key column if there are multiple key columns
        if len(key_columns) > 1:
            composite_key = concat(*[col(c) for c in key_columns]).alias("CompositeKey")
            key_column_name = "CompositeKey"
            key_columns_string = ", ".join(key_columns)
        else:
            key_column_name = key_columns[0]
            key_columns_string = key_columns[0]

        # Duplicate Checks
        duplicate_count = (
            spark.table(delivery_table_path)
            .filter(col("BusinessDate") == run_date)
            .withColumn("CompositeKey", composite_key if len(key_columns) > 1 else col(key_column_name))
            .groupBy(key_column_name)
            .count()
            .filter("count > 1")
            .select(key_column_name, "count")
            .toPandas()
            .to_dict(orient='records')
        )

        if duplicate_count:
            message = f"Duplicate {key_columns_string} entries found: {duplicate_count}"
            if duplicate_check_action == "error":
                errors.append(message)
            elif duplicate_check_action == "warning":
                warnings.append(message)

    # Check if the table for the specific date is empty
    row_count = spark.sql(f"""
        SELECT * FROM {delivery_table_path}
        WHERE BusinessDate = '{run_date}'
    """).count()

    if row_count == 0:
        message = f"The {delivery_table_path} dataset is empty for the date {run_date}."
        if empty_check_action == "error":
            errors.append(message)
        elif empty_check_action == "warning":
            warnings.append(message)

    if warnings:
        print("Warnings:\n" + "\n".join(warnings))

    if errors:
        raise ValueError("Errors found:\n" + "\n".join(errors))

key_columns = ["ExposureKey"]
check_table(delivery_table_path, run_date, key_columns, duplicate_check_action="error", empty_check_action="warning")

# COMMAND ----------

import re
def rename_and_cleanup_files(output_path, file_name):
    part_file_regex = re.compile(r'part-.*')
    system_file_regex = re.compile(r'^_(committed_|started_|SUCCESS)')
 
    # Move the part file to the final filename.
    files = dbutils.fs.ls(output_path)
    for file in files:
        print(f"file: {file.name}")
        if part_file_regex.match(file.name):
            original_path = file.path
            new_path = output_path + file_name
            dbutils.fs.mv(original_path, new_path)
 
    # Remove system files from the common folder.
    files = dbutils.fs.ls(output_path)
    for file in files:
        if system_file_regex.match(file.name):
            dbutils.fs.rm(file.path, recurse=False)
 
# Example usage:
# rename_and_cleanup_files(output_path, file_name)

# COMMAND ----------

PaymentsMtD = spark.sql(
f'''
select * from {delivery_table_path}
where BusinessDate = {run_date!r}
'''
)
PaymentsMtD.createOrReplaceTempView("PaymentsMtD")

# COMMAND ----------

output_path = "/Volumes/"+env_prefix+"-cdh-delivery/repoint/exports/Core/PaymentsMtD/"
file_name = "PaymentsMtD_" +run_date+ ".csv"
PaymentsMtD.coalesce(1).write.mode("append").option("header", True).csv(f"{output_path}")
rename_and_cleanup_files(output_path, file_name)
