# Databricks notebook source
# MAGIC %md
# MAGIC ## 📑 HAPS FLDT Modifications
# MAGIC  
# MAGIC #### 🔗 Links
# MAGIC - **[BRD link](https://cepalhellas.atlassian.net/wiki/x/AQDdHQ)**  
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


from pyspark.sql import DataFrame
from pyspark.sql.functions import trim

# COMMAND ----------

import boto3
import os

#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ"S3PREFIX" 

dbutils.widgets.text("run_date", '')
run_date = dbutils.widgets.get("run_date").strip()

if not run_date:
    run_date = spark.sql(f'''
                         select MAX(BusinessDate) BusinessDate from `{env_prefix}-cdh`.derivative.dexposure 
                         union
                         select MAX(BusinessDate) BusinessDate from `p-cdh`.landed.profits_loan_details
                         ''').collect()

    if len(run_date) != 1: raise ValueError(f"Incorrect run_date!")
    run_date = run_date[0]['BusinessDate']
    run_date = run_date.strftime('%Y-%m-%d')  

som_date = spark.sql(
f'''
SELECT 
     MAX(cal.CalendarDate) CalendarDate
FROM `{env_prefix}-cdh`.reference.ref_calendar AS cal
WHERE cal.IsBusinessDay = 1
AND cal.CalendarDate <= LAST_DAY(DATE_ADD(MONTH, -1, {run_date!r}))
''').collect()[0][0]



# COMMAND ----------

# Define the schema name
schema_name = f"`{env_prefix}-cdh-delivery`.repoint"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

table_name = 'HAPS_FLDTModifications'

schema = (
"BusinessDate date," 
"ImplementationDate date,"
"AssetSegment string,"
"CDI string,"
"TaxNumber string,"
"OldExposureKey string,"
"NewExposureKey string,"
"ModificationAmountAfterHaircut decimal(19,2),"
"HaircutAmount decimal(19,2),"
"ModificationType string,"
"ConditionalwoffExposureKey string,"
"SOMBalanceOfPreviousAccountEUR decimal(19,2),"
"SOMSourceBucketOfPreviousAccount string,"
"SOMAccountStatusOfPreviousAccount string,"
"ApplicationKey string,"
"ApplicationDateCreated date,"
"SolutionType string,"
"FlowSegment string,"
"CommunicationChannel string,"
"ModificationAmountBeforeHaircut decimal(19,2),"
"InsertTimeStamp timestamp"
)

# COMMAND ----------

partition_columns = ["BusinessDate"]
delivery_table_path = ""
 
# ================== Create Delta Table ==================
#delivery_table_path = f"`{env_prefix}-cdh-delivery`.repoint.{table_name}"  
#AWS Credentials not available thus env_prefix returns empty temporarily we manually set it to `qa`
#env_prefix = 'qa'
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
# MAGIC ##### haps_portfolios

# COMMAND ----------

haps_portfolios = spark.sql(
f'''
SELECT DISTINCT A.PortfolioName
FROM `{env_prefix}-reportinputfiles`.repoint.haps_repointportfolios  A
INNER JOIN (SELECT BusinessDate, MAX(IngestionTimeStamp) IngestionTimeStamp, PortfolioName 
            FROM `{env_prefix}-reportinputfiles`.repoint.haps_repointportfolios
            GROUP BY  BusinessDate, PortfolioName) B
ON A.BusinessDate = B.BusinessDate AND A.PortfolioName = B.PortfolioName AND A.IngestionTimeStamp = B.IngestionTimeStamp
WHERE A.Report = 'HAPS_FLDTModifications'
AND A.IsActive = 1 
''' 
)
haps_portfolios.createOrReplaceTempView("haps_portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dexposure (Active / Inactive / SOM)

# COMMAND ----------


dexposure_active = spark.sql(
f'''
SELECT 
    A.BusinessDate
    , A.ExposureKey
    , A.SourceSystem
    , A.PortfolioName
    , A.CDI
    , A.AssetSegment
    , A.ExposureStatus 
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.OutstandingTotalBalance, A.BusinessDate) as `TotalLegalClaimEUR`
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.GBV, A.BusinessDate) as `GBVEUR`
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.OnBalance, A.BusinessDate) as `OnBalanceAmountEUR`
    , e.Bucket
    , A.Currency
    , case when A.IsActiveFlag = 'true' THEN 1 ELSE 0 END as `IsActiveFlag`
    , A.DaysPastDueFinal
    , A.Opendate
    , A.ProductKey
FROM `{env_prefix}-cdh`.derivative.dexposure as A 
INNER JOIN haps_portfolios AS HP
ON A.PortfolioName = HP.PortfolioName
INNER JOIN (select BusinessDate, ExposureKey, Bucket  
             from`{env_prefix}-cdh`.primary.exposure
             where  PartyRelationshipType = 'Primary Owner' 
                and SourceSystem = 'profits'
             ) as e on e.ExposureKey = A.ExposureKey and e.BusinessDate = A.BusinessDate
WHERE A.BusinessDate in ({run_date!r},{som_date!r})    
AND A.IsActiveFlag = true
'''
)
dexposure_active.createOrReplaceTempView("dexposure_active")

# COMMAND ----------


dexposure_inactive = spark.sql(
f'''
SELECT 
    {run_date!r} as BusinessDate
    , A.ExposureKey
    , A.SourceSystem
    , A.PortfolioName
    , A.CDI
    , A.AssetSegment
    , A.ExposureStatus 
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.OutstandingTotalBalance, A.BusinessDate) as `TotalLegalClaimEUR`
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.GBV, A.BusinessDate) as `GBVEUR`
    , `{env_prefix}-cdh`.derivative.converttoeur(A.Currency, A.OnBalance, A.BusinessDate) as `OnBalanceAmountEUR`
    , e.Bucket
    , A.Currency
    , case when A.IsActiveFlag = 'true' THEN 1 ELSE 0 END as `IsActiveFlag`
    , A.DaysPastDueFinal
    , A.Opendate
    , A.ProductKey
from (select e.*
      , rank() over (partition by e.ExposureKey order by e.BusinessDate desc) as Rank
      from `{env_prefix}-cdh`.derivative.dexposure e
      INNER JOIN haps_portfolios AS HP
      ON E.PortfolioName = HP.PortfolioName
      where e.SourceSystem = 'profits'
        and e.IsActiveFlag = false
        and e.BusinessDate > {som_date!r}
        and e.BusinessDate <= {run_date!r}
        ) AS A
INNER JOIN (select BusinessDate, ExposureKey, Bucket  
             from`{env_prefix}-cdh`.primary.exposure
             where  PartyRelationshipType = 'Primary Owner' 
                and SourceSystem = 'profits'
             ) as e on e.ExposureKey = A.ExposureKey and e.BusinessDate = A.BusinessDate
left anti join dexposure_active e_pf_act on e_pf_act.ExposureKey = A.ExposureKey and e_pf_act.BusinessDate = {run_date!r} 
where A.Rank = 1
'''
)
dexposure_inactive.createOrReplaceTempView("dexposure_inactive")

# COMMAND ----------

dexposure = spark.sql(
f'''
select *
from dexposure_active
where BusinessDate = {run_date!r}

union all

select *
from dexposure_inactive

'''
)
dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### product

# COMMAND ----------

product = spark.sql(
f'''
SELECT P.BusinessDate, P.ProductKey, P.ProductDescription
FROM `{env_prefix}-cdh`.primary.product as P
WHERE P.BusinessDate = {run_date!r}
'''
)
product.createOrReplaceTempView("product")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### restructuring

# COMMAND ----------

restructuring = spark.sql(
f'''
SELECT distinct M.BusinessDate, M.OldExposureKey, M.NewExposureKey, M.RestructuringDate
FROM `{env_prefix}-cdh`.primary.restructuring as M
INNER JOIN haps_portfolios AS HP
ON M.PortfolioName = HP.PortfolioName
WHERE M.BusinessDate = {run_date!r}
'''
)
restructuring.createOrReplaceTempView("restructuring")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT P.BusinessDate, P.PortfolioName, DATE(P.AssignmentDate)
FROM `{env_prefix}-cdh`.primary.portfolio P
INNER JOIN haps_portfolios AS HP
ON P.PortfolioName = HP.PortfolioName
WHERE P.BusinessDate = {run_date!r}
'''
)
portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fldt_unifiedloan

# COMMAND ----------

FLDTApplications = spark.sql(
f'''
SELECT 
app.AccountImplementationStatus
,	app.unifiedLoan_SpvDescription																 
,	app.profitsInfo_DateCreated		
,	app.profitsInfo_TransactionDate		
,	app.customer_VATNumber																	     
,	app.unifiedLoan_AccountNumberWithPrefix													     
,	app.profitsInfo_NewAccountNumber 
,	app.loan_ContractNumberSystemicNr														     
,	app.product_ModificationAmountAfterHaircut												     
,	app.product_HairCutAmount																     
,	app.product_Code	
,	app.product_ProductCategoryType	
,	app.app_ApplicationKey																	     
,	app.app_DateCreated																		     
,	app.product_AutoCreditStatusDescription													     
,	app.app_FlowSegmentDescription															     
,	app.app_CommunicationChannelDescription		
,   app.QualcoAppDisbursedDate			
FROM `{env_prefix}-cdh-delivery`.repoint.haps_fldtallapplications AS app 
WHERE app.BusinessDate = {run_date!r};
'''
)
FLDTApplications.createOrReplaceTempView("FLDTApplications")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dexposure_som

# COMMAND ----------

dexposure_som = spark.sql(
f'''
select *
from dexposure_active
where BusinessDate = {som_date!r}

'''
)
dexposure_som.createOrReplaceTempView("dexposure_som")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ConditionalWoff

# COMMAND ----------

ConditionalWoff = spark.sql(
f'''
SELECT DISTINCT mod.NewExposureKey,	mod.OldExposureKey
FROM dexposure AS acc 


LEFT JOIN restructuring as mod
	ON acc.ExposureKey = mod.NewExposureKey 
	AND acc.BusinessDate = mod.BusinessDate

LEFT JOIN product Prd
	ON acc.ProductKey = Prd.ProductKey
	AND acc.BusinessDate = Prd.BusinessDate  
WHERE Prd.ProductDescription LIKE '%ΥΠΟ ΑΙΡΕΣΗ%'
AND acc.IsActiveFlag = 1
'''
)
ConditionalWoff.createOrReplaceTempView("ConditionalWoff")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FLDTModifications

# COMMAND ----------

FLDTModifications = spark.sql(
f'''
SELECT DISTINCT
    {run_date!r} AS `BusinessDate`
  ,	som.PortfolioName
  ,	app.QualcoAppDisbursedDate AS `ImplementationDate`
  ,	som.AssetSegment													 
  ,	som.CDI
  ,	app.customer_VATNumber AS `TaxNumber`
  ,	app.unifiedLoan_AccountNumberWithPrefix AS `OldExposureKey`
  ,	CONCAT(LEFT(app.unifiedLoan_AccountNumberWithPrefix,3),app.profitsInfo_NewAccountNumber) AS `NewExposureKey`
  ,	app.loan_ContractNumberSystemicNr AS `AgreementKey`
  ,	app.product_ModificationAmountAfterHaircut AS `ModificationAmountAfterHaircut`
  ,	app.product_HairCutAmount AS `HaircutAmount`
  ,	app.product_ProductCategoryType AS `ModificationType`
  ,	cwoff.NewExposureKey AS `ConditionalwoffExposureKey`
  ,	som.OnBalanceAmountEUR AS `SOMBalanceOfPreviousAccountEUR`
  ,	som.Bucket AS `SOMSourceBucketOfPreviousAccount`
  ,	som.ExposureStatus AS `SOMAccountStatusOfPreviousAccount`
  ,	app.app_ApplicationKey AS `ApplicationKey`				
  ,	app.app_DateCreated AS `ApplicationDateCreated`
  ,	app.product_AutoCreditStatusDescription AS `SolutionType`
  ,	app.app_FlowSegmentDescription AS `FlowSegment`
  ,	app.app_CommunicationChannelDescription AS `CommunicationChannel`
  ,	app.product_ModificationAmountAfterHaircut + app.product_HairCutAmount AS `ModificationAmountBeforeHaircut`
FROM FLDTApplications AS app
LEFT JOIN ConditionalWoff AS cwoff 
  ON app.unifiedLoan_AccountNumberWithPrefix = cwoff.OldExposureKey
INNER JOIN dexposure_som AS som   
  ON app.unifiedLoan_AccountNumberWithPrefix = som.ExposureKey
WHERE app.AccountImplementationStatus = 'Implemented'
AND CAST(app.QualcoAppDisbursedDate AS DATE) >  DATEADD(MONTH, - 1, last_day({run_date!r}) )
AND CAST(app.QualcoAppDisbursedDate AS DATE) <= {run_date!r}
'''
)
FLDTModifications.createOrReplaceTempView("FLDTModifications")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### final_df

# COMMAND ----------

final_df = spark.sql(
f'''
SELECT 
  CAST(A.BusinessDate AS DATE) AS BusinessDate
, CAST(A.ImplementationDate AS DATE) AS ImplementationDate
, CAST(A.AssetSegment AS STRING) AS AssetSegment
, CAST(A.CDI AS STRING) AS CDI
, CAST(A.TaxNumber AS STRING) AS TaxNumber
, CAST(A.OldExposureKey AS STRING) AS OldExposureKey
, CAST(A.NewExposureKey AS STRING) AS NewExposureKey
, CAST(A.ModificationAmountAfterHaircut AS DECIMAL(19,2)) AS ModificationAmountAfterHaircut
, CAST(A.HaircutAmount AS DECIMAL(19,2)) AS HaircutAmount
, CAST(A.ModificationType AS STRING) AS ModificationType
, CAST(A.ConditionalwoffExposureKey AS STRING) AS ConditionalwoffExposureKey
, CAST(A.SOMBalanceOfPreviousAccountEUR AS DECIMAL(19,2)) AS SOMBalanceOfPreviousAccountEUR
, CAST(A.SOMSourceBucketOfPreviousAccount AS STRING) AS SOMSourceBucketOfPreviousAccount
, CAST(A.SOMAccountStatusOfPreviousAccount AS STRING) AS SOMAccountStatusOfPreviousAccount
, CAST(A.ApplicationKey AS STRING) AS ApplicationKey
, CAST(A.ApplicationDateCreated AS DATE) AS ApplicationDateCreated
, CAST(A.SolutionType AS STRING) AS SolutionType
, CAST(A.FlowSegment AS STRING) AS FlowSegment
, CAST(A.CommunicationChannel AS STRING) AS CommunicationChannel
, CAST(A.ModificationAmountBeforeHaircut AS DECIMAL(19,2)) AS ModificationAmountBeforeHaircut
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM FLDTModifications A
'''
)
final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

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

key_columns = ["NewExposureKey", "OldExposureKey", "ImplementationDate"]
check_table(delivery_table_path, run_date, key_columns, duplicate_check_action="error", empty_check_action="error")

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

FLDTModifications = spark.sql(
f'''
select * from {delivery_table_path}
where BusinessDate = {run_date!r}
'''
)
FLDTModifications.createOrReplaceTempView("FLDTModifications")

# COMMAND ----------

output_path = "/Volumes/"+env_prefix+"-cdh-delivery/repoint/exports/HAPS/FLDTModifications/" 
file_name = "FLDTModifications_" +run_date+ ".csv"
FLDTModifications.coalesce(1).write.mode("append").option("header", True).csv(f"{output_path}") 
rename_and_cleanup_files(output_path, file_name)
