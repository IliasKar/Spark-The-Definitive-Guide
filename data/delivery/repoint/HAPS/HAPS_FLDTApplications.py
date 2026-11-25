# Databricks notebook source
# MAGIC %md
# MAGIC ## 📑 HAPS FLDT Applications
# MAGIC  
# MAGIC #### 🔗 Links
# MAGIC - **[BRD link](https://cepalhellas.atlassian.net/wiki/x/BADFHQ)**  
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
    print("mode is shared")
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

# COMMAND ----------

table_name = 'HAPS_FLDTApplications'

schema = (
"BusinessDate date," 
"PortfolioName string,"
"ApplicationKey string,"
"ApplicationActivationDate date,"
"CDI string,"
"TaxNumber string,"
"AccountNumber string,"
"AssetSegment string,"
"AccountStatusAtApplicationApprovalDate string,"
"TotalLegalClaimAtApplicationApprovalDateEUR decimal(19,2),"
"GBVAtApplicationApprovalDateEUR decimal(19,2),"
"OnBalanceAmountAtApplicationApprovalDateEUR  decimal(19,2),"
"SourceBucketAtApplicationApprovalDate string,"
"Bucket90PlusCategoryAtApplicationApprovalDate string,"
"Bucket60PlusCategoryAtApplicationApprovalDate string,"
"DenouncedFlag boolean,"
"InsertTimeStamp timestamp"
)

# COMMAND ----------

partition_columns = ["BusinessDate"]
delivery_table_path = ""

# ================== Create Delta Table ==================
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
    run_date  # type: ignore
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### fldtapplications

# COMMAND ----------

FLDTApplications = spark.sql(
f'''
SELECT 
    app.unifiedLoan_SpvDescription		
,	app.app_ApplicationKey					
,	cast(app.ApplicationActivationDate as date) AS ApplicationActivationDate
,	app.customer_VATNumber					
,	app.unifiedLoan_AccountNumberWithPrefix	
FROM `{env_prefix}-cdh-delivery`.repoint.haps_fldtallapplications AS app
WHERE app.BusinessDate = {run_date!r}
AND app.ActivatedApplicationFlag = 1;
'''
)
FLDTApplications.createOrReplaceTempView("FLDTApplications")

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
WHERE A.Report = 'HAPS_FLDTApplications'
AND A.IsActive = 1 
''' 
)
haps_portfolios.createOrReplaceTempView("haps_portfolios")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT P.BusinessDate, P.PortfolioName, P.AssignmentDate
FROM `{env_prefix}-cdh`.primary.portfolio P
INNER JOIN haps_portfolios HP ON P.PortfolioName = HP.PortfolioName 
WHERE P.BusinessDate = {run_date!r}
'''
)
portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

RefDates = spark.sql(
f'''
SELECT 
     MAX(cal.CalendarDate) AS ReferenceDate
    ,cal.LastDateOfMonth	
FROM `{env_prefix}-cdh`.reference.ref_calendar AS cal
INNER JOIN FLDTApplications AS app on LAST_DAY(DATE_ADD(MONTH, -1, app.ApplicationActivationDate)) = cal.LastDateOfMonth
WHERE cal.IsBusinessDay = 1
AND cal.CalendarDate <= {run_date!r}
GROUP BY cal.LastDateOfMonth
UNION 
SELECT 
     cal.CalendarDate AS ReferenceDate
    ,cal.LastDateOfMonth	
FROM `{env_prefix}-cdh`.reference.ref_calendar AS cal
WHERE cal.CalendarDate = {run_date!r}
UNION 
SELECT 
     MAX(cal.CalendarDate) AS ReferenceDate
    ,cal.LastDateOfMonth	
FROM `{env_prefix}-cdh`.reference.ref_calendar AS cal
INNER JOIN portfolio p on LAST_DAY(p.AssignmentDate) = cal.LastDateOfMonth
WHERE cal.IsBusinessDay = 1
AND cal.CalendarDate <= {run_date!r}
GROUP BY cal.LastDateOfMonth
'''
)
RefDates.createOrReplaceTempView("RefDates")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dexposure

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
    , A.DaysPastDueWithMateriality
    , A.Currency
    , A.IsActiveFlag
    , A.DaysPastDueFinal 
    , A.Opendate
    , p.AssignmentDate 
FROM `{env_prefix}-cdh`.derivative.dexposure as A
INNER JOIN portfolio p on p.PortfolioName = A.PortfolioName
INNER JOIN RefDates ref on A.BusinessDate = ref.ReferenceDate
WHERE A.IsActiveFlag = true
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
    , A.DaysPastDueWithMateriality
    , A.Currency
    , A.IsActiveFlag
    , A.DaysPastDueFinal 
    , A.Opendate
    , p.AssignmentDate     
from (select e.*
      , rank() over (partition by e.ExposureKey order by e.BusinessDate desc) as Rank
      from `{env_prefix}-cdh`.derivative.dexposure e
      inner join portfolio p on p.PortfolioName = e.PortfolioName
      where e.SourceSystem = 'profits'
        and e.IsActiveFlag = false
        and e.BusinessDate <= {run_date!r} 
        ) AS A
INNER JOIN portfolio p on p.PortfolioName = A.PortfolioName
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

union all

select *
from dexposure_inactive

'''
)
dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

from pyspark.sql.functions import when, col

def get_bucket_from_dpd(dpd):
    return (when(dpd.isNull(), None)
            .when(dpd <= 0, 0)
            .when((dpd >= 1) & (dpd <= 29), 1)
            .when((dpd >= 30) & (dpd <= 59), 2)
            .when((dpd >= 60) & (dpd <= 89), 3)
            .when((dpd >= 90) & (dpd <= 119), 4)
            .when((dpd >= 120) & (dpd <= 149), 5)
            .when((dpd >= 150) & (dpd <= 179), 6)
            .when((dpd >= 180) & (dpd <= 209), 7)
            .when((dpd >= 210) & (dpd <= 239), 8)
            .when((dpd >= 240) & (dpd <= 269), 9)
            .when((dpd >= 270) & (dpd <= 299), 10) 
            .when((dpd >= 300) & (dpd <= 329), 11)
            .when((dpd >= 330) & (dpd <= 359), 12)
            .when((dpd >= 360) & (dpd <= 720), 13)
            .when(dpd > 720, 14)
            .otherwise(None))

dexposure = dexposure.withColumn("SourceBucket", get_bucket_from_dpd(col("DaysPastDueWithMateriality")))
dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FLDTApplications (Part_1 / Part_2 / Part_3)

# COMMAND ----------

FLDTApplications_Part1 = spark.sql(
f'''
SELECT DISTINCT 
        {run_date!r} AS `BusinessDate`
    ,	acc.PortfolioName
    ,	app.app_ApplicationKey AS `ApplicationKey`
    ,	app.ApplicationActivationDate AS `ApplicationActivationDate` 
    ,	acc.CDI
    ,	app.customer_VATNumber AS `TaxNumber`
    ,	app.unifiedLoan_AccountNumberWithPrefix	 AS `AccountNumber`
    ,	acc.AssetSegment
    ,	acc.ExposureStatus AS `AccountStatusAtApplicationApprovalDate`
    ,   acc.TotalLegalClaimEUR AS `TotalLegalClaimAtApplicationApprovalDateEUR`
    ,   acc.GBVEUR AS  `GBVAtApplicationApprovalDateEUR`
    ,   acc.OnBalanceAmountEUR AS `OnBalanceAmountAtApplicationApprovalDateEUR`
    ,	acc.SourceBucket AS `SourceBucketAtApplicationApprovalDate`
    ,	CASE WHEN acc.DaysPastDueWithMateriality >  90 THEN '90+'
             WHEN acc.DaysPastDueWithMateriality <= 90 THEN '90-'
        END	AS `Bucket90PlusCategoryAtApplicationApprovalDate`	
    ,	CASE WHEN acc.DaysPastDueWithMateriality > 60 THEN '60+'
             WHEN acc.DaysPastDueWithMateriality <= 60 THEN '60-'
        END	AS `Bucket60PlusCategoryAtApplicationApprovalDate`
    ,	CASE WHEN acc.ExposureStatus IN ('Define Delay', 'Writen Off') THEN 1 ELSE 0 END	AS `DenouncedFlag`
    FROM FLDTApplications AS app
    INNER JOIN dexposure AS acc ON app.unifiedLoan_AccountNumberWithPrefix = acc.ExposureKey 
                              AND LAST_DAY(DATE_ADD(MONTH, -1, app.ApplicationActivationDate)) = last_day(acc.BusinessDate)


    WHERE app.ApplicationActivationDate <= {run_date!r}
        AND last_day(app.ApplicationActivationDate) > last_day(acc.AssignmentDate)
'''
)
FLDTApplications_Part1.createOrReplaceTempView("FLDTApplications_Part1")

# COMMAND ----------

FLDTApplications_Part2 = spark.sql(
f'''
SELECT DISTINCT 
        acc.BusinessDate
    ,	acc.PortfolioName
    ,	app.app_ApplicationKey AS `ApplicationKey`
    ,	app.ApplicationActivationDate AS `ApplicationActivationDate`
    ,	acc.CDI
    ,	app.customer_VATNumber AS `TaxNumber`
    ,	app.unifiedLoan_AccountNumberWithPrefix AS `AccountNumber`
    ,	acc.AssetSegment
    ,	acc.ExposureStatus AS `AccountStatusAtApplicationApprovalDate`
    ,   acc.TotalLegalClaimEUR AS `TotalLegalClaimAtApplicationApprovalDateEUR`
    ,   acc.GBVEUR AS  `GBVAtApplicationApprovalDateEUR`
    ,   acc.OnBalanceAmountEUR AS `OnBalanceAmountAtApplicationApprovalDateEUR`
    ,	acc.SourceBucket AS `SourceBucketAtApplicationApprovalDate`
    ,	CASE WHEN acc.DaysPastDueWithMateriality >  90 THEN '90+'
             WHEN acc.DaysPastDueWithMateriality <= 90 THEN '90-'
        END AS `Bucket90PlusCategoryAtApplicationApprovalDate`	
    ,	CASE WHEN acc.DaysPastDueWithMateriality > 60 THEN '60+'  
             WHEN acc.DaysPastDueWithMateriality <= 60 THEN '60-'
        END AS `Bucket60PlusCategoryAtApplicationApprovalDate`
    ,	CASE WHEN acc.ExposureStatus IN ('Define Delay', 'Writen Off') THEN 1
                                                                       ELSE 0
        END AS `DenouncedFlag`
FROM FLDTApplications AS app
INNER JOIN dexposure AS acc
        ON app.unifiedLoan_AccountNumberWithPrefix = acc.ExposureKey 
WHERE app.ApplicationActivationDate <= {run_date!r}
AND last_day(app.ApplicationActivationDate) > last_day(acc.AssignmentDate)
AND last_day(app.ApplicationActivationDate) = last_day(acc.OpenDate)
AND acc.BusinessDate = {run_date!r}		
'''
)
FLDTApplications_Part2.createOrReplaceTempView("FLDTApplications_Part2")

# COMMAND ----------

FLDTApplications_Part3 = spark.sql(
f'''
SELECT DISTINCT
        {run_date!r} AS `BusinessDate`
    ,	acc.PortfolioName
    ,	app.app_ApplicationKey AS `ApplicationKey`
    ,	app.ApplicationActivationDate AS `ApplicationActivationDate` 
    ,	acc.CDI
    ,	app.customer_VATNumber AS `TaxNumber`
    ,	app.unifiedLoan_AccountNumberWithPrefix AS `AccountNumber`
    ,	acc.AssetSegment
    ,	acc.ExposureStatus AS `AccountStatusAtApplicationApprovalDate`
    ,   acc.TotalLegalClaimEUR AS `TotalLegalClaimAtApplicationApprovalDateEUR`
    ,   acc.GBVEUR AS  `GBVAtApplicationApprovalDateEUR`
    ,   acc.OnBalanceAmountEUR AS `OnBalanceAmountAtApplicationApprovalDateEUR`
    ,	acc.SourceBucket AS `SourceBucketAtApplicationApprovalDate`
    ,	CASE WHEN acc.DaysPastDueWithMateriality >  90 THEN '90+'
             WHEN acc.DaysPastDueWithMateriality <= 90 THEN '90-'
        END AS `Bucket90PlusCategoryAtApplicationApprovalDate`	
    ,	CASE WHEN acc.DaysPastDueWithMateriality > 60 THEN '60+'
             WHEN acc.DaysPastDueWithMateriality <= 60 THEN '60-'
        END AS `Bucket60PlusCategoryAtApplicationApprovalDate`
    ,	CASE WHEN acc.ExposureStatus IN ('Define Delay', 'Writen Off') THEN 1
                                                                           ELSE 0
        END AS `DenouncedFlag`
FROM FLDTApplications AS app
INNER JOIN dexposure AS acc 
        ON app.unifiedLoan_AccountNumberWithPrefix = acc.ExposureKey 
WHERE last_day(app.ApplicationActivationDate) = last_day(acc.AssignmentDate)
AND acc.BusinessDate = last_day(acc.AssignmentDate)
'''
)
FLDTApplications_Part3.createOrReplaceTempView("FLDTApplications_Part3")

# COMMAND ----------

FLDTApplications_Total = spark.sql(
f'''
SELECT * 
FROM (
  SELECT *
  FROM FLDTApplications_Part1

  UNION ALL

  SELECT *
  FROM FLDTApplications_Part2

  UNION ALL
  
  SELECT *
  FROM FLDTApplications_Part3
) A
'''
)
FLDTApplications_Total.createOrReplaceTempView("FLDTApplications_Total")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Final_df
# MAGIC

# COMMAND ----------

final_df = spark.sql(
f'''
SELECT 
  CAST(A.BusinessDate AS DATE) AS BusinessDate
, CAST(A.PortfolioName AS STRING) AS PortfolioName
, CAST(A.ApplicationKey AS STRING) ApplicationKey 
, CAST(A.ApplicationActivationDate AS DATE) AS ApplicationActivationDate
, CAST(A.CDI AS STRING) AS CDI
, CAST(A.TaxNumber AS STRING) AS TaxNumber
, CAST(A.AccountNumber AS STRING ) AccountNumber
, CAST(A.AssetSegment AS STRING ) AssetSegment
, CAST(A.AccountStatusAtApplicationApprovalDate AS STRING ) AccountStatusAtApplicationApprovalDate
, CAST(A.TotalLegalClaimAtApplicationApprovalDateEUR AS DECIMAL(19,2)) AS TotalLegalClaimAtApplicationApprovalDateEUR
, CAST(A.GBVAtApplicationApprovalDateEUR AS DECIMAL(19,2)) AS GBVAtApplicationApprovalDateEUR
, CAST(A.OnBalanceAmountAtApplicationApprovalDateEUR AS DECIMAL(19,2)) AS OnBalanceAmountAtApplicationApprovalDateEUR
, CAST(A.SourceBucketAtApplicationApprovalDate AS STRING) AS SourceBucketAtApplicationApprovalDate
, CAST(A.Bucket90PlusCategoryAtApplicationApprovalDate AS STRING) AS Bucket90PlusCategoryAtApplicationApprovalDate
, CAST(A.Bucket60PlusCategoryAtApplicationApprovalDate AS STRING) AS Bucket60PlusCategoryAtApplicationApprovalDate
, CAST(A.DenouncedFlag AS BOOLEAN) AS DenouncedFlag
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM FLDTApplications_Total A
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

key_columns = ["AccountNumber", "ApplicationKey"]
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

fldtApplications = spark.sql(
f'''
select * from {delivery_table_path}
where BusinessDate = {run_date!r}
'''
)
fldtApplications.createOrReplaceTempView("fldtApplications")

# COMMAND ----------

output_path = "/Volumes/"+env_prefix+"-cdh-delivery/repoint/exports/HAPS/FLDTApplications/" 
file_name = "FLDTApplications_" +run_date+ ".csv"
fldtApplications.coalesce(1).write.mode("append").option("header", True).csv(f"{output_path}") 
rename_and_cleanup_files(output_path, file_name)
