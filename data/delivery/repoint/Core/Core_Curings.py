# Databricks notebook source
# MAGIC %md
# MAGIC ## 📑 Core Curings
# MAGIC  
# MAGIC #### 🔗 Links
# MAGIC - **[BRD link](https://cepalhellas.atlassian.net/wiki/x/AoBLGQ)**  
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
 
#dbutils.widgets.text("env_prefix","p")
#env_prefix = dbutils.widgets.get("env_prefix")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Schema of the table

# COMMAND ----------

# Define the schema name
schema_name = f"`{env_prefix}-cdh-delivery`.repoint"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

table_name = 'Core_Curings'

schema = (
"BusinessDate date," 
"ExposureNumber string,"
"IFRSGrossCarryingAmount decimal(19,2),"
"BucketGroup string,"
"DenouncementIndicator string,"
"AssetSegment string,"
"DelAmount string,"
"MLThreshold string,"
"AnalyticSalesPool string,"
"OperationalBucket string,"
"Boarded string,"
"ProbDateExit int,"
"SnapshotMonth int,"
"ProbDateStart string,"
"Covid19Flag string,"
"UTP12month string,"
"UTPindAlter string,"
"UTPReason string,"
"UTPTypeCode string,"
"Month string,"
"ModificationType string,"
"NotAllowToCure string,"
"ModificationTypeUp string,"
"ModificationTypeGrouped string,"
"NotAllowToCureDesc string,"
"NotAllowerTocureDes string,"
"SSMCore string,"
"NonCoreFlag string,"
"ExitMonthGroupedNumber string,"
"SegmentInitial string,"
"Segment string,"
"SegmentAnalytic string,"
"ActualCured string,"
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
    run_date,  # type: ignore
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dexposure

# COMMAND ----------

dexposure = spark.sql(
f'''
SELECT dex.*, dp.CASE_BUCKET as CaseBucket
    from `{env_prefix}-cdh`.derivative.dexposure dex 
    left join `{env_prefix}-cdh`.landed.repointqcab_daily_portfolio dp on dex.ExposureKey = dp.Account_Number and dex.BusinessDate = dp.BusinessDate
    WHERE dex.PortfolioName = 'Core Retail' 
    and dex.BusinessDate = {run_date!r} 
'''
)   
dexposure.createOrReplaceTempView("dexposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Modifications

# COMMAND ----------

Core_Modifications = spark.sql(
f'''
SELECT 
		       ModificationDate
             , NewExposureKey
             , Category
			 , RANK() OVER(PARTITION BY ModificationDate ,OldExposureKey,NewExposureKey ORDER BY BusinessDate DESC) AS Ranking
            from `{env_prefix}-cdh-delivery`.repoint.core_modifications
			WHERE ModificationDate > (SELECT MAX(RestructuringDate) FROM `{env_prefix}-cdh`.reference.corestatichistoricalmodificationbeforemigration)
'''
)   
Core_Modifications.createOrReplaceTempView("Core_Modifications")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### corestatichistoricalmodificationbeforemigration

# COMMAND ----------

corestatichistoricalmodificationbeforemigration = spark.sql(
f'''
select RestructuringDate as ModificationDate 
,NewExposureKey
, Category
,1 as Ranking
from `{env_prefix}-cdh`.reference.corestatichistoricalmodificationbeforemigration
'''
)   
corestatichistoricalmodificationbeforemigration.createOrReplaceTempView("corestatichistoricalmodificationbeforemigration")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Modifications

# COMMAND ----------

Modifications = spark.sql(
f'''
select distinct ModificationDate, NewExposureKey, Category, RANK() OVER (PARTITION BY NewExposureKey ORDER BY ModificationDate DESC,Category DESC) AS Rank
 from 
 (select * from Core_Modifications 
  union 
  select * from corestatichistoricalmodificationbeforemigration )
  AS a
		WHERE a.Ranking =1
'''
)   
Modifications.createOrReplaceTempView("Modifications")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UnionFile

# COMMAND ----------

UnionFile = spark.sql(
f'''
select * from `{env_prefix}-cdh`.externalfiles.extunionfile
            WHERE SnapshotDate = last_day(add_months({run_date!r},-1))
		    AND EBAStatus = 'FNPL' 
            AND (HFSFlag = 0 OR HFSFlag IS NULL) 
            AND ExposureSegmentation > 3
			and ingestiontimestamp = (Select Max(ingestiontimestamp) from `{env_prefix}-cdh`.externalfiles.extunionfile where SnapshotDate = LAST_DAY(ADD_MONTHS({run_date!r}, -1))) 
''')   
UnionFile.createOrReplaceTempView("UnionFile")

previous_month_last_day = spark.sql(f"SELECT last_day(add_months({run_date!r}, -1)) AS date").collect()[0]['date']
# Check if the dataset is empty and print a message accordingly
if UnionFile.count() == 0:
    print(f"The required external file extunionfile dataset for date {previous_month_last_day!r} is empty")

# COMMAND ----------

# EoMUnion = spark.sql(
# f'''
# select * from `{env_prefix}-cdh`.externalfiles.extunionfile
#             WHERE SnapshotDate = last_day({run_date!r})
# 			AND ingestiontimestamp = (Select Max(ingestiontimestamp) from `{env_prefix}-cdh`.externalfiles.extunionfile where SnapshotDate = LAST_DAY({run_date!r}))
# '''
# )   
# EoMUnion.createOrReplaceTempView("EoMUnion")

# current_month_last_day = spark.sql(f"SELECT last_day(add_months({run_date!r}, -1)) AS date").collect()[0]['date']
# # Check if the dataset is empty and print a message accordingly
# if EoMUnion.count() == 0:
#     print(f"The required external file extunionfile dataset for date {current_month_last_day!r} is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UTP12Month

# COMMAND ----------

UTP12Month = spark.sql(
f'''
SELECT AC_CODE, BusinessDate
FROM `{env_prefix}-cdh`.externalfiles.extutp
WHERE BusinessDate = last_day(add_months({run_date!r}, -1))
AND IngestionTimeStamp = (SELECT MAX(IngestionTimeStamp) IngestionTimeStamp FROM `{env_prefix}-cdh`.externalfiles.extutp WHERE BusinessDate = last_day(add_months({run_date!r}, -1)))
GROUP BY AC_CODE,BusinessDate
HAVING MIN(coalesce(UTP_DRTN_CLCN, 0)) = 12
'''
)
UTP12Month.createOrReplaceTempView("UTP12Month")

# Check if the dataset is empty and print a message accordingly
if UTP12Month.count() == 0:
    print(f"The required external file extutp dataset for date {previous_month_last_day!r} is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UTP_Mapping

# COMMAND ----------

UTP_Mapping = spark.sql(
f'''
 SELECT '1' AS index, '01' AS UTP_ID, 'UTP_DR' AS UTP_IND, 'Ρύθμιση με NPV Loss > 1%' AS `Επεξήγηση`, '' AS hard
        UNION 
		SELECT '2', '02', 'UTP_Balloon', 'Ρύθμιση με NPV Loss <1% και Balloon Payment (Last payment / Balance at forbearance date > 20%)', ''
        UNION 
		SELECT '3', '03', 'UTP_Grace_Period', 'Ρύθμιση με NPV Loss <1% και περίοδο χάρος (= μη καταβολή κεφαλαίου για πάνω από ένα έτος)', ''
        UNION 
		SELECT '4', '04', 'UTP_FonF', 'Ρύθμιση με NPV Loss <1% η οποία είναι η 3η εντός του τελευταίου 12μήνου', ''
        UNION 
		SELECT '5', '06', 'UTP_Cured_FPL_30_facility', 'Cured-FPL με υπέρβαση των 30 ημερών', ''
        UNION 
		SELECT '6', '08', 'UTP_Cured_FPL_Reforbearance_facility', 'Cured-FPL με επαναρύθμιση', ''
        UNION 
		SELECT '7', '13', 'UTP_Retail_Bankruptcy_Law', 'Ν. 3869', '1'
        UNION 
		SELECT '8', '14', 'UTP_Debt_Forgiveness', 'Περιορισμός οφειλής - Haircut', ''
        UNION 
		SELECT '9', '21', 'UTP_Denouncement', 'Καταγγελία (βάσει account status)', '1'
        UNION 
		SELECT '10', '30', 'UTP_Restructuring_Rejected', 'Απόρριψη αιτήματος ρύθμισης', '1'
        UNION 
		SELECT '11', '31', 'UTP_Restructuring_Not_Proceeded', 'Υπαναχώρηση πιστούχου μετά την έγκριση του αιτηματος ρύθμισης', ''
        UNION 
		SELECT '12', '36', 'UTP_POCI', 'Derecognition όπου το νέο δάνειο είναι POCI', ''
        UNION 
		SELECT '13', '37', 'UTP_Fraud', 'Επιβεβαιωμένη Απάτη', '1'
        UNION 
		SELECT '14', '38', 'UTP_Deceased', 'Θάνατος Πιστούχου', '1'
        UNION 
		SELECT '15', '49', 'EBA_NPL_facility_forbearance', 'EBA Alignent UTP (εξυπηρετεί την εξίσωση Default=NPE)', ''
        UNION 
		SELECT '16', '50', 'EBA_FPL_facility_90', 'EBA Alignent UTP (εξυπηρετεί την εξίσωση Default=NPE)', ''
        UNION 
		SELECT '17', '51', 'EBA_FPL_facility_UTP', 'EBA Alignent UTP (εξυπηρετεί την εξίσωση Default=NPE)', ''
        UNION 
		SELECT '18', '52', 'EBA_FNPL_facility_forbearance', 'EBA Alignent UTP (εξυπηρετεί την εξίσωση Default=NPE)', ''
        UNION 
		SELECT '19', '54', 'EBA_PL_FPL_facility_forbearance_above90dpd', 'EBA Alignent UTP (εξυπηρετεί την εξίσωση Default=NPE)', ''
'''
)   
UTP_Mapping.createOrReplaceTempView("UTP_Mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CovidEver

# COMMAND ----------

CovidEver = spark.sql(
f'''
 Select AccountCode, Flag from `{env_prefix}-reportinputfiles`.repoint.Core_CovidEver
'''
)   
CovidEver.createOrReplaceTempView("CovidEver")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Final_Step1

# COMMAND ----------

FinalStep1 = spark.sql(
f'''
select ExposureNumber
,SnapshotDate
,cast(replace(IFRSGrossCarryingAmount, ',', '') as decimal(19,2)) as IFRSGrossCarryingAmount
,SecuritizationCode
,CASE WHEN DeNuncementFlag = 1                                                        THEN '9_Denounced'
			        WHEN cast(BucketCategoryWithMateriality as int) >= 4 AND cast(BucketCategoryWithMateriality as int) <= 6 THEN '4-6'
					WHEN cast(BucketCategoryWithMateriality as int) >= 7                                             THEN '7+'
					ELSE CAST(BucketCategoryWithMateriality AS VARCHAR(20))
					END                                                   AS BucketGroup
, DeNuncementFlag as DenouncementIndicator
, CASE WHEN ECAAssetClass = 'Non-revolving Consumer' OR ECAAssetClass = 'Revolving Consumer' THEN 'CL'
			       WHEN ECAAssetClass = 'Residential' THEN 'ML'
				   ELSE 'SB' END AS AssetSegment
, CASE WHEN Cast(UTPIndicator as int)  >= 1 THEN '1'
	   WHEN cast(UTPIndicator as int) = 0  THEN '0'
  END AS UTPind
,replace(DelinquentAmount,',','')                                      AS DelAmount
,CASE WHEN cast(replace(DelinquentAmount,',','') as decimal(19,2)) >= 100 AND ECAAssetClass = 'Residential' THEN '1'
			       ELSE '0' END AS MLThreshold
, NonCoreFlag                                            AS AnalyticSalesPool
, CASE WHEN d.Exposurekey = u.ExposureNumber THEN d.CaseBucket
		           ELSE '0' END                                           AS OperationalBucket
,CASE WHEN IsActiveFlag = '1' THEN '1'
		           ELSE '0' END                                           AS Boarded
, date_format(DATEADD(YEAR,1,u.ProbationStartDate),'yyyyMM')   AS ProbDateExit
, date_format(u.SnapshotDate,'yyyyMM')                          AS SnapshotMonth
, date_format(u.ProbationStartDate,'yyyyMM')                   AS ProbDateStart
, CASE WHEN c.AccountCode = u.ExposureKey THEN c.FLAG ELSE NULL END AS Covid19Flag
, CASE WHEN u12.AC_CODE = u.ExposureKey THEN '12'
				     ELSE '0' END                                           AS UTP12month
, CASE WHEN u12.AC_CODE = u.ExposureKey THEN '0'
			       WHEN cast(u.UTPIndicator as int) >= 1 THEN '1'
				   ELSE '0' END                                           AS UTPindAlter
, CASE WHEN u12.AC_CODE = u.ExposureKey  THEN 'Περιο12'
			       WHEN um.UTP_ID = u.UTPIndicator THEN um.`Επεξήγηση`
				   END                                                    AS UTPReason
,u.UTPIndicator as UTPTypeCode
, date_format(m.ModificationDate,'yyyyMM')                       AS Month
, m.Category                                               AS ModificationType
from UnionFile u
Left Join dexposure d on d.ExposureKey = u.ExposureNumber
LEFT JOIN CovidEver c ON c.AccountCode = u.ExposureKey
left Join Modifications m on m.NewExposureKey = u.ExposureNumber and m.Rank = 1
Left Join UTP_Mapping um on cast(um.UTP_ID as int) = cast(u.UTPIndicator as int)
left join UTP12Month u12 on u12.AC_CODE = u.Exposurekey 
'''
)   
FinalStep1.createOrReplaceTempView("FinalStep1")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### RestructureType

# COMMAND ----------

RestructureType = spark.sql(
f'''
SELECT '1' AS index, 'A1' AS ModificationType, '1' AS NotAllowToCure, 'A1' AS ModificationTypeUp, 'A' AS ModificationTypeGrouped, 'Whole Product' AS NotAllowToCureDesc
        UNION 
		SELECT '2', 'A1 AC', '1', 'A1', 'A', 'Whole Product'
        UNION 
		SELECT '3', 'A2', '1', 'A2', 'A', 'Whole Product'
        UNION 
		SELECT '4', 'A2 AC', '1', 'A2', 'A', 'Whole Product'
        UNION 
		SELECT '5', 'A3', '0', 'A3', 'A', ''
        UNION 
		SELECT '6', 'A3 AC', '0', 'A3', 'A', ''
        UNION 
		SELECT '7', 'B1', '0', 'B1', 'B', ''
        UNION 
		SELECT '8', 'B1 AC', '0', 'B1', 'B', ''
        UNION 
		SELECT '9', 'B1 N.4605', '0', 'B1', 'B', ''
        UNION 
		SELECT '10', 'B2', '2', 'B2', 'B', '2% of  capital'
        UNION 
		SELECT '11', 'B2 AC', '2', 'B2', 'B', '2% of  capital'
        UNION 
		SELECT '12', 'B3', '1', 'B3', 'B', 'Whole Product'
        UNION 
		SELECT '13', 'B3 AC', '1', 'B3', 'B', 'Whole Product'
        UNION 
		SELECT '14', 'B4 AC SPL', '0', 'B4', 'B', ''
        UNION 
		SELECT '15', 'B4 SPL', '0', 'B4', 'B', ''
        UNION 
		SELECT '16', 'B6', '2', 'B6', 'B', '2% of  capital'
        UNION 
		SELECT '17', 'B6 AC', '2', 'B6', 'B', '2% of  capital'
        UNION 
		SELECT '18', 'B7', '0', 'B7', 'B', ''
        UNION 
		SELECT '19', 'C1', '0', 'C1', 'C', ''
        UNION 
		SELECT '20', 'C2 AC SPL', '0', 'C2', 'C', ''
        UNION 
		SELECT '21', 'C2 SPL', '0', 'C2', 'C', ''
        UNION 
		SELECT '22', 'C3', '0', 'C3', 'C', ''
        UNION 
		SELECT '23', 'C3 AC', '0', 'C3', 'C', ''
        UNION 
		SELECT '24', 'C3 AC SPL', '0', 'C3', 'C', ''
        UNION 
		SELECT '25', 'C3 SPL', '0', 'C3', 'C', ''
        UNION 
		SELECT '26', 'C4', '0', 'C4', 'C', ''
        UNION 
		SELECT '27', 'C4 AC', '0', 'C4', 'C', ''
        UNION 
		SELECT '28', 'C4 AC SPL', '0', 'C4', 'C', ''
        UNION 
		SELECT '29', 'C4 N.3869 ATOKO', '0', 'C4', 'C', ''
        UNION 
		SELECT '30', 'C4 N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΑΤΟΚΟ', '0', 'C4', 'C', ''
        UNION 
		SELECT '31', 'C4 N.4605', '0', 'C4', 'C', ''
        UNION 
		SELECT '32', 'C4 SPL', '0', 'C4', 'C', ''
        UNION 
		SELECT '33', 'C4 ΔΠΑ N.3869', '0', 'C4', 'C', ''
        UNION 
		SELECT '34', 'C4 ΔΠΑ N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '0', 'C4', 'C', ''
        UNION 
		SELECT '35', 'C4 ΔΠΑ N.3869 ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '0', 'C4', 'C', ''
        UNION 
		SELECT '36', 'C4 ΔΠΑ N.3869 ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ ΔΙΑΤΡΑΠΕΖΙΚΗ', '0', 'C4', 'C', ''
        UNION 
		SELECT '37', 'C5', '0', 'C5', 'C', ''
        UNION 
		SELECT '38', 'C5 AC', '0', 'C5', 'C', ''
        UNION 
		SELECT '39', 'C5 N.3869', '0', 'C5', 'C', ''
        UNION 
		SELECT '40', 'C5 N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '0', 'C5', 'C', ''
        UNION 
		SELECT '41', 'C5 N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '0', 'C5', 'C', ''
        UNION 
		SELECT '42', 'D1', '0', 'D1', 'D', ''
        UNION 
		SELECT '43', 'D1 AC', '0', 'D1', 'D', ''
        UNION 
		SELECT '44', 'D1 Campaign', '0', 'D1', 'D', ''
        UNION 
		SELECT '45', 'D1 N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΕΝΑΡΞΗ', '0', 'D1', 'D', ''
        UNION 
		SELECT '46', 'D1 N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΛΗΞΗ', '0', 'D1', 'D', ''
        UNION 
		SELECT '47', 'D1 Small Ticket', '0', 'D1', 'D', ''
        UNION 
		SELECT '48', 'D2', '0', 'D2', 'D', ''
        UNION 
		SELECT '49', 'Fast track', '0', 'Fast track', 'Fast track', ''
        UNION 
		SELECT '49', 'Fast Track', '0', 'Fast track', 'Fast track', ''
        UNION 
		SELECT '50', 'Fast Track CMS', '0', 'Fast track', 'Fast track', ''
        UNION 
		SELECT '51', 'LT2', '0', 'LT2', 'LT2', ''
        UNION 
		SELECT '52', 'Ρυθμίσεις N3869', '0', 'Ρυθμίσεις N3869', 'Ρυθμίσεις N3869', ''
        UNION 
		SELECT '53', 'Ρύθμιση FT', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '54', 'Ρύθμιση Επιδίκου', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '55', 'Ρύθμιση Ν.3869-Ληξιπρόθ.Οφειλών', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '56', 'Ρύθμιση ολικής κεφαλαιοποίησης καθυστερήσεων', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '57', 'Συμβατικά', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '58', 'Τροποποίηση όρων εξοφλήσεως βάσει ΠΕΕ 47 - 203', '0', 'Συμβατικά', 'Συμβατικά', ''
        UNION 
		SELECT '59', 'Τροποποίηση όρων εξοφλήσεως βάσει ΠΕΕ 47 - 208', '0', 'Συμβατικά', 'Συμβατικά', ''
'''
)   
RestructureType.createOrReplaceTempView("RestructureType")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SPV

# COMMAND ----------

SPV = spark.sql(
f'''
SELECT '1' AS index, '1' AS Securitization_Code, '' AS NonCoreFlag, '' AS SSMflag
              UNION 
			  SELECT '2', '2', '', ''
              UNION 
			  SELECT '3', '3', '', ''
              UNION 
			  SELECT '4', '4', '', ''
              UNION 
			  SELECT '5', '5', '', ''
              UNION 
			  SELECT '6', '6', '', ''
              UNION 
			  SELECT '7', '7', '', ''
              UNION 
			  SELECT '8', '8', '', ''
              UNION 
			  SELECT '9', '9', '0', '0'
              UNION 
			  SELECT '10', '10', '0', '0'
              UNION 
			  SELECT '11', '11', '1', ''
              UNION 
			  SELECT '12', '12', '1', '1'
              UNION 
			  SELECT '13', '14', '1', '1'
'''
)   
SPV.createOrReplaceTempView("SPV")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FinalStep2

# COMMAND ----------

Finalstep2 = spark.sql(
f'''
SELECT 		   f.ExposureNumber
		     , f.SnapshotDate
		     , f.IFRSGrossCarryingAmount
			 , f.BucketGroup
			 , f.DenouncementIndicator
			 , f.AssetSegment
			 , f.DelAmount
			 , f.MLThreshold
			 , f.AnalyticSalesPool
			 , f.OperationalBucket
			 , f.Boarded
			 , CAST(f.ProbDateExit AS INT)  AS ProbDateExit
			 , CAST(f.SnapshotMonth AS INT) AS SnapshotMonth
			 , f.ProbDateStart
			 , f.Covid19Flag
			 , f.UTP12month
			 , f.UTPindAlter
			 , f.UTPReason
			 , f.Month
			 , f.UTPTypeCode
			 , f.ModificationType
			 , CASE WHEN r.ModificationType = f.ModificationType THEN r.NotAllowToCure
			        END AS NotAllowToCure
			 , CASE WHEN r.ModificationType = f.ModificationType THEN r.ModificationTypeUp		
			        END AS ModificationTypeUp
			 , CASE WHEN r.ModificationType = f.ModificationType THEN r.ModificationTypeGrouped	
			        END AS ModificationTypeGrouped
			 , CASE WHEN r.ModificationType = f.ModificationType THEN r.NotAllowToCureDesc
			        END AS NotAllowToCureDesc
			 , CASE WHEN r.ModificationType = f.ModificationType AND r.NotAllowToCureDesc = 'Whole Product' THEN '1'
			        ELSE '0' END AS NotAllowerTocureDes
             , CASE WHEN s.Securitization_Code = f.SecuritizationCode THEN s.SSMflag
			        ELSE '0' END AS SSMCore
             , CASE WHEN s.Securitization_Code = f.SecuritizationCode THEN s.NonCoreFlag
			        END AS NonCoreFlag
        FROM Finalstep1    f
		LEFT JOIN RestructureType r
		ON r.ModificationType = f.ModificationType
		LEFT JOIN SPV           s
		ON s.Securitization_Code = f.SecuritizationCode

'''
)   
Finalstep2.createOrReplaceTempView("Finalstep2")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SpvAnalytic

# COMMAND ----------


SpvAnalytic = spark.sql(
f'''
SELECT '1' AS index, 'Orbit_top_up' AS AnalyticSalesPool, 'Cosmos' AS Groups, '2' AS Number
              UNION 
			  SELECT '2', 'Cosmos', 'Cosmos', '2'
              UNION 
			  SELECT '3', 'Orbit_2nd_top_up', 'Cosmos', '2'
              UNION 
			  SELECT '4', '', '', '1'
              UNION 
			  SELECT '5', 'Cosmos I', 'Cosmos', '2'
              UNION 
			  SELECT '6', 'Cosmos II', 'Cosmos', '2'
              UNION 
			  SELECT '7', 'Orbit top-up', 'Cosmos', '2'
              UNION 
			  SELECT '8', 'SPV I', 'Galaxy', '1'
              UNION 
			  SELECT '9', 'SPV II', 'Galaxy', '1'
              UNION 
			  SELECT '10', 'SPV III', 'Cosmos', '2'
              UNION 
			  SELECT '11', 'SPV IV', 'Galaxy', '1'
'''
)   
SpvAnalytic.createOrReplaceTempView("SpvAnalytic")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Finalstep3

# COMMAND ----------


Finalstep3 = spark.sql(
f'''
SELECT 		   ExposureNumber	
		     , IFRSGrossCarryingAmount
			 , SnapshotDate
			 , BucketGroup
			 , DenouncementIndicator
			 , AssetSegment
			 , DelAmount
			 , MLThreshold
			 , f.AnalyticSalesPool
			 , OperationalBucket
			 , Boarded
			 , ProbDateExit
			 , SnapshotMonth
			 , ProbDateStart
			 , Covid19Flag
			 , UTP12month
			 , UTPindAlter
			 , UTPReason
			 , UTPTypeCode
			 , Month
			 , ModificationType
			 , NotAllowToCure
			 , ModificationTypeUp
			 , ModificationTypeGrouped
			 , NotAllowToCureDesc
			 , NotAllowerTocureDes
             , SSMCore
             , NonCoreFlag
			 , CASE WHEN UTPindAlter = 1                               THEN 'UTP'
			        WHEN UTPindAlter = 0 AND NotAllowerTocureDes = 1 THEN 'Not Allowed To Cure'
			        WHEN ProbDateExit <= SnapshotMonth               THEN 'Expired'
					WHEN ProbDateExit - SnapshotMonth > 100          THEN '999999'
					ELSE CAST(ProbDateExit AS VARCHAR(20))
					                                                              END AS ExitMonthGroupedNumber
			 , CONCAT(AssetSegment,' & ',BucketGroup,' & ',OperationalBucket)               AS SegmentInitial
			 , CASE WHEN OperationalBucket >= 2 THEN CONCAT(AssetSegment,' & ',BucketGroup,' & ','2')
			        ELSE CONCAT(AssetSegment,' & ',BucketGroup,' & ',OperationalBucket)       END AS Segment
			 , CASE WHEN x.AnalyticSalesPool = f.AnalyticSalesPool THEN x.Groups  END AS Groups
        FROM Finalstep2                  f
		LEFT JOIN SpvAnalytic x
		ON x.AnalyticSalesPool = f.AnalyticSalesPool
'''
)   
Finalstep3.createOrReplaceTempView("Finalstep3")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Curings

# COMMAND ----------


Curings = spark.sql(
f'''
SELECT   f.ExposureNumber
       , f.IFRSGrossCarryingAmount
       , f.BucketGroup
       , f.DenouncementIndicator
       , f.AssetSegment
       , f.DelAmount
       , f.MLThreshold
       , f.AnalyticSalesPool
       , f.OperationalBucket
       , f.Boarded
       , f.ProbDateExit
       , f.SnapshotMonth
       , f.ProbDateStart
       , f.Covid19Flag
       , f.UTP12month
       , f.UTPindAlter
       , f.UTPReason
       ,  f.UTPTypeCode
       , f.Month
       , f.ModificationType
       , f.NotAllowToCure
       , f.ModificationTypeUp
       , f.ModificationTypeGrouped
       , f.NotAllowToCureDesc
       , f.NotAllowerTocureDes
      , f.SSMCore
      , f.NonCoreFlag
       , f.ExitMonthGroupedNumber
       , f.SegmentInitial
       , f.Segment
       , CONCAT(f.Groups,' & ',f.Segment) AS SegmentAnalytic
       ,CASE when u.EBAStatus = 'FPL' and u.FPLUnderProbationFlag = '1'  THEN 'Actual Cured'  else '' END as ActualCured
        FROM Finalstep3 f 
        LEFT JOIN UnionFile u on u.ExposureNumber =f.ExposureNumber        
'''
)   
Curings.createOrReplaceTempView("Curings")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### final_df

# COMMAND ----------

final_df = spark.sql(
f'''
SELECT 
  CAST({run_date!r} AS date) AS BusinessDate
, CAST(ExposureNumber AS string) AS ExposureNumber
, CAST(IFRSGrossCarryingAmount AS decimal(19,2)) AS IFRSGrossCarryingAmount
, CAST(BucketGroup AS string) AS BucketGroup
, CAST(DenouncementIndicator AS string) AS DenouncementIndicator
, CAST(AssetSegment AS string) AS AssetSegment
, CAST(DelAmount AS string) AS DelAmount
, CAST(MLThreshold AS string) AS MLThreshold
, CAST(AnalyticSalesPool AS string) AS AnalyticSalesPool
, CAST(OperationalBucket AS string) AS OperationalBucket
, CAST(Boarded AS string) AS Boarded
, CAST(ProbDateExit AS int) AS ProbDateExit
, CAST(SnapshotMonth AS int) AS SnapshotMonth
, CAST(ProbDateStart AS string) AS ProbDateStart
, CAST(Covid19Flag AS string) AS Covid19Flag
, CAST(UTP12month AS string) AS UTP12month
, CAST(UTPindAlter AS string) AS UTPindAlter
, CAST(UTPReason AS string) AS UTPReason
, CAST(UTPTypeCode AS string) AS UTPTypeCode
, CAST(Month AS string) AS Month
, CAST(ModificationType AS string) AS ModificationType
, CAST(NotAllowToCure AS string) AS NotAllowToCure
, CAST(ModificationTypeUp AS string) AS ModificationTypeUp
, CAST(ModificationTypeGrouped AS string) AS ModificationTypeGrouped
, CAST(NotAllowToCureDesc AS string) AS NotAllowToCureDesc
, CAST(NotAllowerTocureDes AS string) AS NotAllowerTocureDes
, CAST(SSMCore AS string) AS SSMCore
, CAST(NonCoreFlag AS string) AS NonCoreFlag
, CAST(ExitMonthGroupedNumber AS string) AS ExitMonthGroupedNumber
, CAST(SegmentInitial AS string) AS SegmentInitial
, CAST(Segment AS string) AS Segment
, CAST(SegmentAnalytic AS string) AS SegmentAnalytic
, CAST(ActualCured AS string) AS ActualCured
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM Curings
'''
)
final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

if final_df.rdd.isEmpty():
    raise ValueError(f"Core_Curings: final DataFrame is empty - aborting write to {delivery_table_path} to avoid overwriting with no data")

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

key_columns = ["ExposureNumber"]
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

curings = spark.sql(
f'''
select * from {delivery_table_path}
where BusinessDate = {run_date!r}
'''
)
curings.createOrReplaceTempView("curings")

# COMMAND ----------

output_path = "/Volumes/"+env_prefix+"-cdh-delivery/repoint/exports/Core/Curings/"
file_name = "Curings_" +run_date+ ".csv"
curings.coalesce(1).write.mode("append").option("header", True).csv(f"{output_path}")
rename_and_cleanup_files(output_path, file_name)
