# Databricks notebook source
# MAGIC %md
# MAGIC ## 📑 Core NPE Planning Data
# MAGIC  
# MAGIC #### 🔗 Links
# MAGIC - **[BRD link](https://cepalhellas.atlassian.net/wiki/x/BIDiGQ)**  
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

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Schema of the table

# COMMAND ----------

# Define the schema name
schema_name = f"`{env_prefix}-cdh-delivery`.repoint"

# Create the schema if it does not exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

table_name = 'Core_NPEPlanningData'

schema = (
"BusinessDate date," 
"PartyTaxIdentificationNumber string,"
"CDI string,"
"AssetSegment string,"
"ExposureKey string,"
"AgreementKey string,"
"Decision string,"
"DateEvaluation date,"
"MonthEvaluation string,"
"ImplementationMonth string,"
"ModificationType string,"
"DenouncedStatus bigint,"
"NPLIndicator bigint,"
"ApplicationNumber string,"
"BalanceAmount decimal(19,2),"
"AccountBalance decimal(19,2),"
"ModificationTypeFinal string,"
"ChannelFinal string,"
"Status string,"
"ApplicationStep string,"
"ImplementationStep string,"
"ApplicationStepFinal string,"
"BRCStepMapping string,"
"Implementation string,"
"ImplementationDate date,"
"ActiveRequestMIS string,"
"QualcoBucket string,"
"InputChannel string,"
"ApplicationBranch string,"
"Hub string,"
"CFlag string,"
"L3869Flag string,"
"L3869Final string,"
"CoreFlag string,"
"ApplicationAutocredit string,"
"ApplicationStepUpdated string,"
"ApprovalSubRole string,"
"ReceiptDateCurrentStep date,"
"ApplicationSignatureRequired string,"
"ChannelPreFinalRegistration string,"
"ChannelRegistration string,"
"InsertTimeStamp timestamp"
)

table_name_secondary = 'Core_tb1totalapplications'

schema = (
"BusinessDate date,"
"CDI string,"
"AssetSegment string,"
"ExposureKey string,"
"ApplicationType string,"
"Decision string,"
"FirstDateReceiptValuation date,"
"MonthEvaluation string,"
"ApplicationNumber string,"
"TotalOutstandingAmount decimal(19,2),"
"ModificationTypeFinal string,"
"Implementation string,"
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

delivery_table_secondary_path = ""

# ================== Create Delta Table ==================
delivery_table_secondary_path = f"`{env_prefix}-cdh-delivery`.repoint.{table_name_secondary}"
delta_table_creator.create_unity_delta_table(
    spark, delivery_table_secondary_path, partition_columns, schema
)
# if not DeltaTable.isDeltaTable(spark, delivery_table_secondary_path):
#     delta_table_creator.create_unity_delta_table(
#         spark, delivery_table_secondary_path, partition_columns, schema
#     )
  
# ================== Delete From Delta Table ==================
transform.delete_from_unity_table(
    spark,
    delivery_table_secondary_path,
    run_date,  # type: ignore
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LPFXB364_Step0

# COMMAND ----------

LPFXB364_Step0 = spark.sql(
f'''
select distinct 
date_format(to_date(RestructuringDate,'dd/MM/yyy'),'yyyy-MM-dd') as ImplementationDate
,NewExposureKey
,OldExposureKey
,RANK() OVER(PARTITION BY RestructuringDate,OldExposureKey,NewExposureKey  ORDER BY BusinessDate DESC) AS Ranking
    from `{env_prefix}-cdh`.externalfiles.extlpfxb364
    where 1=1 -- BusinessDate = {run_date!r} 
    and date_format(to_date(RestructuringDate,'dd/MM/yyy'),'yyyy-MM-dd') > (SELECT MAX(RestructuringDate) 
                                                                            FROM `{env_prefix}-cdh`.reference.corestatichistoricalmodificationbeforemigration)
                                                                            and RestructuringType not in ('Calypso Proactive Management','ΠΕΕ 47 Wholesale')
                                                                            and RestructuringProductKey NOT IN ('4015', '4016', '6078', '6079', '7363', '7362') 
    UNION
    select distinct 
    date_format(to_date(RestructuringDate,'dd/MM/yyy'),'yyyy-MM-dd') as ImplementationDate
    ,NewExposureKey
    ,OldExposureKey
    ,1 As Ranking
    from `{env_prefix}-cdh`.reference.corestatichistoricalmodificationbeforemigration
    where 1=1 
    AND Category NOT IN ('Fast track','Fast Track', 'Calypso')
    AND RestructuringProductCode NOT IN ('4015', '4016', '6078', '6079', '7363', '7362') 
    and RestructuringType = ''
'''
)

LPFXB364_Step0.createOrReplaceTempView("LPFXB364_Step0")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LPFXB364

# COMMAND ----------

LPFXB364= spark.sql(
f'''
select * from LPFXB364_Step0 where Ranking = 1 
'''
)

LPFXB364.createOrReplaceTempView("LPFXB364")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FinalLPFXB364

# COMMAND ----------

FinalLPFXB364Old= spark.sql(
f'''
SELECT ImplementationDate
      ,OldExposureKey AS ExposureKey
			  ,RANK() OVER(PARTITION BY OldExposureKey ORDER BY ImplementationDate DESC) AS Rank
        FROM LPFXB364
'''
)

FinalLPFXB364Old.createOrReplaceTempView("FinalLPFXB364Old")

# COMMAND ----------

FinalLPFXB364= spark.sql(
f'''
SELECT ImplementationDate
      ,ExposureKey
			,date_format(ImplementationDate,'yyyyMM') AS ImplementationMonth
        FROM FinalLPFXB364Old
        where Rank = 1 
'''
)

FinalLPFXB364.createOrReplaceTempView("FinalLPFXB364")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ReportUnified_step1

# COMMAND ----------

extreport_unified_cl = spark.sql(
f'''		
SELECT *
FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_cl
WHERE BusinessDate = {run_date!r}
AND IngestionTimeStamp = (SELECT MAX(IngestionTimeStamp) IngestionTimeStamp FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_cl WHERE BusinessDate = {run_date!r})
''')
extreport_unified_cl.createOrReplaceTempView("extreport_unified_cl")     

# Check if the dataset is empty and print a message accordingly
if extreport_unified_cl.count() == 0:
    print(f"The required external file extreport_unified_cl dataset for date {run_date!r} is empty")

extreport_unified_ml = spark.sql(
f'''		
SELECT *
FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_ml
WHERE BusinessDate = {run_date!r}
AND IngestionTimeStamp = (SELECT MAX(IngestionTimeStamp) IngestionTimeStamp FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_ml WHERE BusinessDate = {run_date!r})
''')
extreport_unified_ml.createOrReplaceTempView("extreport_unified_ml")     

# Check if the dataset is empty and print a message accordingly
if extreport_unified_ml.count() == 0:
    print(f"The required external file extreport_unified_ml dataset for date {run_date!r} is empty")

extreport_unified_sb = spark.sql(
f'''		
SELECT *
FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_sb
WHERE BusinessDate = {run_date!r}
AND IngestionTimeStamp = (SELECT MAX(IngestionTimeStamp) IngestionTimeStamp FROM `{env_prefix}-cdh`.externalfiles.extreport_unified_sb WHERE BusinessDate = {run_date!r})
''')
extreport_unified_sb.createOrReplaceTempView("extreport_unified_sb")     

# Check if the dataset is empty and print a message accordingly
if extreport_unified_sb.count() == 0:
    print(f"The required external file extreport_unified_sb dataset for date {run_date!r} is empty")



# COMMAND ----------

ReportUnified_temp = spark.sql(f"""
SELECT 'CL' as AssetSegment,
BusinessDate,
PartyTaxIdentificationNumber as PartyTaxIdentificationNumber,
ExposureKey as ExposureKey,
Hraklisagreementkey as AgreementKey,
UnifiedApplicationStatus as UnifiedApplicationStatus,
Decision as Decision,
FirstDateReceiptValuation as FirstDateReceiptValuation,
RestructuringType as RestructuringType,
ApplicationNumber as ApplicationNumber,
cast(ExposureOriginalAmount as decimal(19,2)) as TotalOutstandingAmount,
CommonApplicationStep as CommonApplicationStep,
ImplemenationApplicationStep as ImplemenationApplicationStep,
ApplicationBranch as ApplicationBranch,
Hub as Hub,
L3869Flag as N3869Flag,
Channel as Channel,
ApplicationAutocredit as ApplicationAutocredit,
ApprovalSubRole as ApprovalSubRole,
ReceiptDateCurrentStep as ReceiptDateCurrentStep, 
FirstDateSendDecisionToProcessor as FirstDateSendDecisionToProcessor,
ApplicationSignatureRequired as ApplicationSignatureRequired
FROM extreport_unified_cl

UNION ALL

SELECT 'ML' as AssetSegment,
BusinessDate,
PartyTaxIdentificationNumber as PartyTaxIdentificationNumber,
ExposureKey as ExposureKey,
AgreementKey as AgreementKey,
UnifiedApplicationStatus as UnifiedApplicationStatus,
Decision as Decision,
FirstDateReceiptValuation as FirstDateReceiptValuation,
RestructuringType as RestructuringType,
ApplicationNumber as ApplicationNumber,
cast(Balance as decimal(19,2)) as TotalOutstandingAmount,  --cast(Totaloutstandingamount as decimal(19,2)) as TotalOutstandingAmount,-- IB 26032026
CommonApplicationStep as CommonApplicationStep,
ImplemenationApplicationStep as ImplemenationApplicationStep,
ApplicationBranch as ApplicationBranch,
Hub as Hub,
L3869Flag as N3869Flag,
Channel as Channel,
ApplicationAutocredit as ApplicationAutocredit,
ApprovalSubRole as ApprovalSubRole,
CurrentStepDate as ReceiptDateCurrentStep,
FirstDateSendDecisionToProcessor as FirstDateSendDecisionToProcessor,
ApplicationSignatureRequired as ApplicationSignatureRequired
FROM extreport_unified_ml

UNION ALL

SELECT 'SB' as AssetSegment,
BusinessDate,
PartyTaxIdentificationNumber as PartyTaxIdentificationNumber,
ExposureKey as ExposureKey,
AgreementKey as AgreementKey,
UnifiedApplicationStatus as UnifiedApplicationStatus,
decisionreason as Decision, 
FirstDateReceiptValuation as FirstDateReceiptValuation,
ResrtucturingType as RestructuringType,
ApplicationNumber as ApplicationNumber,
CASE WHEN SuggestedAmount = ' ' THEN NULL ELSE CAST(SuggestedAmount AS DECIMAL(19,2)) END as TotalOutstandingAmount, --cast(Totaloutstandingamount as decimal(19,2)) as TotalOutstandingAmount, -- IB 26032026, FM 16042025
'CommonApplicationStep_SB' as CommonApplicationStep,
ApplicationImplementationFlag as ImplemenationApplicationStep, 
ApplicationBranch as ApplicationBranch,
NPLHubs as Hub,
L3869Flag as N3869Flag,
Channel as Channel,
ApplicationAutocredit as ApplicationAutocredit,
ApprovalSubRole as ApprovalSubRole,
CurrentStepReceiptDate as ReceiptDateCurrentStep,
FirstDateSendDecisionToProcessor as FirstDateSendDecisionToProcessor,
ApplicationSignatureRequired as ApplicationSignatureRequired
FROM extreport_unified_sb
""")

ReportUnified_temp.createOrReplaceTempView("ReportUnified_temp")


# COMMAND ----------

# DBTITLE 1,ReportUnified_step1 (refactored)
ReportUnified_step1 = spark.sql(
f'''
WITH U_clean AS (
select U.*
,CASE WHEN U.FirstDateReceiptValuation IS NULL OR TRIM(U.FirstDateReceiptValuation) = '' OR UPPER(TRIM(U.FirstDateReceiptValuation)) = 'NULL' THEN NULL
      -- If the value matches the format 'YYYY-MM-DD HH:mm:ss.0'
      WHEN SUBSTRING(TRIM(U.FirstDateReceiptValuation), 5, 1) = '-' THEN TRY_CAST(U.FirstDateReceiptValuation AS DATE)
      -- If the value matches the format 'DD/MM/YYYY'
      WHEN SUBSTRING(TRIM(U.FirstDateReceiptValuation), 3, 1) = '/' THEN TO_DATE(U.FirstDateReceiptValuation, 'dd/MM/yyyy')
      ELSE NULL
 END AS FirstDateReceiptValuation_Clean
FROM ReportUnified_temp U
)
select 
U.AssetSegment
,CASE WHEN CONCAT(SUBSTRING('000000000', 1, 10 - LEN(CAST(U.PartyTaxIdentificationNumber AS VARCHAR(10)))), CAST(U.PartyTaxIdentificationNumber AS VARCHAR(10))) = '000000000' then null
      ELSE CONCAT(SUBSTRING('000000000', 1, 10 - LEN(CAST(U.PartyTaxIdentificationNumber AS VARCHAR(10)))), CAST(U.PartyTaxIdentificationNumber AS VARCHAR(10))) END AS PartyTaxIdentificationNumber
,CONCAT(SUBSTRING('000000000', 1, 10 - LEN(CAST(null AS VARCHAR(10)))), CAST(null AS VARCHAR(10))) AS CDI
,U.ExposureKey
,Concat(SUBSTRING('000000000',1,10-LEN(CAST(U.AgreementKey AS VARCHAR(10)))),substring(CAST(U.AgreementKey AS VARCHAR(10)),1,10)) AS AgreementKey
,CASE WHEN U.UnifiedApplicationStatus = 'Ακυρωμένη' THEN '2.ΑΚΥΡΩΣΗ'
      WHEN U.Decision = 'Ναι'                       THEN '4.ΈΓΚΡΙΣΗ'
      WHEN U.Decision = 'Όχι'                       THEN '1.ΑΠΟΡΡΙΨΗ'
      ELSE '3.ΕΚΚΡΕΜΗ' 
  END AS Decision 
-- Already converted to DATE in U_clean
,U.FirstDateReceiptValuation_Clean AS FirstDateReceiptValuation
,U.RestructuringType
,U.ApplicationNumber 
,U.TotalOutstandingAmount
,U.UnifiedApplicationStatus
,U.CommonApplicationStep
,U.ImplemenationApplicationStep
,CASE when (U.ImplemenationApplicationStep is null or trim(U.ImplemenationApplicationStep)='')  then U.CommonApplicationStep
	  else U.ImplemenationApplicationStep 
 end as ApplicationStepUpdated
,U.ApplicationBranch
,U.Hub
,U.N3869Flag
,case when U.ExposureKey = L.ExposureKey and L.ImplementationDate >= U.FirstDateReceiptValuation_Clean and U.decision = 'Ναι' then 'Yes' Else 'No' end As Implementation
,U.Channel
,U.ApplicationAutocredit 
,U.ApprovalSubRole
-- ,cast(case when U.ReceiptDateCurrentStep = 'NULL' then null else U.ReceiptDateCurrentStep end as date) as ReceiptDateCurrentStep
,CASE   when U.ReceiptDateCurrentStep IS NULL or trim(U.ReceiptDateCurrentStep)= '' or UPPER(TRIM(U.ReceiptDateCurrentStep)) = 'NULL' then null
        -- If the value matches the format 'YYYY-MM-DD HH:mm:ss.0'
        WHEN SUBSTRING(TRIM(U.ReceiptDateCurrentStep), 5, 1) = '-' THEN TRY_CAST(U.ReceiptDateCurrentStep AS DATE)
        -- If the value matches the format 'DD/MM/YYYY'
        WHEN SUBSTRING(TRIM(U.ReceiptDateCurrentStep), 3, 1) = '/' THEN TO_DATE(U.ReceiptDateCurrentStep, 'dd/MM/yyyy')
        -- For any other format, assuming it's already in the desired format
    END AS ReceiptDateCurrentStep
-- ,cast(case when U.FirstDateSendDecisionToProcessor = 'NULL' then null else U.FirstDateSendDecisionToProcessor end as date) as FirstDateSendDecisionToProcessor
,CASE   when U.FirstDateSendDecisionToProcessor IS NULL or trim(U.FirstDateSendDecisionToProcessor)= '' or UPPER(TRIM(U.FirstDateSendDecisionToProcessor)) = 'NULL' then null
        -- If the value matches the format 'YYYY-MM-DD HH:mm:ss.0'
        WHEN SUBSTRING(TRIM(U.FirstDateSendDecisionToProcessor), 5, 1) = '-' THEN TRY_CAST(U.FirstDateSendDecisionToProcessor AS DATE)
        -- If the value matches the format 'DD/MM/YYYY'
        WHEN SUBSTRING(TRIM(U.FirstDateSendDecisionToProcessor), 3, 1) = '/' THEN TO_DATE(U.FirstDateSendDecisionToProcessor, 'dd/MM/yyyy')
        -- For any other format, assuming it's already in the desired format
    END AS FirstDateSendDecisionToProcessor
,case when U.ExposureKey = L.ExposureKey and L.ImplementationDate >= U.FirstDateReceiptValuation_Clean and U.decision = 'Ναι' then L.ImplementationDate Else Null end As ImplementationDate
,case when U.ExposureKey = L.ExposureKey and L.ImplementationDate >= U.FirstDateReceiptValuation_Clean and U.decision = 'Ναι' then L.ImplementationMonth Else Null end As ImplementationMonth
,U.ApplicationSignatureRequired
from U_clean U 
left join FinalLPFXB364 L on U.ExposureKey = L.ExposureKey
where U.ApplicationNumber <> ''
and U.TotalOutstandingAmount >0 
and U.BusinessDate = {run_date!r}
'''
)
ReportUnified_step1.createOrReplaceTempView("ReportUnified_step1")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### ReportUnified

# COMMAND ----------


ReportUnified = spark.sql(
    f'''
   SELECT        u.AssetSegment
                ,u.PartyTaxIdentificationNumber
                ,u.CDI
                ,u.ExposureKey
                ,u.AgreementKey
                ,CASE WHEN m.ApplicationNumber = u.ApplicationNumber AND m.ApplicationNumber IS NOT NULL THEN '2.ΑΚΥΡΩΣΗ'
                      WHEN u.Decision = '4.ΈΓΚΡΙΣΗ' AND (u.ApplicationAutocredit = 'Credit Review' OR u.ApplicationAutocredit = 'Credit Agreement') 
                       AND u.FirstDateSendDecisionToProcessor is NULL 
                       AND LEFT(u.ApplicationNumber,6) > 190902  THEN '3.ΕΚΚΡΕΜΗ' 
                      ELSE Decision END AS Decision
                ,u.FirstDateReceiptValuation
                ,u.ImplementationDate
                ,u.ImplementationMonth
                ,u.TotalOutstandingAmount
                ,date_format(u.FirstDateReceiptValuation,'yyyyMM') AS MonthEvaluation
                ,u.RestructuringType
                ,u.ApplicationNumber
                ,u.UnifiedApplicationStatus
                ,u.CommonApplicationStep
                ,u.ImplemenationApplicationStep   
                ,u.ApplicationStepUpdated 
                ,u.ApplicationBranch
                ,u.Hub
                ,u.N3869Flag
                ,u.Implementation
                ,u.Channel
                ,u.ApplicationAutocredit
                ,u.ApprovalSubRole
                ,u.ReceiptDateCurrentStep
                ,u.FirstDateSendDecisionToProcessor
                ,u.ApplicationSignatureRequired
        FROM ReportUnified_step1                                     u
        LEFT JOIN `{env_prefix}-cdh`.reference.core_paymentjumpmasscancelation    m     
        ON m.ApplicationNumber = u.ApplicationNumber
        
'''
)
ReportUnified.createOrReplaceTempView("ReportUnified")



# COMMAND ----------


ReportUnifiedUpdated = spark.sql(
    f'''
   SELECT        AssetSegment
                ,PartyTaxIdentificationNumber
                ,CDI
                ,u.ExposureKey
                ,AgreementKey
                ,Decision
                ,FirstDateReceiptValuation
                ,Case when u.Decision<>'4.ΈΓΚΡΙΣΗ' and  Implementation = 'Yes' then Null  else ImplementationDate end as ImplementationDate
                ,Case when u.Decision<>'4.ΈΓΚΡΙΣΗ' and  Implementation = 'Yes' then  Null else ImplementationMonth end as ImplementationMonth
                ,MonthEvaluation
                ,RestructuringType
                ,u.ApplicationNumber
                ,TotalOutstandingAmount
                ,UnifiedApplicationStatus
                ,CommonApplicationStep
                ,ImplemenationApplicationStep   
                ,ApplicationStepUpdated 
                ,ApplicationBranch
                ,Hub
                ,N3869Flag
                ,Case when u.Decision <>'4.ΈΓΚΡΙΣΗ' and Implementation = 'Yes' then 'No' else Implementation end as Implementation
                ,Channel
                ,ApplicationAutocredit
                ,ApprovalSubRole
                ,ReceiptDateCurrentStep
                ,FirstDateSendDecisionToProcessor
                ,ApplicationSignatureRequired
        FROM ReportUnified                                    u
        LEFT JOIN `{env_prefix}-cdh`.reference.core_paymentjumpmasscancelation    m    
        ON m.ApplicationNumber = u.ApplicationNumber

'''
)
ReportUnifiedUpdated.createOrReplaceTempView("ReportUnifiedUpdated")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### MasterTable

# COMMAND ----------

MasterTable = spark.sql(
    f'''
   select ExposureKey, Bucket, BusinessDate 
   from `{env_prefix}-cdh-delivery`.repoint.core_mastertable
   where BusinessDate = (select max(BusinessDate) BusinessDate from `{env_prefix}-cdh-delivery`.repoint.core_mastertable where BusinessDate <= {run_date!r})
'''
)
MasterTable.createOrReplaceTempView("MasterTable")




# COMMAND ----------

# MAGIC %md
# MAGIC #####  UnionWithrank

# COMMAND ----------


UnionWithrank = spark.sql(
    f'''
   select ru.FirstDateReceiptValuation, ul.ExposureNumber, ul.SnapshotDate,ROW_NUMBER() OVER (PARTITION BY ul.ExposureNumber ORDER BY 
   to_date(ul.ingestiontimestamp) DESC) AS rn,  ul.DeNuncementFlag, ul.NPLWithMaterialityFlag, ul.IFRSGrossCarryingAmount
from `{env_prefix}-cdh`.externalfiles.extunionfile ul 
join ReportUnifiedUpdated ru on ul.ExposureNumber = ru.ExposureKey and ul.SnapshotDate = last_day(add_months(ru.FirstDateReceiptValuation,-1))
join (select SnapshotDate, max(IngestionTimeStamp) IngestionTimeStamp from `{env_prefix}-cdh`.externalfiles.extunionfile group by SnapshotDate) ulm on ulm.IngestionTimeStamp = ul.IngestionTimeStamp
'''
)
UnionWithrank.createOrReplaceTempView("UnionWithrank")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### DailyApplications_ChannelsMapping

# COMMAND ----------


DailyApplications_ChannelsMapping = spark.sql(
    f'''
 SELECT 1 AS Index, 'ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - Microsite' AS `Channel pre-final`, '0.Microsite/ΔΚΛΤ' AS `Channel final`
      UNION ALL 
	  SELECT 2, 'ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ', '1.DNC total'
      UNION ALL 
	  SELECT 3, 'ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ ΟΦΕΙΛΕΤΩΝ', '1.DNC total'
      UNION ALL 
	  SELECT 4, 'CALL CENTER - ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ', '1.DNC total'
      UNION ALL 
	  SELECT 5, 'CallCenterDKLT', '1.DNC total'
      UNION ALL 
	  SELECT 6, 'CEPAL - INHOUSE', '2.CEPAL'
      UNION ALL 
	  SELECT 7, 'CEPAL - ΨΥΧΑΣ', '2.CEPAL'
      UNION ALL 
	  SELECT 8, 'ΔΔΚΛΤ I & II - CEPAL & ΕΞΩΤΕΡΙΚΟΙ ΣΥΝΕΡΓΑΤΕΣ', '3.ΔΚΛΤ'
      UNION ALL 
	  SELECT 9, 'ΔΔΚΛΤ II', '3.ΔΚΛΤ'
      UNION ALL 
	  SELECT 10, 'ΔΚΛΤ-CEPAL & ΕΞΩΤΕΡΙΚΟΙ ΣΥΝΕΡΓΑΤΕΣ', '3.ΔΚΛΤ'
      UNION ALL 
	  SELECT 11, 'ΔΔΟΛΤ', '4.ΔΔΟΛΤ'
      UNION ALL 
	  SELECT 12, 'ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - Microsite', '4.ΔΔΟΛΤ'
      UNION ALL 
	  SELECT 13, 'ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - ΔΕΚ', '4.ΔΔΟΛΤ'
      UNION ALL 
	  SELECT 14, 'ΔΔΟΛΤ - ΑΙΤΗΣΕΙΣ ΡΥΘΜΙΣΕΩΝ - ΕΙΔΙΚΟΥ ΧΕΙΡΙΣΜΟΥ', '4.ΔΔΟΛΤ'
      UNION ALL 
	  SELECT 15, 'A.Kitsikopoulos', '5.Legal Agencies'
      UNION ALL 
	  SELECT 16, 'Calavros & Partners', '5.Legal Agencies'
      UNION ALL 
	  SELECT 17, 'Lexis Legal', '5.Legal Agencies'
      UNION ALL 
	  SELECT 18, 'SIOYFAS & ASSOCIATES LAW FIRM', '5.Legal Agencies'
      UNION ALL 
	  SELECT 19, 'Α. ΚΟΝΤΟΠΟΥΛΟΣ & ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 20, 'Α.Ε  ΜΠΙΤΣΑΞΗΣ & ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 21, 'Δ. ΠΑΤΑΚΑΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 22, 'Δ.Γ. - JURE', '5.Legal Agencies'
      UNION ALL 
	  SELECT 23, 'Δ.Γ. - ΑΝΔΡΙΚΟΠΟΥΛΟΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 24, 'Δ.Γ. - ΚΑΝΕΛΛΟΠΟΥΛΟΥ-ΠΑΠΑΘΑΝΑΣΙΟΥ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 25, 'Δ.Γ. - ΚΟΡΑΚΗ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 26, 'Δ.Γ. - ΜΑΣΣΑΛΗ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 27, 'Δ.Γ. - ΠΑΝΟΥ-ΤΣΙΑΛΙΚΗΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 28, 'Δ.Γ. - ΠΑΤΑΚΑΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 29, 'Δ.Γ. - ΣΙΟΥΦΑΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 30, 'Δ.Γ. - ΣΥΜΒΟΥΛΑΚΗΣ-ΤΣΑΓΚΟΥ-ΣΥΜΒΟΥΛΑΚΗ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 31, 'Δ.Γ. - ΤΖΑΝΝΙΝΗ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 32, 'Δ.Γ. - ΤΣΑΦΑΡΑΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 33, 'Δ.Γ. - ΦΡΑΓΚΟΥ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 34, 'Δ.Γ. - ΧΑΝΙΚΙΑΝ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 35, 'Δ.Γ. - ΧΑΡΑΚΤΙΝΙΩΤΗΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 36, 'Δ.Γ. - ΨΥΧΑΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 37, 'ΔΙΚΗΓΟΡΙΚΟ ΓΡΑΦΕΙΟ ANNA A. ΚΟΡΑΚΗ και ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 38, 'ΔΙΚΗΓΟΡΙΚΟ ΓΡΑΦΕΙΟ ΑΝΔΡΙΚΟΠΟΥΛΟΣ ΑΑ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 39, 'ΔΙΚΗΓΟΡΙΚΟ ΓΡΑΦΕΙΟ ΜΑΣΣΑΛΗ ΕΥΑΓΓΕΛΙΑ-ΜΑΡΙΑ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 40, 'ΔΙΚΗΓΟΡΙΚΟ ΓΡΑΦΕΙΟ ΜΟΥΡΓΕΛΑ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 41, 'ΔΙΚΗΓΟΡΙΚΟ ΓΡΑΦΕΙΟ ΤΖΑΝΝΙΝΗ και ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 42, 'Ι.ΧΑΡΑΚΤΙΝΙΩΤΗΣ & ΣΥΝΕΡΓΑΤΕΣ ΔΙΚΗΓ. ΕΤΑΙΡΕΙΑ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 43, 'Ν. ΠΑΝΟΥ και ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 44, 'ΝΙΚΟΛΑΟΣ Α. ΑΝΔΡΙΚΟΠΟΥΛΟΣ και ΣΥΝΕΡΓΑΤΕΣ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 45, 'Σπυρίδων Τσαφαράς - Δικηγόρος Αθηνών', '5.Legal Agencies'
      UNION ALL 
	  SELECT 46, 'ΣΥΜΒΟΥΛΑΚΗΣ & ΤΣΑΓΚΟΥ', '5.Legal Agencies'
      UNION ALL 
	  SELECT 47, 'ΚΑΤΑΣΤΗΜΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 48, 'ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ ΜΕΣΩ ΔΙΚΤΥΟΥ ΚΑΤ/ΤΩΝ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 49, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΑΝΑΤ. ΑΤΤΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 50, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΑΝΑΤ. ΘΕΣ/ΝΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 51, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΒΟΛΟΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 52, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΒΟΡΕΙOΔΥΤΙΚΗ ΑΤΤΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 53, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΒΟΡΕΙΑ ΑΤΤΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 54, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΔΥΤΙΚΗ ΑΤΤΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 55, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΔΥΤΙΚΗ ΘΕΣ/ΝΙΚΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 56, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΗΡΑΚΛΕΙΟ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 57, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΙΩΑΝΝΙΝΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 58, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΚΑΒΑΛΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 59, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΚΕΝΤΡΟ ΑΘΗΝΩΝ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 60, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΚΟΖΑΝΗ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 61, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΚΟΡΙΝΘΟΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 62, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-Λ.ΒΟΥΛΙΑΓΜΕΝΗΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 63, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-Λ.ΣΥΓΓΡΟΥ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 64, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΛΑΡΙΣΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 65, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΠΑΓΚΡΑΤΙ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 66, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΠΑΤΡΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 67, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΠΕΙΡΑΙΑΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 68, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΠΟΛΗ ΘΕΣ/ΝΙΚΗΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 69, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΡΟΔΟΣ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 70, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΧΑΝΙΑ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 71, 'ΠΕΡ. ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ-ΨΥΧΙΚΟ', '7.Δίκτυο'
      UNION ALL 
	  SELECT 72, 'ΠΕΡΙΦΕΡΕΙΑΚΟ ΚΕΝΤΡΟ', '7.Δίκτυο'
'''
)
DailyApplications_ChannelsMapping.createOrReplaceTempView("DailyApplications_ChannelsMapping")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### DailyApplications_CategoryStep

# COMMAND ----------

DailyApplications_CategoryStep = spark.sql(
f'''
SELECT 1 AS Index, 'Υπογραφή Συμβατικών Εγγράφων - Αίτημα με διαφορετικούς όρους' AS `Τρέχον Βήμα`, 'Στάδιο Αξιολόγησης' AS Stage, 'ΚΑΤΑΣΤΗΜΑ' AS `Κατηγορία_Βήματος`, 'Παραλαβή Απόφασης' AS `Τρέχον Βήμα Final`, '3.Approved/ Pending Communication with Customer' AS `MAPPING for BRC presentation old`, '5.Exclude' AS `MAPPING for BRC presentation`, '' AS `MAPPING extra`, '' AS `Επιστροφή`
UNION ALL 
SELECT 2, 'Υπογραφή Σύμβασης', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 3, 'Υπογραφή Καταστήματος', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '5.Exclude', '', ''
UNION ALL 
SELECT 4, 'Υλοποίηση Ρυθμίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 5, 'Τμήμα Οριστικών Εγκρίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 6, 'Τέλος-Ολοκλήρωση', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 7, 'Τέλος-Ακύρωση', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Ακυρωμένη αίτηση', '5.Exclude', '4.Contracts Prepared & Signed/ Implementation', '', ''
UNION ALL 
SELECT 8, 'Παρατηρήσεις Οριστ.Εγκρίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 9, 'Παραλαβή Απόφασης', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 10, 'Ολοκληρωμένη Αίτηση', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ολοκληρωμένη Αίτηση', '5.Exclude', '4.Contracts Prepared & Signed/ Implementation', '', ''
UNION ALL 
SELECT 11, 'Νομικές και Τεχνικές Ένέργειες', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 12, 'Κατάστημα ? Εκκρεμότητα Εξωτερικού Συνεργάτη', '', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '', '4.Contracts Prepared & Signed/ Implementation', '', ''
UNION ALL 
SELECT 13, 'Κατάστημα / Θεώρηση Αίτησης Υποστήριξης', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 14, 'Κατάστημα / Επιστροφή στο Κατάστημα', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 15, 'Κατάστημα / Επιστροφή από ΔΥΠΛΤ Ν/Τ Έλεγχος', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 16, 'Κατάστημα / Επιστροφή από ΔΥΠΛΤ Έλεγχο Εξασφαλίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 17, 'Κατάστημα / Επιστροφή από ΔΥΠΛΤ - Τμήμα Δανείων και Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 18, 'Κατάστημα / Επιστροφή από ΔΥΠΛΤ - Πρ. Τμήματος Δανείων και Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 19, 'Κατάστημα / Επιστροφή από Δικηγόρο Έλεγχος Εξασφαλίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 20, 'Κατάστημα / Ενημέρωση Ολοκλήρωσης Ενεργειών', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 21, 'Κατάστημα / Αναμονή Υπογραφής Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 22, 'Κατάστημα - Υπογραφή Συμβατικών Εγγράφων - Σύμβαση με διαφορετικούς όρους', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 23, 'Κατάστημα - Υπογραφή Συμβατικών Εγγράφων', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 24, 'Κατάστημα - Υπογραφή Καταστήματος', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 25, 'Κατάστημα - Παραλαβή Απόφασης Εξωτερικών Συνεργατών', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 26, 'Κατάστημα - Παραλαβή Απόφασης', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 27, 'Κατάστημα - Καταχώρηση αίτησης', 'Στάδιο Αξιολόγησης', 'ΔΥΠ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '5.Exclude', '', ''
UNION ALL 
SELECT 28, 'Κατάστημα - Θεώρηση αίτησης', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 29, 'Κατάστημα - Εσφαλμένη Καταχώρηση', 'Στάδιο Αξιολόγησης', 'ΔΥΠ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '5.Exclude', '', ''
UNION ALL 
SELECT 30, 'Κατάστημα - Επιστροφή από Υλοποίηση Αιτήματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 31, 'Κατάστημα - Επιστροφή από Έλεγχο και Παραγωγή Συμβατικών Εγγράφων', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 32, 'Κατάστημα - Επιστροφή από Έλεγχο αιτήματος προς Υλοποίηση', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 33, 'Κατάστημα - Επιστροφή από ΔΥΠ', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 34, 'Κατάστημα - Επιστροφή από Αξιολόγηση/Επανεξέταση', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 35, 'Κατάστημα - Επιστροφή από Αξιολόγηση', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 36, 'Κατάστημα - Ενημέρωση Ολοκλήρωσης Διαδικασίας', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 37, 'Κατάστημα - Εκκρεμότητα Εξωτερικού Συνεργάτη', '', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 38, 'Κατάστημα - Εκκρεμότητα Εγγραφής Προσημείωσης', '', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 39, 'Κατάστημα - Αρχική Υπογραφή Συμβατικών Εγγράφων', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '5.Exclude', '', ''
UNION ALL 
SELECT 40, 'Επιστροφή από Νομικές και Τεχνικές Ενέργειες', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 41, 'Επιστροφή από ΔΥΠ-Μ.Υ.Στ.Ε.Κα.Δ Τμήμα Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 42, 'Επιστροφή από ΔΥΠ-Μ.Υ.Στ.Ε.Κα.Δ Νομικές και Τεχνικές Ένέργειες', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 43, 'Επιστροφή από ΔΥΠ-Μ.Υ.Στ.Ε.Κα.Δ Αξιολόγηση Καλύμματος', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 44, 'Επικαιροποίηση Ρύθμισης από ΔΔΟΛΤ', 'Στάδιο Υλοποίησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 45, 'Επανεξέταση Ρυθμίσεων από ΔΔΟΛΤ', 'Στάδιο Υλοποίησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 46, 'Επανεξέταση από ΔΔΟΛΤ', 'Στάδιο Υλοποίησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 47, 'Ενημέρωση Ολοκλήρωσης Ενεργειών', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 48, 'Έλεγχος Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 49, 'Εκταμίευση / Λήψη Καλυμμάτων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 50, 'ΔΥΠ-Μ.Υ.Στ.Ε.Κα.Δ-Ολοκλήρωση Ενεργειών Καταστήματος', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 51, 'ΔΥΠΛΤ / Προϊστάμενος Τμήματος Δανείων και Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 52, 'ΔΥΠΛΤ / Παρατηρήσεις Καταστήματος Αίτησης Τμήματος Δανείων και Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 53, 'ΔΥΠΛΤ / Παραλαβή Εντολής Ν/Τ Ελέγχου', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 54, 'ΔΥΠΛΤ / Παραλαβή Αίτησης', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 55, 'ΔΥΠΛΤ / Παραλαβή Αιτήματος Εξασφαλίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 56, 'ΔΥΠΛΤ / Παραλαβή Αιτήματος Εκταμίευσης', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 57, 'ΔΥΠΛΤ / Επεξεργασία Ν/Τ Ελέγχου', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 58, 'ΔΥΠΛΤ / Επεξεργασία Αίτησης Τμήματος Δανείων και Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 59, 'ΔΥΠΛΤ / Επεξεργασία Αιτήματος Εξασφαλίσεων', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 60, 'ΔΥΠΛΤ / Αναμονή Ν/Τ Ελέγχου', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 61, 'ΔΥΠΛΤ / Ανάθεση Αιτήματος Εξασφαλίσεων σε Δικηγόρο', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 62, 'ΔΥΠ - Υλοποίηση Αιτήματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 63, 'ΔΥΠ - Θεώρηση Υλοποίησης Αιτήματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 64, 'ΔΥΠ - Θεώρηση Ελέγχου και Παραγωγής Συμβατικών Εγγράφων', '', 'ΔΥΠ', 'Παραλαβή Απόφασης', '', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 65, 'ΔΥΠ - Θεώρηση Ελέγχου Αιτήματος προς Υλοποίηση', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 66, 'ΔΥΠ - Έλεγχος και Παραγωγή Συμβατικών Εγγράφων', '', 'ΔΥΠ', 'Παραλαβή Απόφασης', '', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 67, 'ΔΥΠ - Έλεγχος Αιτήματος προς Υλοποίηση', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 68, 'ΔΥΠ - Εγγραφή Προσημείωσης/Λήψη καλύμματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 69, 'ΔΥΠ - Δημιουργία Συμβατικών Εγγράφων', 'Στάδιο Αξιολόγησης', 'ΔΥΠ', 'Παραλαβή Απόφασης', '4.Contracts Preparation and Signing', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 70, 'ΔΥΠ - Αναμονή Ενέργειών', 'Στάδιο Αξιολόγησης', 'ΔΥΠ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 71, 'ΔΠΚ - Επανεξέταση Απόφασης Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'ΔΠΚ - Επανεξέταση Απόφασης Υποδιευθυντή', '', '', '', ''
UNION ALL 
SELECT 72, 'ΔΠΚ - Επανεξέταση Απόφασης Διευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'ΔΠΚ - Επανεξέταση Απόφασης Διευθυντή', '', '', '', ''
UNION ALL 
SELECT 73, 'ΔΠΚ - Επανεξέταση Απόφασης Senior Officer', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'Επανεξέταση Απόφασης Senior Officer', '', '', '', ''
UNION ALL 
SELECT 74, 'ΔΠΚ - Απόφαση Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'ΔΠΚ - Απόφαση Υποδιευθυντή', '', '', '', ''
UNION ALL 
SELECT 75, 'ΔΠΚ - Απόφαση Διευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'ΔΠΚ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 76, 'ΔΠΚ - Απόφαση Senior Officer', 'Στάδιο Αξιολόγησης', 'ΔΠΚ', 'ΔΠΚ - Απόφαση Senior Officer', '', '', '', ''
UNION ALL 
SELECT 77, 'ΔΚΛΤ - Υπογραφή Συμβατικών Εγγράφων - Αίτημα με διαφορετικούς όρους', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 78, 'ΔΚΛΤ - Υπογραφή Καταστήματος', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 79, 'ΔΚΛΤ - Παραλαβή Απόφασης (Channel)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 80, 'ΔΚΛΤ - Παραλαβή Απόφασης', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 81, 'ΔΚΛΤ - Καταχώρηση αίτησης', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '5.Exclude', '', ''
UNION ALL 
SELECT 82, 'ΔΚΛΤ - Θεώρηση αίτησης', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 83, 'ΔΚΛΤ - Εσφαλμένη Καταχώρηση', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '5.Exclude', '', ''
UNION ALL 
SELECT 84, 'ΔΚΛΤ - Επιστροφή από Υλοποίηση Αιτήματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 85, 'ΔΚΛΤ - Επιστροφή από Έλεγχο και Παραγωγή Συμβατικών Εγγράφων', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 86, 'ΔΚΛΤ - Επιστροφή από Έλεγχο αιτήματος προς Υλοποίηση', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 87, 'ΔΚΛΤ - Επιστροφή από ΔΥΠ', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 88, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση/Επανεξέταση', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 89, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση (Εξωτ. Συνεργάτες)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 90, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση (Εξω  τ. Συνεργάτες)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 91, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση (Channel)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 92, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση (Cepal/N3869)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 93, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση (Cepal)', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 94, 'ΔΚΛΤ - Επιστροφή από Αξιολόγηση', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 95, 'ΔΚΛΤ - Ενημέρωση Ολοκλήρωσης Διαδικασίας', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 96, 'ΔΚΛΤ - Έλεγχος αίτησης', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '5.Exclude', '', ''
UNION ALL 
SELECT 97, 'ΔΚΛΤ - Εκκρεμότητα Εγγραφής Προσημείωσης', '', 'ΔΚΛΤ', 'Παραλαβή Απόφασης', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 98, 'ΔΚΛΤ - Αρχική Υπογραφή Συμβατικών Εγγράφων', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Παραλαβή Απόφασης', '4.Contracts Preparation and Signing', '5.Exclude', '', ''
UNION ALL 
SELECT 99, 'ΔΚΛΤ - Αναμονή Ενεργειών', 'Στάδιο Αξιολόγησης', 'ΔΚΛΤ', 'Επιστροφή από αξιολόγηση', '2.Return to Channels due to Incomplete Submission', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 100, 'Δημιουργία Συμβάσεων', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Ενημέρωση Ολοκλήρωσης Διαδικασίας', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 101, 'ΔΔΟΛΤ ? Εκκρεμότητα Εξωτερικού Συνεργάτη', '', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 102, 'ΔΔΟΛΤ - Προώθηση Απόφασης AC', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ - Προώθηση Απόφασης AC', '1.Pending Credit Information Validation', '', '', ''
UNION ALL 
SELECT 103, 'ΔΔΟΛΤ - Προώθηση Απόφασης', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '3.Approved/ Pending Communication with Customer & Signing', '', ''
UNION ALL 
SELECT 104, 'ΔΔΟΛΤ - Θεώρηση Απόφασης Διευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 105, 'ΔΔΟΛΤ - Επιστροφή από Υλοποίηση Αιτήματος', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 106, 'ΔΔΟΛΤ - Επιστροφή από Έλεγχο και Παραγωγή Συμβατικών Εγγράφων', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 107, 'ΔΔΟΛΤ - Επιστροφή από Έλεγχο Αιτήματος προς υλοποίηση', '', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 108, 'ΔΔΟΛΤ - Επιστροφή Από Decision Maker', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 109, 'ΔΔΟΛΤ - Επανεξέταση Απόφασης Αίτησης εκτός Ευχέρειας', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 110, 'ΔΔΟΛΤ - Επανεξέταση Απόφασης Validator', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 111, 'ΔΔΟΛΤ - Επανεξέταση Απόφασης Processor', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 112, 'ΔΔΟΛΤ - Επανεξέταση Απόφασης Decision Maker', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 113, 'ΔΔΟΛΤ - Επανεξέταση Αποφάσεως Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 114, 'ΔΔΟΛΤ - Επανεξέταση Αποφάσεως Διευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 115, 'ΔΔΟΛΤ - Επανεξέταση Αποφάσεως SCO', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 116, 'ΔΔΟΛΤ - Επανεξέταση Αποφάσεως CO', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 117, 'ΔΔΟΛΤ - Έλεγχος Αίτησης', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 118, 'ΔΔΟΛΤ - Εκκρεμότητα Εξωτερικού Συνεργάτη', '', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '', '2.Return to Channels due to Incomplete Submission/ Credit Reassessment', '', 'yes'
UNION ALL 
SELECT 119, 'ΔΔΟΛΤ - Αξιολόγηση Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 120, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Προετοιμασία Απόφασης', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 121, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 122, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Συμβουλίου', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 123, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Διευθυντή', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 124, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση για Αίτηση εκτός Ευχέρειας', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 125, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Senior Officer', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 126, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Officer B', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 127, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Απόφαση Officer A', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 128, 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Αλλαγή Εγκριτικής Ευχέρειας', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ - Αξιολόγηση Αίτησης/Αλλαγή Εγκριτικής Ευχέρειας', '', '', '', ''
UNION ALL 
SELECT 129, 'ΔΔΟΛΤ - Αξιολόγηση Senior CO', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 130, 'ΔΔΟΛΤ - Αξιολόγηση CO', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 131, 'Αρχική Υπογραφή Συμβατικών Εγγράφων', 'Στάδιο Αξιολόγησης', 'ΚΑΤΑΣΤΗΜΑ', 'Παραλαβή Απόφασης', '3.Approved/ Pending Communication with Customer', '5.Exclude', '', ''
UNION ALL 
SELECT 132, 'Απόφαση - Ενημέρωση Καταστήματος', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '2.Return to Channels due to Incomplete Submission', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 133, 'Αξιολόγηση Καλύμματος', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 134, 'Αξιολόγηση Αιτήματος ΔΥΠ', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '3.Approved/ Pending Communication with Customer', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 135, 'Αξιολόγηση Αιτήματος ΔΛΠ', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '3.Approved/ Pending Communication with Customer', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 136, 'Αξιολόγηση Αιτήματος ΔΔΟΛΤ', 'Στάδιο Υλοποίησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 137, 'Αναμονή Νομικών και Τεχνικών Ενεργειών', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'ΔΥΠ προς υλοποίηση', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 138, 'Αναμονή Ενεργειών Καταστήματος', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 139, 'Αναμονή Ενέργειας Καταστήματος', 'Στάδιο Υλοποίησης', 'ΚΑΤΑΣΤΗΜΑ', 'Στο Κατάστημα αναμονή υπογραφής συμβατικών', '4.Contracts Preparation and Signing', '4.Contracts Prepared & Signed/ Implementation', '5. Implementation', ''
UNION ALL 
SELECT 140, 'Ακύρωση Αίτησης', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Ακυρωμένη αίτηση', '5.Exclude', '4.Contracts Prepared & Signed/ Implementation', '', ''
UNION ALL 
SELECT 141, 'Ακυρωμένη Αίτηση', 'Στάδιο Υλοποίησης', 'ΔΥΠ', 'Ακυρωμένη αίτηση', '5.Exclude', '4.Contracts Prepared & Signed/ Implementation', '', ''
UNION ALL 
SELECT 142, 'NSRM - Επανεξέταση Εισήγησης', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Επανεξέταση Εισήγησης', '', '', '', ''
UNION ALL 
SELECT 143, 'NSRM - Επανεξέταση Απόφασης Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Επανεξέταση Απόφασης Υποδιευθυντή', '', '', '', ''
UNION ALL 
SELECT 144, 'NSRM - Επανεξέταση Απόφασης Διευθυντή', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Επανεξέταση Απόφασης Διευθυντή', '', '', '', ''
UNION ALL 
SELECT 145, 'NSRM - Επανεξέταση Απόφασης Senior Officer', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Επανεξέταση Απόφασης Senior Officer', '', '', '', ''
UNION ALL 
SELECT 146, 'NSRM - Εισήγηση', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Εισήγηση', '', '', '', ''
UNION ALL 
SELECT 147, 'NSRM - Απόφαση Υποδιευθυντή', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Απόφαση Υποδιευθυντή', '', '', '', ''
UNION ALL 
SELECT 148, 'NSRM - Απόφαση Διευθυντή', 'Στάδιο Αξιολόγησης', 'NSRM', '', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 149, 'NSRM - Απόφαση Senior Officer', 'Στάδιο Αξιολόγησης', 'NSRM', 'NSRM - Απόφαση Senior Officer', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''
UNION ALL 
SELECT 150, 'Leasing - Εισήγηση', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', '1.Pending Credit Information Validation', '', ''

 
'''
)
DailyApplications_CategoryStep.createOrReplaceTempView("DailyApplications_CategoryStep")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DailyApplications_BlankStep

# COMMAND ----------

DailyApplications_BlankStep = spark.sql(
f'''
SELECT 1 AS Index ,'190604800810171987' AS `Αριθμός_Αίτησης`, 'ΔΔΟΛΤ - Αξιολόγηση CO' AS `Τρέχον Βήμα`, '' AS `Τρέχον Βήμα Υλοποίησης`, 'Στάδιο Αξιολόγησης' AS Stage, 'ΔΔΟΛΤ - Αξιολόγηση CO' AS `Τρέχον Βήμα Updated`, 'ΔΔΟΛΤ-Αξιολόγηση' AS `Τρέχον Βήμα Final`, '1.Pending Credit Information Validation' AS `BRC_Vima_Mapping`, 'ΔΔΟΛΤ' AS `Κατηγορία_Βήματος`, '' AS `Κατηγορία_Βήματος_Υλοποίησης`
UNION ALL SELECT 2,'190604800810171987', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
UNION ALL SELECT 3,'181212800149135347', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
UNION ALL SELECT 4,'190604800810171987', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
UNION ALL SELECT 5,'190604800810171987', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
UNION ALL SELECT 6,'190725800374189929', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
UNION ALL SELECT 7,'190725800374189929', 'ΔΔΟΛΤ - Αξιολόγηση CO', '', 'Στάδιο Αξιολόγησης', 'ΔΔΟΛΤ - Αξιολόγηση CO', 'ΔΔΟΛΤ-Αξιολόγηση', '1.Pending Credit Information Validation', 'ΔΔΟΛΤ', ''
'''
)
DailyApplications_BlankStep.createOrReplaceTempView("DailyApplications_BlankStep")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ranked_Bucket

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP0

# COMMAND ----------

npe_planning_data_frozen= spark.sql(
f'''
SELECT * FROM `{env_prefix}-reportinputfiles`.repoint.core_npe_planning_data_frozen
'''
)
npe_planning_data_frozen.createOrReplaceTempView("npe_planning_data_frozen")

# COMMAND ----------


TEMP0 = spark.sql(
    f'''
   select ru.*
   , `Channel final` AS ChannelFinal
   ,case when npe.QualcoBucket is not NULL and  cast(npe.QualcoBucket as string) <> ' ' then npe.QualcoBucket else m.Bucket end as QualcoBucket
   , CASE WHEN lower(ru.ApplicationStepUpdated) = lower(cv.`Τρέχον Βήμα`) THEN `MAPPING for BRC presentation`    
	   WHEN bv.`Αριθμός_Αίτησης` = ru.ApplicationNumber THEN BRC_Vima_Mapping END AS BRCStepMapping
   , cv.`Τρέχον Βήμα Final` AS ApplicationStepFinal
   , case when ul.DeNuncementFlag is Null or ul.DeNuncementFlag = ' ' then npe.DenouncedStatus else ul.DeNuncementFlag end as DeNuncementFlag
   , case when ul.NPLWithMaterialityFlag is null or ul.NPLWithMaterialityFlag = ' ' then npe.NPLIndicator else ul.NPLWithMaterialityFlag end as NPLWithMaterialityFlag
   , case when ul.IFRSGrossCarryingAmount is null or trim(ul.IFRSGrossCarryingAmount) = '' then (case when npe.AccountBalance is not null and trim(npe.AccountBalance) <> '' and  npe.AccountBalance <> 'NULL' then cast(replace(npe.AccountBalance,',','.') as decimal(19,2)) 
   else null END ) else cast(replace(ul.IFRSGrossCarryingAmount,',','') as decimal(19,2)) end as AccountBalance
from ReportUnifiedUpdated ru  
LEFT JOIN DailyApplications_ChannelsMapping da
        ON ru.Hub = da.`Channel pre-final`
LEFT JOIN DailyApplications_CategoryStep    cv
       on lower(ru.ApplicationStepUpdated) = lower(cv.`Τρέχον Βήμα`)
LEFT JOIN DailyApplications_BlankStep bv
       ON bv.`Αριθμός_Αίτησης` = ru.ApplicationNumber
left join UnionWithrank ul
on ul.ExposureNumber = ru.ExposureKey 
and ul.SnapshotDate = last_day(add_months(ru.FirstDateReceiptValuation,-1)) 
and ul.rn = 1
left join npe_planning_data_frozen npe on npe.AccountNumber = ru.ExposureKey and cast(npe.DateEvaluation as date) = ru.FirstDateReceiptValuation
left join MasterTable     m on ru.ExposureKey = m.ExposureKey and m.BusinessDate <= last_day(add_months(ru.FirstDateReceiptValuation, -1)) AND m.BusinessDate > last_day(add_months(ru.FirstDateReceiptValuation, -2))
'''
)
TEMP0.createOrReplaceTempView("TEMP0")



# COMMAND ----------


StaticHistoricalTB3UniqueApplsBU = spark.sql(
    f'''
select * from `{env_prefix}-reportinputfiles`.repoint.core_StaticHistoricalTB3UniqueApplsBU
'''
)
StaticHistoricalTB3UniqueApplsBU.createOrReplaceTempView("StaticHistoricalTB3UniqueApplsBU")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Temp0Updated

# COMMAND ----------


TEMP0Updated = spark.sql(
    f'''
   select t.*
,Case when (t.DeNuncementFlag is null or cast(t.DeNuncementFlag as string) = 'NULL')
and (case when bbs.`Month_Evaluation` is null or cast(bbs.`Month_Evaluation` as string) = '' or cast(bbs.`Month_Evaluation` as string) = ' ' then '999901' else bbs.`Month_Evaluation` end) <202202 
then 
(CASE WHEN cast(BBS.Denounced_Status as string) = '' OR BBS.Denounced_Status IS NULL THEN NULL ELSE bbs.Denounced_Status END) 
else t.DeNuncementFlag
 end as DenouncedStatus
 , Case when (t.NPLWithMaterialityFlag is null or cast(t.NPLWithMaterialityFlag as string) = 'NULL')
and (case when bbs.`Month_Evaluation` is null or cast(bbs.`Month_Evaluation` as string) = '' or cast(bbs.`Month_Evaluation` as string) = ' ' then '999901' else bbs.`Month_Evaluation` end) <202202 
then 
(CASE WHEN cast(BBS.NPL_Indicator as string) = '' OR BBS.NPL_Indicator IS NULL THEN NULL ELSE bbs.NPL_Indicator END) 
else t.NPLWithMaterialityFlag
 end as NPLIndicator
,AccountBalance
from TEMP0 t
left join StaticHistoricalTB3UniqueApplsBU BBS
 on BBS.BU = t.AssetSegment and BBS.`Αριθμός_Αίτησης` = t.ApplicationNumber  

'''
)
TEMP0Updated.createOrReplaceTempView("TEMP0Updated")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### ModificationTypeFinal

# COMMAND ----------

ModificationTypeFinal = spark.sql(
f'''
SELECT 	
		1 AS Index, NULL AS `Τύπος Ρύθμισης`, '0.Χωρίς Τύπο Ρύθμισης' AS `Τύπος Ρύθμισης Final`
        UNION ALL 
		SELECT 2,'6018', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 3,'3303', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 4,'7335', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 5,'7469', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 6,'7478', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 7,'4019', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 8,'3026', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 9,'7503', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 10,'3310', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 11,'3078', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 12,'Υφιστάμενος Τύπος Ρύθμισης', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 13,'3079', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 14,'7324', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 15,'7467', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 16,'7474', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 17,'3081', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 18,'3072', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 19,'3021', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 20,'6030', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 21,'7702', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 22,'6019', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 23,'7321', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 24,'3074', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 25,'7410', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 26,'Υφιστάμενος Τύπος Ρύθμισης', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 27,'7473', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 28,'7477', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 29,'7476', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 30,'7475', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 31,'7309', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 32,'6022', '1.Υφιστάμενος Τύπος Ρύθμισης'
        UNION ALL 
		SELECT 33,'D1 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΛΗΞΗ', '2.D'
        UNION ALL 
		SELECT 34,'D1 - Διαγραφή οφειλής στην έναρξη για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 35,'D1 - Διαγραφή οφειλής στην έναρξη για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 36,'D1 - Διαγραφή οφειλής στη λήξη για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 37,'Small Ticket', '2.D'
        UNION ALL 
		SELECT 38,'D1 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΛΗΞΗ', '2.D'
        UNION ALL 
		SELECT 39,'Small Ticket', '2.D'
        UNION ALL 
		SELECT 40,'D1 - Διαγραφή οφειλής στην έναρξη - Δάνεια χωρίς εξασφάλιση έως Ευρώ 30.000', '2.D'
        UNION ALL 
		SELECT 41,'D1 - Διαγραφή οφειλής στη λήξη - Δάνεια χωρίς εξασφάλιση έως Ευρώ 30.000', '2.D'
        UNION ALL 
		SELECT 42,'D1 - Διαγραφή οφειλής στην έναρξη - Δάνεια χωρίς εξασφάλιση έως Ευρώ 30.000', '2.D'
        UNION ALL 
		SELECT 43,'D1 - Διαγραφή οφειλής στη λήξη - Δάνεια χωρίς εξασφάλιση έως Ευρώ 30.000', '2.D'
        UNION ALL 
		SELECT 44,'D1 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΕΝΑΡΞΗ', '2.D'
        UNION ALL 
		SELECT 45,'D2 - Διαγραφή οφειλής για πελάτες με ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 46,'D1 - Διαγραφή οφειλής στη λήξη για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 47,'D', '2.D'
        UNION ALL 
		SELECT 48,'D2 - Διαγραφή οφειλής για πελάτες με ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 49,'D1 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ - ΔΙΑΓΡΑΦΗ ΣΤΗΝ ΕΝΑΡΞΗ', '2.D'
        UNION ALL 
		SELECT 50,'D1 - Διαγραφή οφειλής για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 51,'D1 - Διαγραφή οφειλής για πελάτες χωρίς ακίνητη περιουσία', '2.D'
        UNION ALL 
		SELECT 52,'A9 - Προσωρινή μείωση επιτοκίου με μειωμένη δόση', '3.A'
        UNION ALL 
		SELECT 53,'A', '3.A'
        UNION ALL 
		SELECT 54,'A3 Step Up - Επιμήκυνση διάρκειας με δυνατότητα Step Up', '3.A'
        UNION ALL 
		SELECT 55,'A3 Step Up - Επιμήκυνση διάρκειας με δυνατότητα Step Up', '3.A'
        UNION ALL 
		SELECT 56,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων - Δάνεια με εξασφάλιση με LTV <=100%', '3.A'
        UNION ALL 
		SELECT 57,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων - Δάνεια με εξασφάλιση με LTV <= 120%', '3.A'
        UNION ALL 
		SELECT 58,'A3 - Επιμήκυνση διάρκειας με δυνατότητα Step Up', '3.A'
        UNION ALL 
		SELECT 59,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων - Δάνεια με εξασφάλιση με LTV <=100%', '3.A'
        UNION ALL 
		SELECT 60,'A9 SU - Προσωρινή μείωση επιτοκίου με μειωμένη δόση και με Step-up', '3.A'
        UNION ALL 
		SELECT 61,'A3 - Επιμήκυνση διάρκειας', '3.A'
        UNION ALL 
		SELECT 62,'A3 - Επιμήκυνση διάρκειας με δυνατότητα Step Up', '3.A'
        UNION ALL 
		SELECT 63,'A9 - Προσωρινή μείωση επιτοκίου με μειωμένη δόση', '3.A'
        UNION ALL 
		SELECT 64,'A1 - Τοκοπληρωμή με επέκταση διάρκειας', '3.A'
        UNION ALL 
		SELECT 65,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων - Δάνεια με εξασφάλιση με LTV <= 120%', '3.A'
        UNION ALL 
		SELECT 66,'A6 - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης και StepUp', '3.A'
        UNION ALL 
		SELECT 67,'A7 - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης', '3.A'
        UNION ALL 
		SELECT 68,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων', '3.A'
        UNION ALL 
		SELECT 69,'A3 - Επιμήκυνση διάρκειας', '3.A'
        UNION ALL 
		SELECT 70,'A5 - Μειωμένη δόση άνω των τόκων και Step Up', '3.A'
        UNION ALL 
		SELECT 71,'A1 - Τοκοπληρωμή με επέκταση διάρκειας', '3.A'
        UNION ALL 
		SELECT 72,'A4 - Μειωμένη δόση άνω των τόκων', '3.A'
        UNION ALL 
		SELECT 73,'A7 - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης', '3.A'
        UNION ALL 
		SELECT 74,'A4 - Μειωμένη δόση άνω των τόκων', '3.A'
        UNION ALL 
		SELECT 75,'A6 - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης', '3.A'
        UNION ALL 
		SELECT 76,'A7 Step Up - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης και Step-Up', '3.A'
        UNION ALL 
		SELECT 77,'A6 - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης και StepUp', '3.A'
        UNION ALL 
		SELECT 78,'A3 - Επιμήκυνση διάρκειας με step up', '3.A'
        UNION ALL 
		SELECT 79,'A5 - Μειωμένη δόση άνω των τόκων και Step Up', '3.A'
        UNION ALL 
		SELECT 80,'A6 - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης', '3.A'
        UNION ALL 
		SELECT 81,'A6 Step Up - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης και StepUp', '3.A'
        UNION ALL 
		SELECT 82,'A9 SU - Προσωρινή μείωση επιτοκίου με μειωμένη δόση και με Step-up', '3.A'
        UNION ALL 
		SELECT 83,'A6 Step Up - Μειωμένη δόση άνω των τόκων με επαναφορά της προ ρύθμισης δόσης και StepUp', '3.A'
        UNION ALL 
		SELECT 84,'A7 Step Up - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης και Step-Up', '3.A'
        UNION ALL 
		SELECT 85,'A2 - Μειωμένη δόση με κεφαλαιοποίηση τόκων', '3.A'
        UNION ALL 
		SELECT 86,'A3 - Επιμήκυνση διάρκειας με step up', '3.A'
        UNION ALL 
		SELECT 87,'B4A Step Up - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.A'
        UNION ALL 
		SELECT 88,'B3 Step Up - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων και Step Up', '4.B'
        UNION ALL 
		SELECT 89,'B7 - Εποχικότητα πληρωμών', '4.B'
        UNION ALL 
		SELECT 90,'B2 Step Up - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 91,'B4 Step Up - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.B'
        UNION ALL 
		SELECT 92,'B2 Step Up - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 93,'B3 Step Up - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων και Step Up', '4.B'
        UNION ALL 
		SELECT 94,'B1 Step Up - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 95,'B4 - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.B'
        UNION ALL 
		SELECT 96,'B1 - Επιμήκυνση διάρκειας, μείωση επιτοκίου και Step-up', '4.B'
        UNION ALL 
		SELECT 97,'B4 - Διαδοχικές περίοδοι αποπληρωμής', '4.B'
        UNION ALL 
		SELECT 98,'B2 - Reverse Haircut με Step-Up', '4.B'
        UNION ALL 
		SELECT 99,'7345', '4.B'
        UNION ALL 
		SELECT 100,'B3 - Μείωση επιτοκίου και δόσης με κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 101,'B3 - Μείωση επιτοκίου και δόσης με κεφαλαιοποίηση τόκων και Step-up', '4.B'
        UNION ALL 
		SELECT 102,'B4 - Διαχωρισμός Οφειλής με Split', '4.B'
        UNION ALL 
		SELECT 103,'B4 Step Up - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.B'
        UNION ALL 
		SELECT 104,'B6 - Εισαγωγικό επιτόκιο και μείωση δόσεως χωρίς κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 105,'B3 - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 106,'B4 - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση', '4.B'
        UNION ALL 
		SELECT 107,'B4 - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.B'
        UNION ALL 
		SELECT 108,'B4 - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση', '4.B'
        UNION ALL 
		SELECT 109,'B6 - Εισαγωγικό επιτόκιο και μείωση δόσης χωρίς κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 110,'B6 - Εισαγωγικό επιτόκιο και μείωση δόσεως χωρίς κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 111,'B3 - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων και Step Up', '4.B'
        UNION ALL 
		SELECT 112,'B7 - Εποχικότητα πληρωμών', '4.B'
        UNION ALL 
		SELECT 113,'B4 - Διαχωρισμός Οφειλής με Split και Step-Up', '4.B'
        UNION ALL 
		SELECT 114,'B4 - Διαχωρισμός Οφειλής με Split και Step-Up', '4.B'
        UNION ALL 
		SELECT 115,'B2 - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 116,'7341', '4.B'
        UNION ALL 
		SELECT 117,'7342', '4.B'
        UNION ALL 
		SELECT 118,'B4B - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης με διαδοχικές περιόδους αποπληρωμής', '4.B'
        UNION ALL 
		SELECT 119,'B4B Step Up - Μειωμένη δόση έως 50% της τρέχουσας με επαναφορά της προ ρύθμισης δόσης με διαδοχικές περιόδους αποπληρωμής και Step-Up', '4.B'
        UNION ALL 
		SELECT 120,'B1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου', '4.B'
        UNION ALL 
		SELECT 121,'B4 Step Up - Διαδοχικές περίοδοι αποπληρωμής και Step Up', '4.B'
        UNION ALL 
		SELECT 122,'B6 - Εισαγωγικό επιτόκιο και μείωση δόσης χωρίς κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 123,'B1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 124,'B4 - Διαχωρισμός Οφειλής με Split', '4.B'
        UNION ALL 
		SELECT 125,'B3 - Μείωση επιτοκίου και δόσης με κεφαλαιοποίηση τόκων και Step-up', '4.B'
        UNION ALL 
		SELECT 126,'B3 - Μείωση επιτοκίου και δόσης με κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 127,'B2 - Reverse Haircut με Step-Up', '4.B'
        UNION ALL 
		SELECT 128,'B1 - Επιμήκυνση διάρκειας, μείωση επιτοκίου και Step-up', '4.B'
        UNION ALL 
		SELECT 129,'B4A - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση', '4.B'
        UNION ALL 
		SELECT 130,'7343', '4.B'
        UNION ALL 
		SELECT 131,'Β1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 132,'B', '4.B'
        UNION ALL 
		SELECT 133,'B3 - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων και Step Up', '4.B'
        UNION ALL 
		SELECT 134,'B4A Step Up - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση και Step Up', '4.B'
        UNION ALL 
		SELECT 135,'B3 - Μείωση επιτοκίου και δόσεως με κεφαλαιοποίηση τόκων', '4.B'
        UNION ALL 
		SELECT 136,'B2 - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 137,'B4 Step Up - Διαδοχικές περίοδοι αποπληρωμής και Step Up', '4.B'
        UNION ALL 
		SELECT 138,'B1 Step Up - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 139,'B1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 140,'Β1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου με Step Up', '4.B'
        UNION ALL 
		SELECT 141,'Β2 - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 142,'Β2 - Reverse Haircut με Step Up', '4.B'
        UNION ALL 
		SELECT 143,'B2 - Reverse Haircut', '4.B'
        UNION ALL 
		SELECT 144,'B4 - Διαδοχικές περίοδοι αποπληρωμής', '4.B'
        UNION ALL 
		SELECT 145,'B2 - Reverse Haircut', '4.B'
        UNION ALL 
		SELECT 146,'B1 - Επιμήκυνση διάρκειας και μείωση επιτοκίου', '4.B'
        UNION ALL 
		SELECT 147,'B4A - Διαδοχικές περίοδοι αποπληρωμής - Δάνεια με εξασφάλιση', '4.B'
        UNION ALL 
		SELECT 148,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές και Step-Up', '5.C'
        UNION ALL 
		SELECT 149,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 150,'C3 - Διαγραφή Balloon με Split - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 151,'C4 - Διαγραφή Balloon - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 152,'C1 - Cut & Prenote', '5.C'
        UNION ALL 
		SELECT 153,'C1 - Cut & Prenote με Step-Up', '5.C'
        UNION ALL 
		SELECT 154,'C2 - Cut & Prenote with Split', '5.C'
        UNION ALL 
		SELECT 155,'C2 - Cut & Prenote με Split και Step-Up', '5.C'
        UNION ALL 
		SELECT 156,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 157,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές και Step-Up', '5.C'
        UNION ALL 
		SELECT 158,'C3 - Διαγραφή Balloon με Split - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 159,'C4 - Διαγραφή Balloon - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 160,'C4 - Διαγραφή Balloon - Μικρές οφειλές και Step-Up', '5.C'
        UNION ALL 
		SELECT 161,'C4 - Διαγραφή Balloon με Split - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 162,'C5 - Διαγραφή Balloon - Ανεξασφάλιστα δάνεια', '5.C'
        UNION ALL 
		SELECT 163,'C5 - Διαγραφή Balloon - Ανεξασφάλιστα δάνεια με Step-Up', '5.C'
        UNION ALL 
		SELECT 164,'C', '5.C'
        UNION ALL 
		SELECT 165,'C4 - Διαγραφή Balloon - Μικρές οφειλές με LTV > 120%', '5.C'
        UNION ALL 
		SELECT 166,'C2 - Cut & Prenote με Split και Step-Up', '5.C'
        UNION ALL 
		SELECT 167,'C4 - ΔΠΑ N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 168,'C5 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '5.C'
        UNION ALL 
		SELECT 169,'C1 Step Up - Cut and Prenote και Step Up', '5.C'
        UNION ALL 
		SELECT 170,'C1 Step Up - Cut and Prenote και Step Up', '5.C'
        UNION ALL 
		SELECT 171,'C2 Step Up - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής και Step Up', '5.C'
        UNION ALL 
		SELECT 172,'C2 Step Up - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής και Step Up', '5.C'
        UNION ALL 
		SELECT 173,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 174,'C3 Step Up - Διαγραφή Balloon - Μεγάλες οφειλές με LTV > 110% και Step Up', '5.C'
        UNION ALL 
		SELECT 175,'C3 ΔΠΑ - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 176,'C1 - Cut & Prenote', '5.C'
        UNION ALL 
		SELECT 177,'C1 - Cut & Prenote με Step-Up', '5.C'
        UNION ALL 
		SELECT 178,'C2 - Cut & Prenote with Split', '5.C'
        UNION ALL 
		SELECT 179,'C4 - ΔΠΑ N.3869', '5.C'
        UNION ALL 
		SELECT 180,'C4 Step Up - Διαγραφή Balloon  - Μικρές οφειλές με LTV > 110% με Step Up', '5.C'
        UNION ALL 
		SELECT 181,'C4 - Διαγραφή Balloon - Μικρές οφειλές με LTV > 120% με Step Up', '5.C'
        UNION ALL 
		SELECT 182,'C6 60% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 183,'C6 50% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 184,'C5 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '5.C'
        UNION ALL 
		SELECT 185,'C4 - ΔΠΑ N.3869 ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 186,'C4 - ΔΠΑ N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '5.C'
        UNION ALL 
		SELECT 187,'C4 ΔΠΑ και Step Up - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής και step-up - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 188,'C5 Step Up - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση με Step Up', '5.C'
        UNION ALL 
		SELECT 189,'C6 75% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 190,'C4 ΔΠΑ - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 191,'C6 SU 50% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 192,'C4 - Διαγραφή Balloon  - Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 193,'C4 ΔΠΑ - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 194,'C4 Step Up - Διαγραφή Balloon - Μικρές οφειλές με LTV > 110% με Step Up', '5.C'
        UNION ALL 
		SELECT 195,'C4 - Διαγραφή Balloon - Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 196,'C3 ΔΠΑ - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 197,'C3 Step Up - Διαγραφή Balloon - Μεγάλες οφειλές με LTV > 110% και Step Up', '5.C'
        UNION ALL 
		SELECT 198,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 199,'C5 - N.3869', '5.C'
        UNION ALL 
		SELECT 200,'C4 - ΔΠΑ N.3869 ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 201,'C5 Step Up - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση με Step Up', '5.C'
        UNION ALL 
		SELECT 202,'C4 - ΔΠΑ N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 203,'C4 - greater discount', '5.C'
        UNION ALL 
		SELECT 204,'C4 ΔΠΑ - greater discount', '5.C'
        UNION ALL 
		SELECT 205,'C4 Step Up - greater discount', '5.C'
        UNION ALL 
		SELECT 206,'C4 ΔΠΑ - greater discount', '5.C'
        UNION ALL 
		SELECT 207,'C4 Step Up ΔΠΑ - greater discount', '5.C'
        UNION ALL 
		SELECT 208,'C4 Step Up ΔΠΑ - greater discount', '5.C'
        UNION ALL 
		SELECT 209,'C4 Step Up - greater discount', '5.C'
        UNION ALL 
		SELECT 210,'C4 - greater discount', '5.C'
        UNION ALL 
		SELECT 211,'C6 65% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 212,'C6 50% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 213,'C4 Step Up - Διαγραφή Balloon - Μικρές οφειλές με Step Up', '5.C'
        UNION ALL 
		SELECT 214,'C5 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 215,'C6 SU 65% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 216,'C6 SU 75% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 217,'C6 75% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 218,'C6 65% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 219,'C6 60% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 220,'C6 SU 75% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 221,'C6 SU 65% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 222,'C6 SU 60% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 223,'C6 SU 60% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 224,'C2 - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 225,'C6 SU 50% - Διαγραφή Balloon με τυποποιημένους όρους αποπληρωμής και δυνατότητα Step-Up', '5.C'
        UNION ALL 
		SELECT 226,'C4 - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μικρές οφειλές με LTV > 120%', '5.C'
        UNION ALL 
		SELECT 227,'C4 - Διαγραφή Balloon - Μικρές οφειλές με LTV > 120%', '5.C'
        UNION ALL 
		SELECT 228,'C3 - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μεγάλες οφειλές με LTV> 120%', '5.C'
        UNION ALL 
		SELECT 229,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV >120% και Step Up', '5.C'
        UNION ALL 
		SELECT 230,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV >120%', '5.C'
        UNION ALL 
		SELECT 231,'C3 - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μεγάλες οφειλές με LTV> 120%', '5.C'
        UNION ALL 
		SELECT 232,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV >120% και Step Up', '5.C'
        UNION ALL 
		SELECT 233,'C4 - ΔΠΑ N.3869', '5.C'
        UNION ALL 
		SELECT 234,'C2 - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής και Step Up', '5.C'
        UNION ALL 
		SELECT 235,'C5 - N.3869', '5.C'
        UNION ALL 
		SELECT 236,'C2 - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής και Step Up', '5.C'
        UNION ALL 
		SELECT 237,'C2 - Cut and Prenote με διαδοχικές περιόδους αποπληρωμής', '5.C'
        UNION ALL 
		SELECT 238,'C1 - Cut and Prenote και Step Up', '5.C'
        UNION ALL 
		SELECT 239,'C1 - Cut and Prenote', '5.C'
        UNION ALL 
		SELECT 240,'C1 - Cut and Prenote και Step Up', '5.C'
        UNION ALL 
		SELECT 241,'C1 - Cut and Prenote', '5.C'
        UNION ALL 
		SELECT 242,'C5 - Διαγραφή Balloon - Ανεξασφάλιστα δάνεια με Step-Up', '5.C'
        UNION ALL 
		SELECT 243,'C5 - Διαγραφή Balloon - Ανεξασφάλιστα δάνεια', '5.C'
        UNION ALL 
		SELECT 244,'C4 - Διαγραφή Balloon με Split - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 245,'C3 - Διαγραφή Balloon - Μεγάλες οφειλές με LTV >120%', '5.C'
        UNION ALL 
		SELECT 246,'C4 - ΔΠΑ N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ', '5.C'
        UNION ALL 
		SELECT 247,'C4 - Διαγραφή Balloon - Μικρές οφειλές και Step-Up', '5.C'
        UNION ALL 
		SELECT 248,'C4 ΔΠΑ και Step Up - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής και step-up - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 249,'C4 ΔΠΑ - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 250,'C4 Step Up - Διαγραφή Balloon - Μικρές οφειλές με Step Up', '5.C'
        UNION ALL 
		SELECT 251,'C3 ΔΠΑ και Step Up - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής και step-up - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 252,'C3 ΔΠΑ - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 253,'C3 Step Up - Διαγραφή Balloon - Μεγάλες οφειλές και Step Up', '5.C'
        UNION ALL 
		SELECT 254,'C3 ΔΠΑ και Step Up - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής και step-up - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 255,'C3 ΔΠΑ - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής - Μεγάλες οφειλές', '5.C'
        UNION ALL 
		SELECT 256,'C4 - Διαγραφή Balloon - Μικρές οφειλές με LTV > 120% με Step Up', '5.C'
        UNION ALL 
		SELECT 257,'C5 - N.3869 ΔΙΑΤΡΑΠΕΖΙΚΗ ΜΕ ΑΤΟΚΗ ΠΕΡΙΟΔΟ', '5.C'
        UNION ALL 
		SELECT 258,'C4 - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής - Μικρές οφειλές με LTV > 120%', '5.C'
        UNION ALL 
		SELECT 259,'C4 ΔΠΑ και Step Up - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής και Step Up-Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 260,'C4 ΔΠΑ και Step Up - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής και Step Up-Μικρές οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 261,'C3 ΔΠΑ και Step Up - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής και Step Up-Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 262,'C3 ΔΠΑ και Step Up - Διαγραφή Balloon με δυνατότητα διαδοχικών περιόδων αποπληρωμής και Step Up-Μεγάλες οφειλές με LTV > 110%', '5.C'
        UNION ALL 
		SELECT 263,'C5 - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση με Step Up', '5.C'
        UNION ALL 
		SELECT 264,'C5 - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση', '5.C'
        UNION ALL 
		SELECT 265,'C5 - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση με Step Up', '5.C'
        UNION ALL 
		SELECT 266,'C5 - Διαγραφή Balloon - Δάνεια χωρίς εξασφάλιση', '5.C'
        UNION ALL 
		SELECT 267,'C4 ΔΠΑ - Διαγραφή Balloon με διαδοχικές περιόδους αποπληρωμής - Μικρές οφειλές', '5.C'
        UNION ALL 
		SELECT 268,'C3 Step Up - Διαγραφή Balloon - Μεγάλες οφειλές και Step Up', '5.C'
'''
)
ModificationTypeFinal.createOrReplaceTempView("ModificationTypeFinal")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP1

# COMMAND ----------


TEMP1 = spark.sql(
    f'''
 select * 
 , CASE WHEN t0.N3869Flag IS NOT NULL AND t0.N3869Flag <> '' 
 AND (PartyTaxIdentificationNumber <> '110503634' OR PartyTaxIdentificationNumber IS NULL) AND (PartyTaxIdentificationNumber <> '110503634' OR PartyTaxIdentificationNumber IS NULL) 
 AND RestructuringType LIKE '%3869%' THEN 'Yes' END AS L3869Final
 , CASE
 WHEN  (Channel IS NOT NULL and Channel<>'' and Channel<>' ') THEN Channel
  WHEN (Channel IS NULL or Channel='' or Channel=' ')  AND Hub = 'CALL CENTER - ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ' THEN 'CALL CENTER - ALPHA BANK ΥΠΗΡΕΣΙΕΣ ΕΝΗΜΕΡΩΣΕΩΣ'
WHEN (Channel IS NULL or Channel='' or Channel=' ')  AND (Hub IS NOT NULL and Hub<>'' and Hub<>' ') THEN Hub
 WHEN (Channel IS NULL or Channel='' or Channel=' ')  AND Hub = 'ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ ΜΕΣΩ ΔΙΚΤΥΟΥ ΚΑΤ/ΤΩΝ' THEN 'ΚΕΝΤΡΟ ΔΙΑΧ/ΣΕΩΣ ΚΑΘ/ΣΕΩΝ ΜΕΣΩ ΔΙΚΤΥΟΥ ΚΑΤ/ΤΩΝ'
WHEN (Channel IS NULL or Channel='' or Channel=' ' ) THEN 
case when ApplicationBranch <>' ' and ApplicationBranch is not null then
                    concat('ΚΑΤΑΣΤΗΜΑ ',Right(ApplicationBranch,3)) else '' end 
                    END AS InputChannel  
, coalesce(tf.`Τύπος Ρύθμισης Final`,'0.Χωρίς Τύπο Ρύθμισης') AS ModificationTypeFinal

 from TEMP0Updated t0
left join ModificationTypeFinal tf
ON tf.`Τύπος Ρύθμισης` = t0.RestructuringType
'''
)
TEMP1.createOrReplaceTempView("TEMP1")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP2

# COMMAND ----------


TEMP2 = spark.sql(
    f'''
 select *
,CASE WHEN (Decision = '4.ΈΓΚΡΙΣΗ' OR Decision = '3.ΕΚΚΡΕΜΗ') AND (Implementation <> 'Yes' OR Implementation =' ') 
    AND (BRCStepMapping <> '5.Exclude' OR BRCStepMapping =' ') THEN 'Yes' END AS ActiveRequestMIS  
 from TEMP1
'''
)
TEMP2.createOrReplaceTempView("TEMP2")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP3

# COMMAND ----------


TEMP3 = spark.sql(
    f'''
 select *
, CASE WHEN RestructuringType NOT LIKE '%3869%' AND ModificationTypeFinal = '5.C' AND UnifiedApplicationStatus = 'Ενεργή' 
                     AND CommonApplicationStep NOT LIKE 'Κατάστημα - Ενημέρωση Ολοκλήρωσης Διαδικασίας' And CommonApplicationStep NOT LIKE 'ΔΚΛΤ - Ενημέρωση Ολοκλήρωσης Διαδικασίας' 
             	     AND ActiveRequestMIS = 'Yes' THEN 'Yes' END AS CFlag
             ,'1.Core' AS CoreFlag
		     , CASE WHEN ModificationTypeFinal = '2.D' OR ModificationTypeFinal= '5.C' THEN 'Long Term With haircut'
		            ELSE 'Long Term' END AS ApplicationType
        
        FROM TEMP2 
'''
)
TEMP3.createOrReplaceTempView("TEMP3")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TB1_TotalApplication_Interim

# COMMAND ----------


TB1_TotalApplication_Interim = spark.sql(
    f'''
 SELECT      CAST({run_date!r} AS DATE) as BusinessDate                      
            ,       CDI							
            ,       AssetSegment 							
            ,       ExposureKey
			,       ApplicationType
            ,       Decision 						
            ,       FirstDateReceiptValuation 				
            ,       MonthEvaluation 								
            ,       ApplicationNumber			
            ,       TotalOutstandingAmount 								
            ,       ModificationTypeFinal					
            ,       Implementation				
		FROM TEMP3

'''
)
TB1_TotalApplication_Interim.createOrReplaceTempView("TB1_TotalApplication_Interim")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP4

# COMMAND ----------


TEMP4 = spark.sql(
    f'''
 SELECT ExposureKey
			 , MAX(FirstDateReceiptValuation)  AS FirstDateReceiptValuation
			 , MAX(MonthEvaluation) AS MonthEvaluation
        FROM TEMP3  
		GROUP BY ExposureKey
'''
)
TEMP4.createOrReplaceTempView("TEMP4")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP5

# COMMAND ----------


TEMP5 = spark.sql(
    f'''
  SELECT 
	                PartyTaxIdentificationNumber                          
            ,       MAX(CDI)                    AS CDI							
            ,       AssetSegment 							
            ,       t4.ExposureKey				
            ,       AgreementKey 				
            ,       MAX(Decision)                AS Decision				
            ,       t4.FirstDateReceiptValuation 		    		
            ,       t4.MonthEvaluation 		    		
            ,       MAX(ImplementationMonth)    AS ImplementationMonth	 				
            ,       MAX(RestructuringType)      AS RestructuringType
            ,       t3.DenouncedStatus 				
            ,       NPLIndicator 				
            ,       MAX(ApplicationNumber)      AS ApplicationNumber		
            ,       MAX(TotalOutstandingAmount) AS TotalOutstandingAmount				
            ,       MAX(AccountBalance)         AS AccountBalance				
            ,       MAX(ModificationTypeFinal)  AS ModificationTypeFinal		
            ,       MAX(ChannelFinal)           AS ChannelFinal 				
            ,       MAX(UnifiedApplicationStatus) AS UnifiedApplicationStatus						
            ,       MAX(CommonApplicationStep)  AS CommonApplicationStep			
            ,       MAX(ImplemenationApplicationStep) AS ImplemenationApplicationStep			
            ,       MAX(ApplicationStepFinal)   AS ApplicationStepFinal		
            ,       MAX(BRCStepMapping)         AS BRCStepMapping				
            ,       MAX(Implementation) 	    AS Implementation					
            ,       MAX(ImplementationDate)     AS ImplementationDate				
            ,       ActiveRequestMIS 			
            ,       QualcoBucket 	
			,       InputChannel 
            ,       ApplicationBranch						     				
            ,       Hub 										
	        ,       CFlag 						
            ,       N3869Flag 					
            ,       L3869Final 
			,       MAX(CoreFlag)               AS CoreFlag
			,       MAX(ApplicationAutocredit)  AS ApplicationAutocredit 
			,       MAX(ApplicationStepUpdated) AS ApplicationStepUpdated
			,       MAX(ApprovalSubRole)      AS ApprovalSubRole
			,       ReceiptDateCurrentStep
			,       ApplicationSignatureRequired
			,       Channel
        
        FROM TEMP3       t3
		INNER JOIN TEMP4 t4
		ON t4.ExposureKey = t3.ExposureKey AND t4.FirstDateReceiptValuation = t3.FirstDateReceiptValuation
		GROUP BY PartyTaxIdentificationNumber
		        ,AssetSegment
				,t4.ExposureKey
				,AgreementKey
				,t4.FirstDateReceiptValuation 
				,t4.MonthEvaluation
				,DenouncedStatus 	
				,NPLIndicator 	
				,ActiveRequestMIS
				,QualcoBucket 	
				,InputChannel 
				,ApplicationBranch			
				,Hub
				,CFlag 			
				,N3869Flag 		
				,L3869Final 
				,ReceiptDateCurrentStep
				,ApplicationSignatureRequired
				,Channel

'''
)
TEMP5.createOrReplaceTempView("TEMP5")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP6

# COMMAND ----------


TEMP6 = spark.sql(
    f'''
       
        SELECT *
			,  CASE WHEN Hub LIKE 'ΠΕΡ%' OR Hub LIKE 'ΚΕΝΤ%'  THEN Hub
			        WHEN Channel IS NOT NULL  and  trim(Channel) <>'' THEN Channel
					WHEN Hub IS NOT NULL and trim(Hub) <>'' THEN Hub
					ELSE 'ΚΑΤΑΣΤΗΜΑ'
					END AS ChannelPreFinalRegistration
        FROM TEMP5     

'''
)
TEMP6.createOrReplaceTempView("TEMP6")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### TEMP7

# COMMAND ----------


TEMP7 = spark.sql(
    f'''
       SELECT *
			,  CASE WHEN t.ChannelPreFinalRegistration = da.`Channel pre-final` THEN da.`Channel final` END AS ChannelRegistration
			,  RANK() OVER(PARTITION BY ExposureKey ORDER BY ReceiptDateCurrentStep DESC)                   AS Rank  
        FROM TEMP6                                        t
		LEFT JOIN DailyApplications_ChannelsMapping da
        ON t.ChannelPreFinalRegistration = da.`Channel pre-final`

  
'''
)
TEMP7.createOrReplaceTempView("TEMP7")



# COMMAND ----------

NonCorePerimeter = spark.sql(
    f'''
    SELECT DISTINCT RIGHT(ExposureKey, LEN(ExposureKey)-3) AS ExposureKeyNoPrefix  
    FROM `{env_prefix}-cdh`.primary.exposure 
    WHERE PortfolioName NOT LIKE '%Core%' 
      AND BusinessDate <= '2026-03-31'
'''
)
NonCorePerimeter.createOrReplaceTempView("NonCorePerimeter")

# COMMAND ----------

TEMP8 = spark.sql(
    f'''
SELECT TMP.*  
FROM TEMP7 TMP
LEFT JOIN NonCorePerimeter NCP ON TMP.ExposureKey = NCP.ExposureKeyNoPrefix
WHERE ExposureKeyNoPrefix IS NULL
'''
)
TEMP8.createOrReplaceTempView("TEMP8")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### NPEPlanningData

# COMMAND ----------


NPEPlanningData = spark.sql(
    f'''
       SELECT 
			
					{run_date!r} as BusinessDate
	        ,       PartyTaxIdentificationNumber                          
            ,       CDI							
            ,       AssetSegment 							
            ,       ExposureKey				
            ,       AgreementKey				
            ,       Decision 						
            ,       FirstDateReceiptValuation AS DateEvaluation 				
            ,       MonthEvaluation 				
            ,       ImplementationMonth 					
            ,       RestructuringType AS ModificationType 			
            ,       DenouncedStatus 				
            ,       NPLIndicator 				
            ,       ApplicationNumber			
            ,       TotalOutstandingAmount AS BalanceAmount 				
            ,       AccountBalance 				
            ,       ModificationTypeFinal		
            ,       ChannelFinal 				
            ,       UnifiedApplicationStatus Status 						
            ,       CommonApplicationStep AS ApplicationStep 				
            ,       ImplemenationApplicationStep AS ImplementationStep			
            ,       ApplicationStepFinal  		
            ,       BRCStepMapping				
            ,       Implementation					
            ,       ImplementationDate 					
            ,       ActiveRequestMIS 			
            ,       QualcoBucket 	
			,       InputChannel 
            ,       ApplicationBranch						     				
            ,       Hub									
	        ,       CFlag 						
            ,       N3869Flag AS L3869Flag 					
            ,       L3869Final 
			,       CoreFlag
			,       ApplicationAutocredit  
			,       ApplicationStepUpdated 
			,       ApprovalSubRole
			,       ReceiptDateCurrentStep
			,       ApplicationSignatureRequired
			,       ChannelPreFinalRegistration
			,       ChannelRegistration
		FROM TEMP8
		WHERE Rank = 1 
'''
)
NPEPlanningData.createOrReplaceTempView("NPEPlanningData")



# COMMAND ----------

# MAGIC %md
# MAGIC ##### final_df

# COMMAND ----------

final_df = spark.sql(
f'''
SELECT 
  CAST(BusinessDate AS date) AS BusinessDate
, CAST(PartyTaxIdentificationNumber AS string) AS PartyTaxIdentificationNumber
, CAST(CDI AS string) AS CDI
, CAST(AssetSegment AS string) AS AssetSegment
, CAST(ExposureKey AS string) AS ExposureKey
, CAST(AgreementKey AS string) AS AgreementKey
, CAST(Decision AS string) AS Decision
, CAST(DateEvaluation AS date) AS DateEvaluation
, CAST(MonthEvaluation AS string) AS MonthEvaluation
, CAST(ImplementationMonth AS string) AS ImplementationMonth
, CAST(ModificationType AS string) AS ModificationType
, CAST(DenouncedStatus AS bigint) AS DenouncedStatus
, CAST(NPLIndicator AS bigint) AS NPLIndicator
, CAST(ApplicationNumber AS string) AS ApplicationNumber
, CAST(BalanceAmount AS decimal(19,2)) AS BalanceAmount
, CAST(AccountBalance AS decimal(19,2)) AS AccountBalance
, CAST(ModificationTypeFinal AS string) AS ModificationTypeFinal
, CAST(ChannelFinal AS string) AS ChannelFinal
, CAST(Status AS string) AS Status
, CAST(ApplicationStep AS string) AS ApplicationStep
, CAST(ImplementationStep AS string) AS ImplementationStep
, CAST(ApplicationStepFinal AS string) AS ApplicationStepFinal
, CAST(BRCStepMapping AS string) AS BRCStepMapping
, CAST(Implementation AS string) AS Implementation
, CAST(ImplementationDate AS date) AS ImplementationDate
, CAST(ActiveRequestMIS AS string) AS ActiveRequestMIS
, CAST(QualcoBucket AS string) AS QualcoBucket
, CAST(InputChannel AS string) AS InputChannel
, CAST(ApplicationBranch AS string) AS ApplicationBranch
, CAST(Hub AS string) AS Hub
, CAST(CFlag AS string) AS CFlag
, CAST(L3869Flag AS string) AS L3869Flag
, CAST(L3869Final AS string) AS L3869Final
, CAST(CoreFlag AS string) AS CoreFlag
, CAST(ApplicationAutocredit AS string) AS ApplicationAutocredit
, CAST(ApplicationStepUpdated AS string) AS ApplicationStepUpdated
, CAST(ApprovalSubRole AS string) AS ApprovalSubRole
, CAST(ReceiptDateCurrentStep AS date) AS ReceiptDateCurrentStep
, CAST(ApplicationSignatureRequired AS string) AS ApplicationSignatureRequired
, CAST(ChannelPreFinalRegistration AS string) AS ChannelPreFinalRegistration
, CAST(ChannelRegistration AS string) AS ChannelRegistration
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM NPEPlanningData 
'''
)
final_df.createOrReplaceTempView("final_df")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### final_sec_df

# COMMAND ----------

final_sec_df = spark.sql(
f'''
SELECT 
  CAST(BusinessDate AS date) AS BusinessDate
, CAST(CDI AS string) AS CDI
, CAST(AssetSegment AS string) AS AssetSegment
, CAST(ExposureKey AS string) AS ExposureKey
, CAST(ApplicationType AS string) AS ApplicationType
, CAST(Decision AS string) AS Decision
, CAST(FirstDateReceiptValuation AS date) AS FirstDateReceiptValuation
, CAST(MonthEvaluation AS string) AS MonthEvaluation
, CAST(ApplicationNumber AS string) AS ApplicationNumber
, CAST(TotalOutstandingAmount AS decimal(19,2)) AS TotalOutstandingAmount
, CAST(ModificationTypeFinal AS string) AS ModificationTypeFinal
, CAST(Implementation AS string) AS Implementation
, CAST(getdate() AS TIMESTAMP) AS InsertTimeStamp
FROM TB1_TotalApplication_Interim 
'''
)
final_sec_df.createOrReplaceTempView("final_sec_df")

# COMMAND ----------

if final_sec_df.rdd.isEmpty():
    raise ValueError(f"Core_NPEPlanningData: final_sec_df DataFrame is empty - aborting write to {delivery_table_secondary_path} to avoid overwriting with no data")

final_sec_df.write \
    .format("delta") \
    .partitionBy("BusinessDate") \
    .mode("append") \
    .saveAsTable(delivery_table_secondary_path) # in order to write in delta_lake

if final_df.rdd.isEmpty():
    raise ValueError(f"Core_NPEPlanningData: final DataFrame is empty - aborting write to {delivery_table_path} to avoid overwriting with no data")

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

NPEPlanningData = spark.sql(
f'''
select * from {delivery_table_path}
where BusinessDate = {run_date!r}
'''
)
NPEPlanningData.createOrReplaceTempView("NPEPlanningData")

# COMMAND ----------

output_path = "/Volumes/"+env_prefix+"-cdh-delivery/repoint/exports/Core/NPEPlanningData/"
file_name = "NPEPlanningData_" +run_date+ ".csv"
NPEPlanningData.coalesce(1).write.mode("append").option("header", True).csv(f"{output_path}")
rename_and_cleanup_files(output_path, file_name)
