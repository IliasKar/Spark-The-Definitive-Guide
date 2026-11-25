# Databricks notebook source
# MAGIC %md
# MAGIC import logging 
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

# import boto3
import os
import logging

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-02-28'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-02-28')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# DBTITLE 1,ENVIRONMENT VARIABLES
# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"]

# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix() 

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
unity = dbutils.widgets.get("unity") 

table_name = 'DDPOFinalTable'

# """
# Takes the s3 parameter value from ssm

# ssm_client = boto3.client("ssm", region_name="eu-central-1") # Initialize the SSM client

# response = ssm_client.get_parameter(
#     Name='/cfn/a360/matching/conf_s3_path',
#     WithDecryption=False
# )

# s3_path = response['Parameter']['Value']
# """

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Initialize the schema retrieved from Redis

# COMMAND ----------

schema = (
    "DPOFinalTableId string"
    ",SourceSystem string"
    ",PortfolioKey string"
    ",PortfolioName string"
    ",IngestionTimestamp timestamp"
    ",BatchId string"
    ",DPOFinalTableKey string"
    ",AgreementKey string"
    ",NetLiquidationValue1stAuction double"
    ",NetLiquidationValue2ndAuction double"
    ",NetLiquidationValue3rdAuction double"
    ",NetLiquidationValue4thAuction double"
    ",ExpectedNetLiquidationValue double"
    ",NetPresentValueOfExpectedNetLiquidationValue double"
    ",TimeToAuction bigint"
    ",HasPayerFlag bigint"
    ",NetPresentValueOfNonRealEstates double"
    ",PaymentPlanSum double"
    ",NetPresentValueOfPaymentPlan double"
    ",ExpectedNetPresentValueOfPaymentPlan double"
    ",DefaultNetPresentValueOfRecoveries double"
    ",ExpectedNetPresentValueOfRecoveries double"
    ",DefaultNetPresentValueOfServicingFees double"
    ",NonDefaultNetPresentValueOfServicingFees double"
    ",ExpectedNetPresentValueOfServicingFees double"
    ",ExpectedNetPresentValueOfNetRecoveries double"
    ",NetPresentValueOfRecoveriesHaps double"
    ",NetPresentValueOfRecoveriesActuals double"
    ",NetPresentValueOfRecoveriesHapsActuals double"
    ",NetPresentValueOfRecoveriesBusinessPlan double"
    ",FairValueBusinessPlan double"
    ",FairValueHaps double"
    ",MinDPOAmount double"
    ",DPOCollectionFees double"
    ",FinalDPOAmount double"
    ",DataIssues string"
    ",TotalOutstandingBalance double"
    ",GrossBookValue double"
    ",SecuredFlag int"
    ",LVnonRE double"
    ",TotalLV double"
    ",AugmentedDPO double"
    ",RunDate date"
    ",ReferencedBusinessDate date"
    ",BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
 
# ================== Create Delta Table ==================
logging.info("Creating Delta Table...")
if spark.catalog.tableExists(derivative_table_path) is False:
    create_delta_table(
        spark,
        derivative_table_path,
        partition_columns,
        schema,  # type: ignore
    )
else:
    logging.info("Table already exists.")
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path,
    run_date,  # type: ignore
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create wanted Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dpo_finaltable

# COMMAND ----------

dpo_finaltable = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.dpo
  WHERE Granularity = 'Agreement'
  AND BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
),
dpo_finaltable AS (
SELECT 
    AgreementKey,
    dpo.PortfolioId,
    LVFirstAuctionNet,
    LVSecondAuctionNet,
    LVThirdAuctionNet,
    LVFourthAuctionNet,
    ExpNetLV,
    NPVExpNetLV,
    TimeToAuction,
    Payer,
    NPVnonRE,
    PaymentPlanSum,
    NPVPaymentPlan,
    ExpectedNPVPaymentPlan,
    defaultNPVrecoveries,
    ExpectedNPVrecoveries,
    defaultNPVServicingFees,
    nondefaultNPVServicingFees,
    ExpectedNPVServicingFees,
    ExpectedNPVNetRecoveries,
    npvRecoveriesHaps,
    npvRecoveriesActuals,
    npvRecoveriesHapsActuals,
    npvRecoveriesBP,
    fairValueBP,
    fairValueHaps,
    MinDPOAmount,
    DPOCollectionFees,
    FinalDPOAmount,
    dataIssues,
    SecuredFlag,
    LVnonRE,
    TotalLV,
    AugmentedDPO,
    RunDate,
    businessdate,
    row_number() OVER(PARTITION BY AgreementKey ORDER BY RunDate DESC, IngestionTimestamp DESC) rn --keep the latest record for each agreement
FROM  `{env}-ds`.aaas.dpo dpo
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON dpo.PortfolioId = maxdate.PortfolioId
AND dpo.BusinessDate = maxdate.MaxBusinessDate
WHERE dpo.Granularity = 'Agreement'
)
SELECT * FROM dpo_finaltable WHERE rn = 1;
'''
)

dpo_finaltable.createOrReplaceTempView("dpo_finaltable")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
    PortfolioName,
    PortfolioId,
    PortfolioKey,
    Businessdate
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Define some subqueries from the final query as temporary views for better readability and maintenance

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Final query with resulted DataFrame
# MAGIC

# COMMAND ----------

final_df = spark.sql(f'''
select sha2(concat_ws(";", dpo_finaltable.AgreementKey, "biteam"),256) DPOFinalTableId
    , 'biteam' SourceSystem
    , portfolio.PortfolioKey
    , portfolio.PortfolioName
    , CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) IngestionTimestamp
    , cast(NUll as string) BatchId
    , sha2(concat_ws(";", dpo_finaltable.AgreementKey),256) DPOFinalTableKey
    , dpo_finaltable.AgreementKey AgreementKey
    , dpo_finaltable.LVFirstAuctionNet NetLiquidationValue1stAuction
    , dpo_finaltable.LVSecondAuctionNet NetLiquidationValue2ndAuction
    , dpo_finaltable.LVThirdAuctionNet NetLiquidationValue3rdAuction
    , dpo_finaltable.LVFourthAuctionNet NetLiquidationValue4thAuction
    , dpo_finaltable.ExpNetLV ExpectedNetLiquidationValue
    , dpo_finaltable.NPVExpNetLV NetPresentValueOfExpectedNetLiquidationValue
    , dpo_finaltable.TimeToAuction TimeToAuction
    , cast(dpo_finaltable.Payer as bigint) HasPayerFlag
    , dpo_finaltable.NPVNonRE NetPresentValueOfNonRealEstates
    , dpo_finaltable.PaymentPlanSum PaymentPlanSum
    , dpo_finaltable.NPVPaymentPlan NetPresentValueOfPaymentPlan
    , dpo_finaltable.ExpectedNPVPaymentPlan ExpectedNetPresentValueOfPaymentPlan
    , dpo_finaltable.DefaultNPVRecoveries DefaultNetPresentValueOfRecoveries
    , dpo_finaltable.ExpectedNPVRecoveries ExpectedNetPresentValueOfRecoveries
    , dpo_finaltable.DefaultNPVServicingFees DefaultNetPresentValueOfServicingFees
    , dpo_finaltable.NonDefaultNPVServicingFees NonDefaultNetPresentValueOfServicingFees
    , dpo_finaltable.ExpectedNPVServicingFees ExpectedNetPresentValueOfServicingFees
    , dpo_finaltable.ExpectedNPVNetRecoveries ExpectedNetPresentValueOfNetRecoveries
    , dpo_finaltable.NPVRecoveriesHaps NetPresentValueOfRecoveriesHaps
    , dpo_finaltable.NPVRecoveriesActuals NetPresentValueOfRecoveriesActuals
    , dpo_finaltable.NPVRecoveriesHapsActuals NetPresentValueOfRecoveriesHapsActuals
    , dpo_finaltable.NPVRecoveriesBP NetPresentValueOfRecoveriesBusinessPlan
    , dpo_finaltable.FairValueBP FairValueBusinessPlan
    , dpo_finaltable.FairValueHaps 
    , dpo_finaltable.MinDPOAmount 
    , dpo_finaltable.DPOCollectionFees 
    , dpo_finaltable.FinalDPOAmount 
    , dpo_finaltable.DataIssues
    , cast(NUll as double) TotalOutstandingBalance
    , cast(NUll as double) GrossBookValue
    , cast(dpo_finaltable.SecuredFlag as int)
    , dpo_finaltable.LVnonRE
    , dpo_finaltable.TotalLV
    , dpo_finaltable.AugmentedDPO 
    , dpo_finaltable.RunDate
    , dpo_finaltable.businessdate ReferencedBusinessDate
    , cast({run_date!r} as date) BusinessDate
from dpo_finaltable 
left join portfolio
on portfolio.portfolioid = dpo_finaltable.PortfolioId     
''')


# COMMAND ----------

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

if unity == "True":
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
