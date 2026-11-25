# Databricks notebook source
import logging
# MAGIC %md
# MAGIC ***Important note!***
# MAGIC *The Crev of closed loans seems to be null. So, for the historical calculation of Sum of Crev of closed loans, Crev is considered as 0 (coalesce(CRev,0)).*

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

# import boto3
import os

from pyspark.sql.functions import (
    coalesce,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    sum,
)
import delta_table_creator, extract, transform
from delta import DeltaTable  # type: ignore
from pyspark.sql import DataFrame

from sql_data_loader import load_dataframe_from_sql_server

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-01-31'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2023-12-31')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

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
# table_name = dbutils.widgets.get("table_name") 
table_name = 'DAnalyticsMetrics_historical'
unity = dbutils.widgets.get("unity") 

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
# MAGIC ##### Initialize the schema
# MAGIC

# COMMAND ----------

# DBTITLE 1,SCHEMA INITIALIZATION
schema = (
            "DAnalyticsMetricsId string,"
            "SourceSystem string,"
            "PortfolioKey string,"
            "PortfolioName string,"
            "IngestionTimestamp timestamp,"
            "BatchId string,"
            "DAnalyticsMetricsKey string,"
            "AgreementKey string,"
            "RealEstateKey string,"
            "ExposureKey string,"
            "PartyTaxRegistrationNumber string,"
            "PrimaryPartyCDI string,"
            "DataIssues string,"
            "BaseLVBorrowerREVbyOwnership double,"
            "BaseLVREVDeductedLegalExpenses double,"
            "BaseLVClaimDeductedLegalExpenses double,"
            "BaseLVSecuredRecoverableAmount double,"
            "BaseLVRecoverableAmountFromUnsecClaims double,"
            "PreferrentialClaims double,"
            "LVNetBase double,"
            "LVBaseNetNPV double,"
            "LVBase_GrossNPV double,"
            "LVGrossBase double,"
            "LV1stAuctionNet double,"
            "LV2ndAuctionNet double,"
            "LV3rdAuctionNet double,"
            "LV4thAuctionNet double,"
            "ExpNetLV double,"
            "NPVExpNetLV double,"
            "LV1stAuctionGross double,"
            "LV2ndAuctionGross double,"
            "LV3rdAuctionGross double,"
            "LV4thAuctionGross double,"
            "ExpGrossLV double,"
            "NPVExpGrossLV double,"
            "TimeTo1stAuction int,"
            "TimeToCollect int,"
            "CREVBaseTLC double,"
            "CREVBase double,"
            "CREV1stAuction double,"
            "CREV2ndAuction double,"
            "CREV3rdAuction double,"
            "CREV4thAuction double,"
            "ExpCREV double,"
            "AllocREVBase double,"
            "AllocREV1stAuction double,"
            "AllocREV2ndAuction double,"
            "AllocREV3rdAuction double,"
            "AllocREV4thAuction double,"
            "ExpAllocREV double,"
            "PayerFlagEntity string,"
            "PayerFlagAccount string,"
            "PayerFlagAgreement string,"
            "RunDate date,"
            "BusinessDate date"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

# DBTITLE 1,CREATE - DELETE DELTA TABLE
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
# MAGIC ##### Create wanted dataframes for each table needed

# COMMAND ----------

Assetmetrics = load_dataframe_from_sql_server("dwhcore","dwh.evn.AssetMetrics")
Assetmetrics.createOrReplaceTempView("AssetMetrics")
 
Asset = load_dataframe_from_sql_server("dwhcore","dwh.ast.Asset")
Asset.createOrReplaceTempView("Asset")

Party = load_dataframe_from_sql_server("dwhcore","dwh.prt.Party")
Party.createOrReplaceTempView("Party")

Portfolio = load_dataframe_from_sql_server("dwhcore","dwh.ref.Portfolio")
Portfolio.createOrReplaceTempView("Portfolio")

Classification = load_dataframe_from_sql_server("dwhcore","dwh.cls.Classification")
Classification.createOrReplaceTempView("Classification")

# COMMAND ----------

hist = spark.sql(f'''
             select agr.AssetKey as AgreementKey
                , pr.AssetKey as PropertyKey
                , portfolio.unit_code_profits
                , portfolio.PortfolioName
                , sum(coalesce(am.Crev,0)) as SumOfCrev --use of coalesce since Crev is null for closed loans
                , {run_date!r} as BusinessDate
             from AssetMetrics am
             left join Asset as agr on agr.AssetId=am.AgreementId and agr.CurrentRecord=1
             left join Asset as pr on pr.AssetId=am.PropertyId and pr.CurrentRecord=1
             left join Portfolio on portfolio.PortfolioId = agr.PortfolioId
             left join Classification on Classification.ClassificationId = am.EventMetricsTypeId
             where am.EffectiveDate <= {run_date!r} and am.EndDate > {run_date!r}
             and Classification.Value = 'crev_account'
             group by AgreementKey, PropertyKey, portfolio.unit_code_profits, portfolio.PortfolioName, BusinessDate
             ''')

# COMMAND ----------

biteam_hist_df_primary_key_fields = [
    hist.AgreementKey,
    hist.PropertyKey,
    lit("biteam"),
]

biteam_hist_df_key_fields = [
    hist.AgreementKey,
    hist.PropertyKey,
]

hist = hist.withColumn("DAnalyticsMetricsID",
        sha2(
            concat_ws(";", *biteam_hist_df_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DAnalyticsMetricsKey",
        concat_ws(";", *biteam_hist_df_key_fields),
    )

# COMMAND ----------

result_df = (hist.select(
                hist.DAnalyticsMetricsID.cast("string").alias("DAnalyticsMetricsID"),
                lit("biteam").cast("string").alias("SourceSystem"),
                hist.unit_code_profits.cast("string").alias("PortfolioKey"),
                hist.PortfolioName.cast("string").alias("PortfolioName"),
                lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
                lit(None).cast("String").alias("BatchId"),
                hist.DAnalyticsMetricsKey.cast("string").alias("DAnalyticsMetricsKey"),
                hist.AgreementKey.cast("string").alias("AgreementKey"),
                hist.PropertyKey.cast("string").alias("RealEstateKey"),
                lit(None).cast("string").alias("ExposureKey"),
                lit(None).cast("string").alias("PartyTaxRegistrationNumber"),
                lit(None).cast("string").alias("PrimaryPartyCDI"),
                lit(None).cast("string").alias("DataIssues"),
                lit(None).cast("double").alias("BaseLVBorrowerREVbyOwnership"),
                lit(None).cast("double").alias("BaseLVREVDeductedLegalExpenses"),
                lit(None).cast("double").alias("BaseLVClaimDeductedLegalExpenses"),
                lit(None).cast("double").alias("BaseLVSecuredRecoverableAmount"),
                lit(None).cast("double").alias("BaseLVRecoverableAmountFromUnsecClaims"),
                lit(None).cast("double").alias("PreferrentialClaims"),
                lit(None).cast("double").alias("LVNetBase"),
                lit(None).cast("double").alias("LVBaseNetNPV"),
                lit(None).cast("double").alias("LVBase_GrossNPV"),
                lit(None).cast("double").alias("LVGrossBase"),
                lit(None).cast("double").alias("LV1stAuctionNet"),
                lit(None).cast("double").alias("LV2ndAuctionNet"),
                lit(None).cast("double").alias("LV3rdAuctionNet"),
                lit(None).cast("double").alias("LV4thAuctionNet"),
                lit(None).cast("double").alias("ExpNetLV"),
                lit(None).cast("double").alias("NPVExpNetLV"),
                lit(None).cast("double").alias("LV1stAuctionGross"),
                lit(None).cast("double").alias("LV2ndAuctionGross"),
                lit(None).cast("double").alias("LV3rdAuctionGross"),
                lit(None).cast("double").alias("LV4thAuctionGross"),
                lit(None).cast("double").alias("ExpGrossLV"),
                lit(None).cast("double").alias("NPVExpGrossLV"),
                lit(None).cast("int").alias("TimeTo1stAuction"),
                lit(None).cast("int").alias("TimeToCollect"),
                hist.SumOfCrev.cast("double").alias("CREVBaseTLC"),
                lit(None).cast("double").alias("CREVBase"),
                lit(None).cast("double").alias("CREV1stAuction"),
                lit(None).cast("double").alias("CREV2ndAuction"),
                lit(None).cast("double").alias("CREV3rdAuction"),
                lit(None).cast("double").alias("CREV4thAuction"),
                lit(None).cast("double").alias("ExpCREV"),
                lit(None).cast("double").alias("AllocREVBase"),
                lit(None).cast("double").alias("AllocREV1stAuction"),
                lit(None).cast("double").alias("AllocREV2ndAuction"),
                lit(None).cast("double").alias("AllocREV3rdAuction"),
                lit(None).cast("double").alias("AllocREV4thAuction"),
                lit(None).cast("double").alias("ExpAllocREV"),
                lit(None).cast("string").alias("PayerFlagEntity"),
                lit(None).cast("string").alias("PayerFlagAccount"),
                lit(None).cast("string").alias("PayerFlagAgreement"),
                lit(None).cast("date").alias("RunDate"),
                hist.BusinessDate.cast("date").alias("BusinessDate"),
            )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

# DBTITLE 1,WRITE TO DELTA TABLE
if unity == "True":
    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    result_df.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
