# Databricks notebook source
import logging
import delta_table_creator, extract, transform
import boto3
import os
from pyspark.sql.functions import (
    coalesce,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
)

from delta import DeltaTable  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql.functions import trim

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-01-31'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-01-31')
# dbutils.widgets.text("table_name", 'DPortfolioToolCore')
# dbutils.widgets.text("hive_unity_indicator", 'hive')
# dbutils.widgets.text("utp_date", '2024-01-17')

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
table_name = 'DAnalyticsMetrics'
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
            "LVUpliftNPV double,"
            "LVUplift double,"
            "cREVGBVUplift double,"
            "cREVTLCUplift double,"
            "PayerFlagEntity string,"
            "PayerFlagAccount string,"
            "PayerFlagAgreement string,"
            "RunDate date,"
            "ReferencedBusinessDate date,"
            "BusinessDate date"
        )

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
# MAGIC ##### Create wanted Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### biteam_biteamlienanalysispropertycontract_extra_filters

# COMMAND ----------

biteam_biteamlienanalysispropertycontract = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT PortfolioId, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.lien_analysis
  WHERE Granularity = 'RealEstateAgreement'
  AND BusinessDate <= {run_date!r}
  GROUP BY PortfolioId
), 
biteam_biteamlienanalysispropertycontract AS(
SELECT 
    lien.PortfolioId,
    RealEstateCepalKey,
    AgreementKey,
    PrimaryDebtorCDI,
    BaseLVBorrowerREVbyOwnership,
    BaseLVREVDeductedLegalExpenses,
    BaseLVClaimDeductedLegalExpenses,
    BaseLVSecuredRecoverableAmount,
    BaseLVRecoverableAmountfromUnsecClaims,
    PreferrentialClaims,
    LVNetBase,
    LVBaseNetNPV,
    LVBaseGrossNPV,
    LVGrossBase,
    LVFirstAuctionNet,
    LVSecondAuctionNet,
    LVThirdAuctionNet,
    LVFourthAuctionNet,
    ExpectedNetLV,
    NPVExpectedNetLV,
    LVFirstAuctionGross,
    LVSecondAuctionGross,
    LVThirdAuctionGross,
    LVFourthAuctionGross,
    ExpectedGrossLV,
    NPVExpectedGrossLV,
    TimetoFirstAuction,
    TimetoCollect,
    CREVBaseTLC,
    CREVBaseGBV,
    CREVFirstAuction,
    CREVSecondAuction,
    CREVThirdAuction,
    CREVFourthAuction,
    ExpectedCREV,
    AllocatedREVBase,
    AllocatedREVFirstAuction,
    AllocatedREVSecondAuction,
    AllocatedREVThirdAuction,
    AllocatedREVFourthAuction,
    ExpectedAllocatedREV,
    LVUpliftNPV,
    LVUplift,
    cREVGBVUplift,
    cREVTLCUplift,
    IngestionTimestamp,
    BusinessDate,
    row_number() OVER(PARTITION BY AgreementKey, PrimaryDebtorCDI, RealEstateCepalKey ORDER BY IngestionTimestamp DESC) rn --keep the latest record for each combination of agreement, cdi and realestate
FROM  `{env}-ds`.aaas.lien_analysis lien
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON lien.PortfolioId = maxdate.PortfolioId
AND lien.BusinessDate = maxdate.MaxBusinessDate
WHERE lien.Granularity = 'RealEstateAgreement'
)
SELECT * FROM biteam_biteamlienanalysispropertycontract WHERE rn = 1;
'''
)

biteam_biteamlienanalysispropertycontract.createOrReplaceTempView("biteam_biteamlienanalysispropertycontract")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### biteam_pf_final_output

# COMMAND ----------

biteam_pf_final_output = spark.sql(
f'''
WITH portfolio_max_businessdate AS(
  SELECT Portfolio_Id, MAX(BusinessDate) as MaxBusinessDate
  FROM `{env}-ds`.aaas.payers_flag_algorithm
  WHERE BusinessDate <= {run_date!r}
  GROUP BY Portfolio_Id
),
biteam_pf_final_output AS (
SELECT 
  Account_Number_with_Prefix,
  PartyTaxRegistrationNumber,
  CDI,
  AgreementNumber,
  payerflag.Portfolio_Id,
  Payer_Flag_Entity,
  Payer_Flag_Account,
  Payer_flag_Aggrement,
  BusinessDate,
  row_number() OVER(PARTITION BY Account_Number_with_Prefix ORDER BY Ref_Date DESC, Upload_date DESC) rn --keep the latest record for each account number
FROM  `{env}-ds`.aaas.payers_flag_algorithm payerflag
JOIN portfolio_max_businessdate maxdate --keep the max businessdate data for each portfolio
ON payerflag.Portfolio_Id = maxdate.Portfolio_Id
AND payerflag.BusinessDate = maxdate.MaxBusinessDate
)
SELECT * FROM biteam_pf_final_output WHERE rn = 1;
'''
)

biteam_pf_final_output.createOrReplaceTempView("biteam_pf_final_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT
  PortfolioId,
  PortfolioName,
  unit_code_profits
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# DBTITLE 1,JOINS
# ================== Joins ==================
biteam_biteamlienanalysispropertycontract_df = (
    biteam_biteamlienanalysispropertycontract.join(
        portfolio,
        biteam_biteamlienanalysispropertycontract["PortfolioId"] == portfolio["PortfolioId"],
        "left",
    )
)

biteam_pf_final_output_df = (
    biteam_pf_final_output.join(
        portfolio,
        biteam_pf_final_output["Portfolio_Id"] == portfolio["PortfolioId"],
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF THE FIRST DATAFRAME OF THE UNION
# ================== biteam_biteamlienanalysispropertycontract_df ==================

biteam_biteamlienanalysispropertycontract_df_primary_key_fields = [
    biteam_biteamlienanalysispropertycontract.AgreementKey,
    biteam_biteamlienanalysispropertycontract.PrimaryDebtorCDI,
    biteam_biteamlienanalysispropertycontract.RealEstateCepalKey,
    lit("biteam"),
]

biteam_biteamlienanalysispropertycontract_df_danalyticsmetrics_key_fields = [
    biteam_biteamlienanalysispropertycontract.AgreementKey,
    biteam_biteamlienanalysispropertycontract.PrimaryDebtorCDI,
    biteam_biteamlienanalysispropertycontract.RealEstateCepalKey,
]

biteam_biteamlienanalysispropertycontract_df = (
    biteam_biteamlienanalysispropertycontract_df.withColumn(
        "DAnalyticsMetricsID",
        sha2(
            concat_ws(
                ";", *biteam_biteamlienanalysispropertycontract_df_primary_key_fields
            ),
            256,
        ),
    )
    .withColumn(
        "DAnalyticsMetricsKey",
        concat_ws(
                ";", *biteam_biteamlienanalysispropertycontract_df_danalyticsmetrics_key_fields
        ),
    )
)

# COMMAND ----------

danalyticsmetrics_first_df = (
            biteam_biteamlienanalysispropertycontract_df.select(
                biteam_biteamlienanalysispropertycontract_df.DAnalyticsMetricsID.cast("string"),
                lit("biteam").cast("string").alias("SourceSystem"),
                portfolio.unit_code_profits.cast("string").alias("PortfolioKey"),
                portfolio.PortfolioName.cast("string").alias("PortfolioName"),
                lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
                lit(None).cast("String").alias("BatchId"),
                biteam_biteamlienanalysispropertycontract_df.DAnalyticsMetricsKey.cast("string").alias("DAnalyticsMetricsKey"),
                biteam_biteamlienanalysispropertycontract.AgreementKey.cast("string").alias("AgreementKey"),
                biteam_biteamlienanalysispropertycontract.RealEstateCepalKey.cast("string").alias("RealEstateKey"),
                lit(None).cast("string").alias("ExposureKey"),
                lit(None).cast("string").alias("PartyTaxRegistrationNumber"),
                biteam_biteamlienanalysispropertycontract.PrimaryDebtorCDI.cast("string").alias("PrimaryPartyCDI"),
                lit(None).cast("string").alias("DataIssues"),
                biteam_biteamlienanalysispropertycontract.BaseLVBorrowerREVbyOwnership.cast("double").alias("BaseLVBorrowerREVbyOwnership"),
                biteam_biteamlienanalysispropertycontract.BaseLVREVDeductedLegalExpenses.cast("double").alias("BaseLVREVDeductedLegalExpenses"),
                biteam_biteamlienanalysispropertycontract.BaseLVClaimDeductedLegalExpenses.cast("double").alias("BaseLVClaimDeductedLegalExpenses"),
                biteam_biteamlienanalysispropertycontract.BaseLVSecuredRecoverableAmount.cast("double").alias("BaseLVSecuredRecoverableAmount"),
                biteam_biteamlienanalysispropertycontract.BaseLVRecoverableAmountfromUnsecClaims.cast("double").alias("BaseLVRecoverableAmountFromUnsecClaims"),
                biteam_biteamlienanalysispropertycontract.PreferrentialClaims.cast("double").alias("PreferrentialClaims"),
                biteam_biteamlienanalysispropertycontract.LVNetBase.cast("double").alias("LVNetBase"),
                biteam_biteamlienanalysispropertycontract.LVBaseNetNPV.cast("double").alias("LVBaseNetNPV"),
                biteam_biteamlienanalysispropertycontract.LVBaseGrossNPV.cast("double").alias("LVBase_GrossNPV"),
                biteam_biteamlienanalysispropertycontract.LVGrossBase.cast("double").alias("LVGrossBase"),
                biteam_biteamlienanalysispropertycontract.LVFirstAuctionNet.cast("double").alias("LV1stAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LVSecondAuctionNet.cast("double").alias("LV2ndAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LVThirdAuctionNet.cast("double").alias("LV3rdAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LVFourthAuctionNet.cast("double").alias("LV4thAuctionNet"),
                biteam_biteamlienanalysispropertycontract.ExpectedNetLV.cast("double").alias("ExpNetLV"),
                biteam_biteamlienanalysispropertycontract.NPVExpectedNetLV.cast("double").alias("NPVExpNetLV"),
                biteam_biteamlienanalysispropertycontract.LVFirstAuctionGross.cast("double").alias("LV1stAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LVSecondAuctionGross.cast("double").alias("LV2ndAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LVThirdAuctionGross.cast("double").alias("LV3rdAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LVFourthAuctionGross.cast("double").alias("LV4thAuctionGross"),
                biteam_biteamlienanalysispropertycontract.ExpectedGrossLV.cast("double").alias("ExpGrossLV"),
                biteam_biteamlienanalysispropertycontract.NPVExpectedGrossLV.cast("double").alias("NPVExpGrossLV"),
                biteam_biteamlienanalysispropertycontract.TimetoFirstAuction.cast("int").alias("TimeTo1stAuction"),
                biteam_biteamlienanalysispropertycontract.TimetoCollect.cast("int").alias("TimeToCollect"),
                biteam_biteamlienanalysispropertycontract.CREVBaseTLC.cast("double").alias("CREVBaseTLC"),
                biteam_biteamlienanalysispropertycontract.CREVBaseGBV.cast("double").alias("CREVBase"),
                biteam_biteamlienanalysispropertycontract.CREVFirstAuction.cast("double").alias("CREV1stAuction"),
                biteam_biteamlienanalysispropertycontract.CREVSecondAuction.cast("double").alias("CREV2ndAuction"),
                biteam_biteamlienanalysispropertycontract.CREVThirdAuction.cast("double").alias("CREV3rdAuction"),
                biteam_biteamlienanalysispropertycontract.CREVFourthAuction.cast("double").alias("CREV4thAuction"),
                biteam_biteamlienanalysispropertycontract.ExpectedCREV.cast("double").alias("ExpCREV"),
                biteam_biteamlienanalysispropertycontract.AllocatedREVBase.cast("double").alias("AllocREVBase"),
                biteam_biteamlienanalysispropertycontract.AllocatedREVFirstAuction.cast("double").alias("AllocREV1stAuction"),
                biteam_biteamlienanalysispropertycontract.AllocatedREVSecondAuction.cast("double").alias("AllocREV2ndAuction"),
                biteam_biteamlienanalysispropertycontract.AllocatedREVThirdAuction.cast("double").alias("AllocREV3rdAuction"),
                biteam_biteamlienanalysispropertycontract.AllocatedREVFourthAuction.cast("double").alias("AllocREV4thAuction"),
                biteam_biteamlienanalysispropertycontract.ExpectedAllocatedREV.cast("double").alias("ExpAllocREV"),
                biteam_biteamlienanalysispropertycontract.LVUpliftNPV.cast("double").alias("LVUpliftNPV"),
                biteam_biteamlienanalysispropertycontract.LVUplift.cast("double").alias("LVUplift"),
                biteam_biteamlienanalysispropertycontract.cREVGBVUplift.cast("double").alias("cREVGBVUplift"),
                biteam_biteamlienanalysispropertycontract.cREVTLCUplift.cast("double").alias("cREVTLCUplift"),
                lit(None).cast("string").alias("PayerFlagEntity"),
                lit(None).cast("string").alias("PayerFlagAccount"),
                lit(None).cast("string").alias("PayerFlagAgreement"),
                biteam_biteamlienanalysispropertycontract.IngestionTimestamp.cast("date").alias("RunDate"),
                biteam_biteamlienanalysispropertycontract.BusinessDate.cast("date").alias("ReferencedBusinessDate"),
                lit(run_date).cast("date").alias("BusinessDate"),
            )
)

# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF THE SECOND DATAFRAME OF THE UNION
# ================== biteam_pf_final_output_df ==================

biteam_pf_final_output_df_primary_key_fields = [
    biteam_pf_final_output.Account_Number_with_Prefix,
    lit("biteam"),
]

biteam_pf_final_output_df = (
    biteam_pf_final_output_df.withColumn(
        "DAnalyticsMetricsID",
        sha2(
            concat_ws(
                ";", *biteam_pf_final_output_df_primary_key_fields
            ),
            256,
        ),
    )
)

# COMMAND ----------

danalyticsmetrics_second_df = (
            biteam_pf_final_output_df.select(
                biteam_pf_final_output_df.DAnalyticsMetricsID.cast("string"),
                lit("biteam").cast("string").alias("SourceSystem"),
                portfolio.unit_code_profits.cast("string").alias("PortfolioKey"),
                portfolio.PortfolioName.cast("string").alias("PortfolioName"),
                lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
                lit(None).cast("String").alias("BatchId"),
                biteam_pf_final_output.Account_Number_with_Prefix.cast("string").alias("DAnalyticsMetricsKey"),
                biteam_pf_final_output.AgreementNumber.cast("string").alias("AgreementKey"),
                lit(None).cast("string").alias("RealEstateKey"),
                biteam_pf_final_output.Account_Number_with_Prefix.cast("string").alias("ExposureKey"),
                biteam_pf_final_output.PartyTaxRegistrationNumber.cast("string").alias("PartyTaxRegistrationNumber"),
                biteam_pf_final_output.CDI.cast("string").alias("PrimaryPartyCDI"),
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
                lit(None).cast("double").alias("CREVBaseTLC"),
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
                lit(None).cast("double").alias("LVUpliftNPV"),
                lit(None).cast("double").alias("LVUplift"),
                lit(None).cast("double").alias("cREVGBVUplift"),
                lit(None).cast("double").alias("cREVTLCUplift"),
                biteam_pf_final_output.Payer_Flag_Entity.cast("string").alias("PayerFlagEntity"),
                biteam_pf_final_output.Payer_Flag_Account.cast("string").alias("PayerFlagAccount"),
                biteam_pf_final_output.Payer_flag_Aggrement.cast("string").alias("PayerFlagAgreement"),
                lit(None).cast("date").alias("RunDate"),
                biteam_pf_final_output.BusinessDate.cast("date").alias("ReferencedBusinessDate"),
                lit(run_date).cast("date").alias("BusinessDate"),
            )
)

# COMMAND ----------

# DBTITLE 1,UNION & APPEND
danalyticsmetrics_result_df = danalyticsmetrics_first_df.union(danalyticsmetrics_second_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

# DBTITLE 1,WRITE TO DELTA TABLE
if unity == "True":
    danalyticsmetrics_result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    danalyticsmetrics_result_df.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
