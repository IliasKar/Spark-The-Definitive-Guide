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
table_name = 'DLienAnalysisPropertyContract'

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
            "DLienAnalysisPropertyContractId string,"
            "SourceSystem string,"
            "PortfolioKey string,"
            "PortfolioName string,"
            "IngestionTimestamp timestamp,"
            "BatchId string,"
            "DLienAnalysisPropertyContractKey string,"
            "AgreementKey string,"
            "RealEstateCepalKey string,"
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
            "RunDate date,"
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
SELECT 
    StagingKey,
    StagingFlowProcessExecutionKey,
    PortfolioId,
    PropertyId,
    AgreementNumber,
    Primary_customer_cdi,
    dataIssues,
    Base_LV_BorrowerREV_byOwnership,
    Base_LV_REV_Deducted_Legal_Expenses,
    Base_LV_Claim_Deducted_Legal_Expenses,
    Base_LV_Secured_Recoverable_Amount,
    Base_LV_Recoverable_Amount_from_Unsec_Claims,
    Preferrential_Claims,
    LV_Net_Base,
    LV_Base_Net_NPV,
    LV_Base_Gross_NPV,
    LV_Gross_Base,
    LV_1stAuction_Net,
    LV_2ndAuction_Net,
    LV_3rdAuction_Net,
    LV_4thAuction_Net,
    Exp_Net_LV,
    NPV_Exp_Net_LV,
    LV_1stAuction_Gross,
    LV_2ndAuction_Gross,
    LV_3rdAuction_Gross,
    LV_4thAuction_Gross,
    Exp_Gross_LV,
    NPV_Exp_Gross_LV,
    Time_to_1stAuction,
    Time_to_Collect,
    CREV_Base_TLC,
    CREV_Base,
    CREV_1stAuction,
    CREV_2ndAuction,
    CREV_3rdAuction,
    CREV_4thAuction,
    Exp_CREV,
    Alloc_REV_Base,
    AllocREV_1stAuction,
    AllocREV_2ndAuction,
    AllocREV_3rdAuction,
    AllocREV_4thAuction,
    Exp_Alloc_REV,
    RunDate,
    BusinessDate
FROM  `{env}-cdh`.landed.biteam_biteamlienanalysispropertycontract
WHERE BusinessDate = {run_date!r}
'''
)

biteam_biteamlienanalysispropertycontract.createOrReplaceTempView("biteam_biteamlienanalysispropertycontract")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

portfolio = spark.sql(
f'''
SELECT 
  PortfolioName,
  Prefix,
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
        substring(biteam_biteamlienanalysispropertycontract["Primary_customer_cdi"],1, 3) == portfolio["Prefix"],
        "left",
    )
)


# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF THE FIRST DATAFRAME OF THE UNION
# ================== biteam_biteamlienanalysispropertycontract_df ==================

biteam_biteamlienanalysispropertycontract_df_primary_key_fields = [
    biteam_biteamlienanalysispropertycontract.StagingKey,
    biteam_biteamlienanalysispropertycontract.StagingFlowProcessExecutionKey,
    lit("biteam"),
]

biteam_biteamlienanalysispropertycontract_df_key_fields = [
    biteam_biteamlienanalysispropertycontract.StagingKey,
    biteam_biteamlienanalysispropertycontract.StagingFlowProcessExecutionKey,
]

biteam_biteamlienanalysispropertycontract_df = (
    biteam_biteamlienanalysispropertycontract_df.withColumn(
        "DLienAnalysisPropertyContractID",
        sha2(
            concat_ws(
                ";", *biteam_biteamlienanalysispropertycontract_df_primary_key_fields
            ),
            256,
        ),
    )
    .withColumn(
        "DLienAnalysisPropertyContractKey",
        concat_ws(
                ";", *biteam_biteamlienanalysispropertycontract_df_key_fields
        ),
    )
)

# COMMAND ----------

dlienanalysispropertycontract_df = (
            biteam_biteamlienanalysispropertycontract_df.select(
                biteam_biteamlienanalysispropertycontract_df.DLienAnalysisPropertyContractID.cast("string"),
                lit("biteam").cast("string").alias("SourceSystem"),
                portfolio.unit_code_profits.cast("string").alias("PortfolioKey"),
                portfolio.PortfolioName.cast("string").alias("PortfolioName"),
                lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"),
                lit(None).cast("String").alias("BatchId"),
                biteam_biteamlienanalysispropertycontract_df.DLienAnalysisPropertyContractKey.cast("string").alias("DLienAnalysisPropertyContractKey"),
                biteam_biteamlienanalysispropertycontract.AgreementNumber.cast("string").alias("AgreementKey"),
                biteam_biteamlienanalysispropertycontract.PropertyId.cast("string").alias("RealEstateCepalKey"),
                biteam_biteamlienanalysispropertycontract.Primary_customer_cdi.cast("string").alias("PrimaryPartyCDI"),
                biteam_biteamlienanalysispropertycontract.dataIssues.cast("string").alias("DataIssues"),
                biteam_biteamlienanalysispropertycontract.Base_LV_BorrowerREV_byOwnership.cast("double").alias("BaseLVBorrowerREVbyOwnership"),
                biteam_biteamlienanalysispropertycontract.Base_LV_REV_Deducted_Legal_Expenses.cast("double").alias("BaseLVREVDeductedLegalExpenses"),
                biteam_biteamlienanalysispropertycontract.Base_LV_Claim_Deducted_Legal_Expenses.cast("double").alias("BaseLVClaimDeductedLegalExpenses"),
                biteam_biteamlienanalysispropertycontract.Base_LV_Secured_Recoverable_Amount.cast("double").alias("BaseLVSecuredRecoverableAmount"),
                biteam_biteamlienanalysispropertycontract.Base_LV_Recoverable_Amount_from_Unsec_Claims.cast("double").alias("BaseLVRecoverableAmountFromUnsecClaims"),
                biteam_biteamlienanalysispropertycontract.Preferrential_Claims.cast("double").alias("PreferrentialClaims"),
                biteam_biteamlienanalysispropertycontract.LV_Net_Base.cast("double").alias("LVNetBase"),
                biteam_biteamlienanalysispropertycontract.LV_Base_Net_NPV.cast("double").alias("LVBaseNetNPV"),
                biteam_biteamlienanalysispropertycontract.LV_Base_Gross_NPV.cast("double").alias("LVBase_GrossNPV"),
                biteam_biteamlienanalysispropertycontract.LV_Gross_Base.cast("double").alias("LVGrossBase"),
                biteam_biteamlienanalysispropertycontract.LV_1stAuction_Net.cast("double").alias("LV1stAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LV_2ndAuction_Net.cast("double").alias("LV2ndAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LV_3rdAuction_Net.cast("double").alias("LV3rdAuctionNet"),
                biteam_biteamlienanalysispropertycontract.LV_4thAuction_Net.cast("double").alias("LV4thAuctionNet"),
                biteam_biteamlienanalysispropertycontract.Exp_Net_LV.cast("double").alias("ExpNetLV"),
                biteam_biteamlienanalysispropertycontract.NPV_Exp_Net_LV.cast("double").alias("NPVExpNetLV"),
                biteam_biteamlienanalysispropertycontract.LV_1stAuction_Gross.cast("double").alias("LV1stAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LV_2ndAuction_Gross.cast("double").alias("LV2ndAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LV_3rdAuction_Gross.cast("double").alias("LV3rdAuctionGross"),
                biteam_biteamlienanalysispropertycontract.LV_4thAuction_Gross.cast("double").alias("LV4thAuctionGross"),
                biteam_biteamlienanalysispropertycontract.Exp_Gross_LV.cast("double").alias("ExpGrossLV"),
                biteam_biteamlienanalysispropertycontract.NPV_Exp_Gross_LV.cast("double").alias("NPVExpGrossLV"),
                biteam_biteamlienanalysispropertycontract.Time_to_1stAuction.cast("int").alias("TimeTo1stAuction"),
                biteam_biteamlienanalysispropertycontract.Time_to_Collect.cast("int").alias("TimeToCollect"),
                biteam_biteamlienanalysispropertycontract.CREV_Base_TLC.cast("double").alias("CREVBaseTLC"),
                biteam_biteamlienanalysispropertycontract.CREV_Base.cast("double").alias("CREVBase"),
                biteam_biteamlienanalysispropertycontract.CREV_1stAuction.cast("double").alias("CREV1stAuction"),
                biteam_biteamlienanalysispropertycontract.CREV_2ndAuction.cast("double").alias("CREV2ndAuction"),
                biteam_biteamlienanalysispropertycontract.CREV_3rdAuction.cast("double").alias("CREV3rdAuction"),
                biteam_biteamlienanalysispropertycontract.CREV_4thAuction.cast("double").alias("CREV4thAuction"),
                biteam_biteamlienanalysispropertycontract.Exp_CREV.cast("double").alias("ExpCREV"),
                biteam_biteamlienanalysispropertycontract.Alloc_REV_Base.cast("double").alias("AllocREVBase"),
                biteam_biteamlienanalysispropertycontract.AllocREV_1stAuction.cast("double").alias("AllocREV1stAuction"),
                biteam_biteamlienanalysispropertycontract.AllocREV_2ndAuction.cast("double").alias("AllocREV2ndAuction"),
                biteam_biteamlienanalysispropertycontract.AllocREV_3rdAuction.cast("double").alias("AllocREV3rdAuction"),
                biteam_biteamlienanalysispropertycontract.AllocREV_4thAuction.cast("double").alias("AllocREV4thAuction"),
                biteam_biteamlienanalysispropertycontract.Exp_Alloc_REV.cast("double").alias("ExpAllocREV"),
                biteam_biteamlienanalysispropertycontract.RunDate.cast("date").alias("RunDate"),
                biteam_biteamlienanalysispropertycontract.BusinessDate.cast("date").alias("BusinessDate"),
            )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

# DBTITLE 1,WRITE TO DELTA TABLE
if unity == "True":
    dlienanalysispropertycontract_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    dlienanalysispropertycontract_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
