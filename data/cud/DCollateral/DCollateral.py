# Databricks notebook source
import logging
from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    trim,
    when,
    months_between,
    dateadd,
    datediff,
)

import os
# env = os.environ["S3PREFIX"] 
# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()
import delta_table_creator, transform

# COMMAND ----------

schema=(
    "DCollateralId string,"
    "SourceSystem string,"
    "PortfolioKey string,"
    "PortfolioName string,"
    "IngestionTimestamp timestamp,"
    "BatchId string,"
    "DCollateralKey string,"
    "CollateralOldKey string,"
    "CollateralCode string,"
    "Comment string,"
    "CollateralAmount decimal(19,2),"
    "Currency string,"
    "CollateralStatusCode string,"
    "CollateralStatus string,"
    "StatusDate date,"
    "MortgageTypeCode string,"
    "MortgageType string,"
    "MortgageTypeENG string,"
    "AgreementKey string,"
    "CategoryCode string,"
    "Category string,"
    "CollateralTypeDescription string,"
    "SubCategoryCode string,"
    "SubCategory string,"
    "OwnerPartyKey string,"
    "RealEstateKey string,"
    "NonRealEstateKey string,"
    "OwnershipKey int,"
    "RealTitleDescription string,"
    "RealEstateStatus string,"
    "CollateralType string,"
    "OwnershipPercentage double,"
    "OwnershipStatusCode string,"
    "OwnershipStatus string,"
    "OwnershipStatusDate date,"
    "NotaryNumber string,"
    "NotaryDate date,"
    "KtisisWayKey string,"
    "RealEstateCollateralKey string,"
    "RealEstateCollateralStatusCode string,"
    "RealEstateCollateralStatus string,"
    "InitialLegalOrder int,"
    "LawyerPartyKey string,"
    "JudgementNumber string,"
    "JudgementDate date,"
    "REMortgageRegistrationDate date,"
    "RegistryNumber string,"
    "LandRegistry int,"
    "TranscriptNumber string,"
    "TranscriptVolume string,"
    "TranscriptSheet string,"
    "CreditorPartyKey string,"
    "CourtNumber int,"
    "BusinessDate date"
)

partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "DCollateral"
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

# Create temp view for each source table, based on the run date

portfolio = spark.sql(f""" select *
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
"""
) 

portfolio.createOrReplaceTempView("portfolio")

collateral_all = spark.sql(f""" select *
FROM  `{env}-cdh`.primary.collateral
WHERE BusinessDate = {run_date!r}
"""
) 

collateral_all.createOrReplaceTempView("collateral_all")

realestatebuilding = spark.sql(f""" 
select distinct 
    RealEstateKey,
    RealEstateStatus,
    case when sourcesystem = 'arotron' then 'qca' else sourcesystem end as sourcesystemmapping
FROM  `{env}-cdh`.primary.realestatebuilding
WHERE BusinessDate = {run_date!r}
"""
) 

realestatebuilding.createOrReplaceTempView("realestatebuilding")

dktranslationmapping = spark.sql(f""" select *
FROM  `{env}-cdh`.reference.dktranslationmapping
WHERE Category='CollateralType'
"""
) 

dktranslationmapping.createOrReplaceTempView("dktranslationmapping")

# COMMAND ----------

# DBTITLE 1,Unification of collaterals between source systems
collateral = spark.sql(f""" 
    select c.*   
    FROM  collateral_all c
    WHERE sourcesystem = 'thesis'

    union all

    select c.*   
    FROM  collateral_all c
    join portfolio p on p.portfolio_id_qca = c.PortfolioKey
    WHERE p.unit_code_profits is null

"""
) 

collateral.createOrReplaceTempView("collateral")

# COMMAND ----------

target_table_dcollateral_pre = spark.sql(f""" SELECT
    c.CollateralId ,
    c.SourceSystem,
    c.PortfolioKey,
    c.PortfolioName,
    c.CollateralKey,
    c.CollateralOldKey,
    c.CollateralCode,
    c.Comment,
    c.CollateralAmount,
    c.CollateralCurrency,
    c.CollateralStatusCode,
    c.StatusDate,
    c.MortgageTypeCode,
    c.MortgageType,
    c.AgreementKey,
    c.CategoryCode,
    c.Category,
    c.SubCategoryCode,
    c.SubCategory,
    c.OwnerPartyKey,
    c.RealEstateKey,
    c.NonRealEstateKey,
    c.OwnershipKey,
    c.RealTitleDescription,
    c.OwnershipPercentage,
    c.OwnershipStatusCode,
    c.OwnershipStatus,
    c.OwnershipStatusDate,
    c.NotaryNumber,
    c.NotaryDate,
    c.KtisisWayKey,
    c.RealEstateCollateralKey,
    c.RealEstateCollateralStatusCode,
    c.RealEstateCollateralStatus,
    c.InitialLegalOrder,
    c.LawyerPartyKey,
    c.JudgementNumber,
    c.JudgementDate,
    c.RegistryNumber,
    c.LandRegistry,
    c.TranscriptNumber,
    c.TranscriptVolume,
    c.TranscriptSheet,
    c.CreditorPartyKey,
    c.CourtNumber,
    c.AppliedDate,
    c.BusinessDate,
    CASE WHEN c.CollateralStatus IN ('Ενεργή') THEN 'Active'
         WHEN c.CollateralStatus IN ('Ανενεργή') THEN 'Inactive'
         ELSE NULL END AS CollateralStatus,
    CASE WHEN r.RealEstateStatus = 'Ενεργό' THEN 'Active'
         WHEN r.RealEstateStatus = 'Ανενεργό' THEN 'Inactive'
         ELSE NULL END AS RealEstateStatus,
    CASE WHEN c.RealEstateCollateralStatus = 'Ενεργή' THEN 'Active'
         WHEN c.RealEstateCollateralStatus = 'Ανενεργή' THEN 'Inactive'
         ELSE NULL END AS RealEstateCollateralStatusCalculated,
    CASE WHEN c.RealEstateKey IS NOT NULL THEN 'RE'
         WHEN c.NonRealEstateKey IS NOT NULL THEN 'Non-RE'
         ELSE NULL END AS CollateralType,
    CASE WHEN CollateralType = 'RE' THEN c.AppliedDate
        ELSE NULL END REMortgageRegistrationDate,
    t.FieldValueENG as CategoryENG
FROM
    collateral c
LEFT JOIN
    realestatebuilding r
ON
    try_cast(c.RealEstateKey as string) = r.RealEstateKey  and r.sourcesystemmapping=c.sourcesystem
LEFT JOIN
    dktranslationmapping t
ON
    c.Category = t.FieldValue
""")

target_table_dcollateral_pre.createOrReplaceTempView("target_table_dcollateral_pre")

# COMMAND ----------

target_table_dcollateralid_primary_key_fields = [
    collateral.CollateralId,
]

target_table_dcollateralKey_primary_key_fields = [
    collateral.CollateralKey,
]

target_table_dcollateral = (
    target_table_dcollateral_pre.withColumn(
        "DCollateralId",
        sha2(
            concat_ws(";", *target_table_dcollateralid_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DCollateralKey",
        concat_ws(";", *target_table_dcollateralKey_primary_key_fields)
    )
)

result_df = target_table_dcollateral.select(
    target_table_dcollateral.DCollateralId.cast("String"),
    target_table_dcollateral.SourceSystem.cast("String"),
    target_table_dcollateral.PortfolioKey.cast("String"),
    target_table_dcollateral.PortfolioName.cast("String"),
    lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
        "IngestionTimestamp"
    ),
    lit(None).alias("BatchId"),
    target_table_dcollateral.DCollateralKey.cast("String"),
    target_table_dcollateral.CollateralOldKey.cast("String"),
    target_table_dcollateral.CollateralCode.cast("String").alias("CollateralCode"),
    target_table_dcollateral.Comment.cast("String"),
    target_table_dcollateral.CollateralAmount.cast("decimal(19,2)").alias("CollateralAmount"),
    target_table_dcollateral.CollateralCurrency.cast("String").alias("Currency"),
    target_table_dcollateral.CollateralStatusCode.cast("String").alias("CollateralStatusCode"),
    target_table_dcollateral.CollateralStatus.cast("String").alias("CollateralStatus"),
    target_table_dcollateral.StatusDate.cast("date").alias("StatusDate"),
    target_table_dcollateral.MortgageTypeCode.cast("String"),
    target_table_dcollateral.MortgageType.alias("MortgageType"),
    target_table_dcollateral.AgreementKey.cast("String"),
    target_table_dcollateral.CategoryCode.cast("String"),
    target_table_dcollateral.Category.cast("String"),
    target_table_dcollateral.CategoryENG.cast("String").alias("CollateralTypeDescription"),
    target_table_dcollateral.SubCategoryCode.cast("String"),
    target_table_dcollateral.SubCategory.cast("String"),
    target_table_dcollateral.OwnerPartyKey.cast("String"),
    target_table_dcollateral.RealEstateKey.cast("String"),
    target_table_dcollateral.NonRealEstateKey.cast("String"),
    target_table_dcollateral.OwnershipKey,
    target_table_dcollateral.RealTitleDescription.cast("String"),
    target_table_dcollateral.RealEstateStatus.cast("String"),
    target_table_dcollateral.CollateralType.cast("String"),
    target_table_dcollateral.OwnershipPercentage.cast("double"),
    target_table_dcollateral.OwnershipStatusCode.cast("String"),
    target_table_dcollateral.OwnershipStatus.cast("String"),
    target_table_dcollateral.OwnershipStatusDate.cast("date").alias("OwnershipStatusDate"),
    target_table_dcollateral.NotaryNumber.cast("String"),
    target_table_dcollateral.NotaryDate.cast("date").alias("NotaryDate"),
    target_table_dcollateral.KtisisWayKey.cast("String"),
    target_table_dcollateral.RealEstateCollateralKey.cast("String"),
    target_table_dcollateral.RealEstateCollateralStatusCode.cast("String"),
    target_table_dcollateral.RealEstateCollateralStatusCalculated.cast("String").alias("RealEstateCollateralStatus"),
    target_table_dcollateral.InitialLegalOrder.cast("int").alias("InitialLegalOrder"),
    target_table_dcollateral.LawyerPartyKey.cast("String"),
    target_table_dcollateral.JudgementNumber.cast("String"),
    target_table_dcollateral.JudgementDate.cast("date").alias("JudgementDate"),
    target_table_dcollateral.REMortgageRegistrationDate.alias("REMortgageRegistrationDate"),
    target_table_dcollateral.RegistryNumber.cast("String"),
    target_table_dcollateral.LandRegistry.cast("int"),
    target_table_dcollateral.TranscriptNumber.cast("String"),
    target_table_dcollateral.TranscriptVolume.cast("String"),
    target_table_dcollateral.TranscriptSheet.cast("String"),
    target_table_dcollateral.CreditorPartyKey.cast("String"),
    target_table_dcollateral.CourtNumber.cast("int"),
    target_table_dcollateral.BusinessDate.alias("BusinessDate"),
)

# COMMAND ----------

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
