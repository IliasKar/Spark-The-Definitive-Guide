# Databricks notebook source
import logging
from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
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

schema = (
        "DRestructuringId	string,"
        "SourceSystem	string,"
        "PortfolioKey	int,"
        "PortfolioName	string,"
        "IngestionTimestamp	timestamp,"
        "BatchId	    string,"
        "DRestructuringKey	string,"
        "OldAgreementKey	string,"
        "NewAgreementKey string,"
        "OldExposureKey string,"
        "NewExposureKey string,"
        "InterestExposureKey string,"
        "InterestFreeExposureKey string,"
        "PartyKey string,"
        "CDI string,"
        "RestructuringProductKey string,"
        "RestructuringLinkType string,"
        "CompletionTargetDate date,"
        "RestructuringDate date,"
        "RestructuringTypeCode string,"
        "RestructuringType string,"
        "RestructuringAmount decimal(19,2),"
        "RestructuringDiscountAmount decimal(19,2),"
        "SpreadPercentage decimal(19,2),"
        "Comment string,"
        "CreatedDate   timestamp,"
        "CreatedBy string,"
        "LastUpdatedDate   date,"
        "LastUpdatedBy string,"
        "BusinessDate date"
    )

partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "DRestructuring"
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
# MAGIC ## **DRestructuring_profits**

# COMMAND ----------

# Create temp view for each source table, based on the run date
dexposure = spark.sql(f"""
    SELECT *
        ,{run_date!r} AS RunDate
    FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date(DATE({run_date!r}))
    WHERE sourcesystem = 'profits'
""")

dexposure.createOrReplaceTempView("dexposure")

Restructuring_profits = spark.sql(f""" 
    select *
    FROM  `{env}-cdh`.primary.Restructuring
    WHERE BusinessDate = {run_date!r} 
        AND SourceSystem="profits"
"""
)

Restructuring_profits.createOrReplaceTempView("Restructuring_profits")

RestructuringWriteoff_profits = spark.sql(f""" 
    select distinct 
        RestructuringExposureKey
        ,WriteOffExposureKey
        ,BusinessDate
    FROM  `{env}-cdh`.primary.RestructuringWriteoff
    WHERE BusinessDate = {run_date!r} 
        AND SourceSystem="profits"
"""
) 

RestructuringWriteoff_profits.createOrReplaceTempView("RestructuringWriteoff_profits")

# COMMAND ----------

target_table_DRestructuring_profits = spark.sql(f""" SELECT
    restr.RestructuringId,
    restr.SourceSystem,
    restr.PortfolioKey,
    restr.PortfolioName,
    restr.IngestionTimestamp,
    restr.BatchId,
    restr.RestructuringKey,
    restr.OldAgreementKey,
    restr.NewAgreementKey,
    restr.OldExposureKey,
    restr.NewExposureKey,
    restr.InterestExposureKey,
    restr.InterestFreeExposureKey,
    restr.PartyKey,
    restr.CDI,
    restr.RestructuringProductKey,
    restr.RestructuringLinkType,
    restr.CompletionTargetDate,
    restr.RestructuringDate,
    restr.RestructuringTypeCode,
    restr.RestructuringType,
    restr.RestructuringAmount,
    restr.RestructuringDiscountAmount,
    restr.SpreadPercentage,
    restr.Comment,
    restr.CreatedDate,
    restr.CreatedBy,
    restr.LastUpdatedDate,
    restr.LastUpdatedBy,
    restr.BusinessDate
FROM Restructuring_profits restr
LEFT JOIN RestructuringWriteoff_profits writeoff
ON restr.OldExposureKey = writeoff.RestructuringExposureKey
AND restr.NewExposureKey = writeoff.WriteOffExposureKey
AND restr.BusinessDate = writeoff.BusinessDate
where writeoff.RestructuringExposureKey is null
""")

# COMMAND ----------

target_table_drestructuringid_primary_key_fields = [
    target_table_DRestructuring_profits.RestructuringId
]

target_table_drestructuringKey_primary_key_fields = [
    target_table_DRestructuring_profits.RestructuringKey
]

target_table_drestructuring_profits = (
    target_table_DRestructuring_profits.withColumn(
        "DRestructuringId",
        sha2(
            concat_ws(";", *target_table_drestructuringid_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DRestructuringKey",
        concat_ws(";", *target_table_drestructuringKey_primary_key_fields)
    )
)

drestructuring_profits = target_table_drestructuring_profits.select(
    target_table_drestructuring_profits.DRestructuringId.cast("String"),
    target_table_drestructuring_profits.SourceSystem.cast("String"),
    target_table_drestructuring_profits.PortfolioKey,
    target_table_drestructuring_profits.PortfolioName.cast("String"),
    lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
        "IngestionTimestamp"
    ),
    lit(None).alias("BatchId"),
    target_table_drestructuring_profits.DRestructuringKey.cast("String"),
    target_table_drestructuring_profits.OldAgreementKey.cast("String"),
    target_table_drestructuring_profits.NewAgreementKey.cast("String"),
    target_table_drestructuring_profits.OldExposureKey.cast("String"),
    target_table_drestructuring_profits.NewExposureKey.cast("String"),
    target_table_drestructuring_profits.InterestExposureKey.cast("String"),
    target_table_drestructuring_profits.InterestFreeExposureKey.cast("String"),
    target_table_drestructuring_profits.PartyKey.cast("String"),
    target_table_drestructuring_profits.CDI.cast("String"),
    target_table_drestructuring_profits.RestructuringProductKey.cast("String"),
    target_table_drestructuring_profits.RestructuringLinkType.cast("String"),
    target_table_drestructuring_profits.CompletionTargetDate,
    target_table_drestructuring_profits.RestructuringDate.cast("Date"),
    target_table_drestructuring_profits.RestructuringTypeCode.cast("String"),
    target_table_drestructuring_profits.RestructuringType.cast("String"),
    target_table_drestructuring_profits.RestructuringAmount.cast("decimal(19,2)"),
    target_table_drestructuring_profits.RestructuringDiscountAmount.cast("decimal(19,2)"),
    target_table_drestructuring_profits.SpreadPercentage.cast("decimal(19,2)"),
    target_table_drestructuring_profits.Comment.cast("String"),
    target_table_drestructuring_profits.CreatedDate,
    target_table_drestructuring_profits.CreatedBy.cast("String"),
    target_table_drestructuring_profits.LastUpdatedDate,
    target_table_drestructuring_profits.LastUpdatedBy.cast("String"),
    target_table_drestructuring_profits.BusinessDate,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DRestructuring_arotron

# COMMAND ----------

# Create temp view for each source table, based on the run date

Restructuring_arotron = spark.sql(f""" select 
    restr.RestructuringId,
    restr.SourceSystem,
    restr.PortfolioKey,
    restr.PortfolioName,
    restr.IngestionTimestamp,
    restr.BatchId,
    restr.RestructuringKey,
    restr.OldAgreementKey,
    restr.NewAgreementKey,
    restr.OldExposureKey,
    restr.NewExposureKey,
    restr.InterestExposureKey,
    restr.InterestFreeExposureKey,
    restr.PartyKey,
    restr.CDI,
    restr.RestructuringProductKey,
    restr.RestructuringLinkType,
    restr.CompletionTargetDate,
    restr.RestructuringDate,
    restr.RestructuringTypeCode,
    restr.RestructuringType,
    restr.RestructuringAmount,
    restr.RestructuringDiscountAmount,
    restr.SpreadPercentage,
    restr.Comment,
    restr.CreatedDate,
    restr.CreatedBy,
    restr.LastUpdatedDate,
    restr.LastUpdatedBy,
    restr.BusinessDate
FROM  `{env}-cdh`.primary.Restructuring restr
WHERE BusinessDate = {run_date!r} AND SourceSystem="arotron"
"""
)

Restructuring_arotron.createOrReplaceTempView("Restructuring_arotron")

# COMMAND ----------

target_table_drestructuringid_primary_key_fields = [
    Restructuring_arotron.RestructuringId
]

target_table_drestructuringKey_primary_key_fields = [
    Restructuring_arotron.RestructuringKey
]

Restructuring_arotron = (
    Restructuring_arotron.withColumn(
        "DRestructuringId",
        sha2(
            concat_ws(";", *target_table_drestructuringid_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DRestructuringKey",
        concat_ws(";", *target_table_drestructuringKey_primary_key_fields)
    )
)

drestructuring_arotron = Restructuring_arotron.select(
    Restructuring_arotron.DRestructuringId.cast("String"),
    Restructuring_arotron.SourceSystem.cast("String"),
    Restructuring_arotron.PortfolioKey,
    Restructuring_arotron.PortfolioName.cast("String"),
    lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
        "IngestionTimestamp"
    ),
    lit(None).alias("BatchId"),
    Restructuring_arotron.DRestructuringKey.cast("String"),
    Restructuring_arotron.OldAgreementKey.cast("String"),
    Restructuring_arotron.NewAgreementKey.cast("String"),
    Restructuring_arotron.OldExposureKey.cast("String"),
    Restructuring_arotron.NewExposureKey.cast("String"),
    Restructuring_arotron.InterestExposureKey.cast("String"),
    Restructuring_arotron.InterestFreeExposureKey.cast("String"),
    Restructuring_arotron.PartyKey.cast("String"),
    Restructuring_arotron.CDI.cast("String"),
    Restructuring_arotron.RestructuringProductKey.cast("String"),
    Restructuring_arotron.RestructuringLinkType.cast("String"),
    Restructuring_arotron.CompletionTargetDate,
    Restructuring_arotron.RestructuringDate.cast("Date"),
    Restructuring_arotron.RestructuringTypeCode.cast("String"),
    Restructuring_arotron.RestructuringType.cast("String"),
    Restructuring_arotron.RestructuringAmount.cast("decimal(19,2)"),
    Restructuring_arotron.RestructuringDiscountAmount.cast("decimal(19,2)"),
    Restructuring_arotron.SpreadPercentage.cast("decimal(19,2)"),
    Restructuring_arotron.Comment.cast("String"),
    Restructuring_arotron.CreatedDate,
    Restructuring_arotron.CreatedBy.cast("String"),
    Restructuring_arotron.LastUpdatedDate,
    Restructuring_arotron.LastUpdatedBy.cast("String"),
    Restructuring_arotron.BusinessDate,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final DataFrame DRestructuring

# COMMAND ----------

drestructuring_unfiltered = drestructuring_profits.union(drestructuring_arotron)
drestructuring_unfiltered.createOrReplaceTempView("drestructuring_unfiltered")

# Add IsRestructureActive flag: 1 if exposure is open, 0 otherwise
drestructuring = spark.sql(f"""
    SELECT dr.*,
        CASE WHEN ex.ExposureKey IS NOT NULL AND (ex.ClosedDate IS NULL OR ex.ClosedDate > {run_date!r}) THEN 1 ELSE 0 END AS IsRestructureActive
    FROM drestructuring_unfiltered dr
    LEFT JOIN dexposure ex
    ON dr.NewExposureKey = ex.ExposureKey
""")

# COMMAND ----------

if unity == "True":
    drestructuring.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else: 
    drestructuring.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake 
