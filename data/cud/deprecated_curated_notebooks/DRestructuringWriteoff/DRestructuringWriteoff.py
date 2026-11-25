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
    "DRestructuringWriteoffId  string,"
    "SourceSystem              string,"
    "PortfolioKey              string,"
    "PortfolioName             string,"
    "IngestionTimestamp	    timestamp,"
    "BatchId	               string,"
    "DRestructuringWriteoffKey string,"
    "RestructuringExposureKey  string,"
    "WriteOffExposureKey       string,"
    "BusinessDate              date"
)

partition_columns = ["BusinessDate"]
run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")
table_name = "DRestructuringWriteoff"
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

RestructuringWriteoff = spark.sql(f""" select 
    restr.RestructuringWriteoffId
    ,restr.SourceSystem 
    ,restr.PortfolioKey
    ,restr.PortfolioName
    ,restr.RestructuringWriteoffKey
    ,restr.RestructuringExposureKey
    ,restr.WriteOffExposureKey
    ,restr.BusinessDate
FROM  `{env}-cdh`.primary.RestructuringWriteoff restr
WHERE BusinessDate = {run_date!r}
"""
) 

RestructuringWriteoff.createOrReplaceTempView("RestructuringWriteoff")

EntityRelationships = spark.sql(f""" select 
    ent.EntityRelationshipId
    ,ent.SourceSystem 
    ,ent.PortfolioKey
    ,ent.PortfolioName
    ,ent.EntityRelationshipKey
    ,ent.EntityKey
    ,ent.RelatedEntityKey
    ,ent.BusinessDate
FROM  `{env}-cdh`.primary.EntityRelationships ent
WHERE BusinessDate = {run_date!r}
AND EntityRelationshipType = "Master - Slave Rltnp"
AND EntityRelationshipSubType = "WDN"
"""
)

EntityRelationships.createOrReplaceTempView("EntityRelationships")

# COMMAND ----------

target_table_drestructuringwriteoff = spark.sql(f""" SELECT 
    restr.RestructuringWriteoffId as DRestructuringWriteoffId_primaryid
    ,restr.SourceSystem 
    ,restr.PortfolioKey
    ,restr.PortfolioName
    ,restr.RestructuringWriteoffKey as DRestructuringWriteoffKey_primarykey
    ,restr.RestructuringExposureKey
    ,restr.WriteOffExposureKey
    ,restr.BusinessDate
FROM RestructuringWriteoff restr
UNION
SELECT 
    ent.EntityRelationshipId as DRestructuringWriteoffId_primaryid
    ,ent.SourceSystem 
    ,ent.PortfolioKey
    ,ent.PortfolioName
    ,ent.EntityRelationshipKey as DRestructuringWriteoffKey_primarykey
    ,ent.EntityKey
    ,ent.RelatedEntityKey
    ,ent.BusinessDate
FROM EntityRelationships ent
""")

# COMMAND ----------

target_table_drestructuringwriteoffid_primary_key_fields = [
    "DRestructuringWriteoffId_primaryid",
]

target_table_drestructuringwriteoffKey_primary_key_fields = [
    "DRestructuringWriteoffKey_primarykey",
]

target_table_drestructuringwriteoff = (
    target_table_drestructuringwriteoff.withColumn(
        "DRestructuringWriteoffId",
        sha2(
            concat_ws(";", *target_table_drestructuringwriteoffid_primary_key_fields),
            256,
        ),
    ).withColumn(
        "DRestructuringWriteoffKey",
        concat_ws(";", *target_table_drestructuringwriteoffKey_primary_key_fields)
    )
)

result_df = target_table_drestructuringwriteoff.select(
    target_table_drestructuringwriteoff.DRestructuringWriteoffId.cast("String"),
    target_table_drestructuringwriteoff.SourceSystem.cast("String"),
    target_table_drestructuringwriteoff.PortfolioKey.cast("String"),
    target_table_drestructuringwriteoff.PortfolioName.cast("String"),
    lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
        "IngestionTimestamp"
    ),
    lit(None).alias("BatchId"),
    target_table_drestructuringwriteoff.DRestructuringWriteoffKey.cast("String"),
    target_table_drestructuringwriteoff.RestructuringExposureKey.cast("String"),
    target_table_drestructuringwriteoff.WriteOffExposureKey.cast("String"),
    target_table_drestructuringwriteoff.BusinessDate,
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
