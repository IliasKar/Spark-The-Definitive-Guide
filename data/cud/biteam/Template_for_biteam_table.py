# Databricks notebook source
import logging
# DBTITLE 1,Import Libaries and Functions
# import extra libraries and function if needed

import delta_table_creator, extract, transform
import boto3
import os
from pyspark.sql.functions import (
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

# dbutils.widgets.text("run_date", '2024-08-27')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Get the values of widgets and initialize the environment variable
# MAGIC Ensure that widgets are assigned the wanted values before proceeding with the next step.

# COMMAND ----------

# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
env = os.environ["S3PREFIX"]

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
unity = dbutils.widgets.get("unity") 

table_name = 'ExampleTable' # change the table name

# COMMAND ----------

# DBTITLE 1,SCHEMA INITIALIZATION
# change the schema of thw table with the relevant fields
schema = (
            "ExampleTableId string" # the unique identifier of the table - name as ExampleTableId
            ",SourceSystem string" # no changes needed
            ",PortfolioKey string" # no changes needed
            ",PortfolioName string" # no changes needed
            ",IngestionTimestamp timestamp" # no changes needed
            ",BatchId string" # no changes needed
            ",ExampleTableKey string" # the key of the table - name as ExampleTableKey
            ",ExampleFieldName string" # change to new field names & datatypes needed 
            ",ExampleFieldName2 int"
            ",BusinessDate date" # no changes needed
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

# change the following cells to the relevant table/columns of landed area 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### example_landed_table 

# COMMAND ----------

example_landed_table = spark.sql(
f'''
SELECT 
    ExampleFieldName
  , ExampleFieldName2
  , BusinessDate -- no changes needed
FROM  `{env}-cdh`.landed.example_landed_table
WHERE BusinessDate = {run_date!r} -- no changes needed
'''
)

example_landed_table.createOrReplaceTempView("example_landed_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### portfolio

# COMMAND ----------

# change the columns of portfolio tables if needed in order to have PortfolioKey (i.e. the source system code for each portfolio) & PortfolioName in the final dataframe/cud table.

portfolio = spark.sql(
f'''
SELECT 
  PortfolioName,
  PortfolioId,
  unit_code_profits
FROM  `{env}-cdh`.primary.portfolio
WHERE BusinessDate = {run_date!r}
'''
)

portfolio.createOrReplaceTempView("portfolio")

# COMMAND ----------

# DBTITLE 1,JOINS
# change table name and join condition if needed

# ================== Joins ==================
example_landed_table_df = (
    example_landed_table.join(
        portfolio,
        example_landed_table.PortfolioId == portfolio.PortfolioId,
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,INITIALIZATION OF DATAFRAME
# change the following keys with the appropriate fields and names

# ================== example_landed_table_df ==================
example_landed_table_df_primary_key_fields = [
    example_landed_table.ExampleFieldName,
    example_landed_table.ExampleFieldName2,
    lit("biteam"), # no changes needed
]

example_landed_table_df_key_fields = [
    example_landed_table.ExampleFieldName,
    example_landed_table.ExampleFieldName2,
]

example_landed_table_df = example_landed_table_df.withColumn(
        "ExampleTableID",
        sha2(
            concat_ws(
                ";", *example_landed_table_df_primary_key_fields
            ),
            256,
        ),
    ).withColumn(
        "ExampleTableKey",
            concat_ws(
                ";", *example_landed_table_df_key_fields
            ),
        )

# COMMAND ----------

# DBTITLE 1,Final Dataframe
# change the fields of the final dataframe

final_df = example_landed_table_df.select(
            example_landed_table_df.ExampleTableID.cast("string").alias("ExampleTableID"),
            lit("biteam").cast("string").alias("SourceSystem"), # no changes needed
            portfolio.unit_code_profits.cast("string").alias("PortfolioKey"), # PortfolioKey is the code of each source system for portfolio
            portfolio.PortfolioName.cast("string").alias("PortfolioName"),
            lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias("IngestionTimestamp"), # no changes needed
            lit(None).cast("String").alias("BatchId"), # no changes needed
            example_landed_table_df.ExampleTableKey.cast("string").alias("ExampleTableKey"),
            example_landed_table.ExampleFieldName.cast("string").alias("ExampleFieldName"),
            example_landed_table.ExampleFieldName2.cast("int").alias("ExampleFieldName2"),
            example_landed_table.BusinessDate.cast("date").alias("BusinessDate"),
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

# DBTITLE 1,WRITE TO DELTA TABLE
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
