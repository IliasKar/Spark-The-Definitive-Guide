# Databricks notebook source
# MAGIC %md
# MAGIC ##DWH-3500 - Portfolio updates 

# COMMAND ----------

# MAGIC %sql
# MAGIC update hive_metastore.`p-ec1-cepal-curated-area`.portfolio
# MAGIC set AssignmentDate = '2024-09-23'
# MAGIC where PortfolioName = 'MONZA PORTFOLIO - RETAIL';
# MAGIC  
# MAGIC update hive_metastore.`p-ec1-cepal-curated-area`.portfolio
# MAGIC set AssignmentDate = '2024-09-23'
# MAGIC where PortfolioName = 'MONZA PORTFOLIO - WHOLESALE';
# MAGIC  
# MAGIC update hive_metastore.`p-ec1-cepal-curated-area`.portfolio
# MAGIC set AssignmentDate = '2024-09-23'
# MAGIC where PortfolioName = 'MONZA PORTFOLIO - D';
# MAGIC  
# MAGIC update hive_metastore.`p-ec1-cepal-curated-area`.portfolio
# MAGIC set AssignmentDate = '2024-09-30'
# MAGIC where PortfolioName = 'EUCLID';
# MAGIC
# MAGIC update hive_metastore.`p-ec1-cepal-curated-area`.portfolio
# MAGIC set TiresiasCode='SAN'
# MAGIC    ,TiresiasName='SANSTORM LTD'
# MAGIC    ,profitCenter='78000'
# MAGIC where portfolioname='SANSTORM LTD';

# COMMAND ----------

# MAGIC %md
# MAGIC ## D360-2076  - CreditMemo enhancements
# MAGIC
# MAGIC # DO NOT RUN THIS , IT'S JUST FOR HISTORICITY PURPOSE !!

# COMMAND ----------

# ADD COLUMN
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, concat, to_date
import os 

# Import the module
from aws_utils import *

# Get Environment Prefix

env = get_env_prefix()

table_name="CreditMemoEnhancement"
table_path = f"`{env}-cdh-delivery`.`business-plan`.{table_name}"


# load delta table into DataFrame
df = spark.read.format("delta").table(f"`{env}-cdh-delivery`.`business-plan`.`{table_name}`")

# specify the data type for the new column
new_column_data_type = "boolean"

# add a new column with null values and specified data type
df = df.withColumn("IsPortfolioSecuritizedFlag", lit(None).cast(new_column_data_type))


# Define the desired column order
desired_order = [
    "PortfolioName",
    "CDI",
    "GBV",
    "BusinessDate",
    "CepalBPYearMonth",
    "InvestorBPYearMonth",
    "CepalBPGrossAmount",
    "InvestorBPGrossAmount",
    "CepalBPDuration",
    "InvestorBPDuration",
    "CepalBPAverageLife",
    "CepalBPNPV",
    "CepalBPNVP/GBV",
    "InvestorAverageLife",
    "InvestorBPNPV",
    "InvestorBPNVP/GBV",
    "CollectionsPreOnboardingSecuritized",
    "CollectionsPreOnboardingThrirdParties",
    "IsPortfolioSecuritizedFlag"
]


# Reorder the DataFrame columns
df = df.select(*desired_order)

# write changes back to delta table
(
    df.write
        .format("delta")
        .partitionBy('BusinessDate')
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table_path)
)
