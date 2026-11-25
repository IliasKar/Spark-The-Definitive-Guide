# Databricks notebook source
# MAGIC %md
# MAGIC ### DWH-3500  - Portfolio updates
# MAGIC ####Steps
# MAGIC * Run the following code
# MAGIC * Download portfolio.json from dev enviroment in the path s3://d-ec1-cepal-common-zone/portfolio/ and add it in the corresponding path of production enviroment.

# COMMAND ----------

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("Updates").getOrCreate()
env="p"

# COMMAND ----------

spark.sql(f'''
            update `{env}-ec1-cepal-curated-area`.portfolio   
            set profitCenter = "81000"
            where PortfolioName = 'EUCLID'
          ''')
