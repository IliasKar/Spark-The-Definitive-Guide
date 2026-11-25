# Databricks notebook source
# MAGIC %md
# MAGIC ## CAMPT-118
# MAGIC ### Steps
# MAGIC * Run the following code in prod in a notebook.
# MAGIC * Re-run Casecode for all businessdates in production.
# MAGIC

# COMMAND ----------

import os 
env=os.environ["AWSPREFIX"]  ## else use env="p"

spark.sql("""
ALTER TABLE hive_metastore.`p-ec1-cepal-curated-area`.casecode
ADD COLUMN AccountNumber string  AFTER TransactionPerimeterCode;
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMPT-119
# MAGIC  
# MAGIC ### Steps
# MAGIC * Re-run extreportunified for all businessdates

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA2-76
# MAGIC
# MAGIC ### STEPS
# MAGIC * Run the following code

# COMMAND ----------

spark.sql("""
ALTER TABLE  hive_metastore.`p-ec1-cepal-curated-area`.collection
ADD COLUMN TaxNumber string  AFTER CancellationDate;
""")

# COMMAND ----------

spark.sql("""
ALTER TABLE hive_metastore.`p-ec1-cepal-curated-area`.collection
ADD COLUMN CDI string AFTER TaxNumber ;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # RUN ALL THIS WITH UNITY CLUSTER

# COMMAND ----------

spark.sql('''
ALTER TABLE `p-cdh`.primaryactive.casecode
ADD COLUMN AccountNumber string  AFTER TransactionPerimeterCode;
''')

# COMMAND ----------

spark.sql("""
ALTER TABLE  `p-cdh`.primaryactive.collection
ADD COLUMN TaxNumber string  AFTER CancellationDate;
""")

# COMMAND ----------

spark.sql("""
ALTER TABLE `p-cdh`.primaryactive.collection
ADD COLUMN CDI string AFTER TaxNumber;
""")
