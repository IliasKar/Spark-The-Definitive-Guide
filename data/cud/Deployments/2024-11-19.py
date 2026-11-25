# Databricks notebook source
# MAGIC %md
# MAGIC ## DATA2-117
# MAGIC ### Steps
# MAGIC * Re-run collection_process for all businessdates

# COMMAND ----------

# MAGIC %md
# MAGIC ## D360-2067
# MAGIC
# MAGIC ### Steps :
# MAGIC * Re-run for all businessdates (2024-11-07 and forth) exposure & dexposure 
# MAGIC * Run the following code

# COMMAND ----------


spark.sql('''
ALTER TABLE `p-cdh`.primaryactive.exposure
ADD COLUMN LegalActionPriority string AFTER ExclusionReason ;
''')

# COMMAND ----------


spark.sql('''
ALTER TABLE `p-cdh`.derivativeactive.dexposure
ADD COLUMN LegalActionPriority string AFTER SettlementDaysPastDue;
''')

spark.sql('''
ALTER TABLE `p-cdh`.derivativearchive.dexposure
ADD COLUMN LegalActionPriority string AFTER SettlementDaysPastDue;
''')

# COMMAND ----------

# MAGIC %md
# MAGIC # RUN THIS WITH HIVE-METASTORE

# COMMAND ----------

spark.sql('''
ALTER TABLE hive_metastore.`p-ec1-cepal-curated-area`.exposure
ADD COLUMN LegalActionPriority string AFTER ExclusionReason;
''')
