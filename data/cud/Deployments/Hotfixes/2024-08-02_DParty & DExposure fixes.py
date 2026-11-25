# Databricks notebook source
# MAGIC %md
# MAGIC #### DParty : Rename column IsWholeshaleFlag to IsWholesaleFlag

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dparty
# MAGIC RENAME COLUMN IsWholeshaleFlag TO IsWholesaleFlag

# COMMAND ----------

# MAGIC %md
# MAGIC #### DExposure : Change datatype of column CodeofConductStage;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dexposure
# MAGIC DROP COLUMN CodeofConductStage;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dexposure
# MAGIC ADD COLUMN CodeofConductStage string AFTER ExposureRestructureType;
