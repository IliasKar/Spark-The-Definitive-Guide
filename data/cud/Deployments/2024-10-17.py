# Databricks notebook source
# MAGIC %md
# MAGIC ###DATA1-1920 - DpaymentScheduleKey modification
# MAGIC * Re-run dpaymentschedule for all business dates.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA1-796 - derivative tables daily run configuration
# MAGIC * Tune the corresponding parameters inside the Systems Manager in AWS for DerivativeUnityRunTargeted & DerivativeUnityRunBITEAM.
# MAGIC * Copy the job id from Databricks and use it as a value for the previous mentioned parameter

# COMMAND ----------

# MAGIC %md
# MAGIC ### DWH-3500 - Portfolio updates
# MAGIC * Run the following script
# MAGIC * Add the new portfolio.json in the corresponding path of portfolio in prod.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE `d-ec1-cepal-curated-area`.portfolio
# MAGIC SET AssignmentDate ='2024-10-17'
# MAGIC WHERE PortfolioId =63
