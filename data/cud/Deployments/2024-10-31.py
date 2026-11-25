# Databricks notebook source
# MAGIC %md
# MAGIC ## D360-2001
# MAGIC ### DAnalyticsMetrics
# MAGIC * Correction of  PortfolioName
# MAGIC  
# MAGIC ### Steps:
# MAGIC * Run the following code in different cells of a Databricks Jupyter Notebook in order to  update the fields.

# COMMAND ----------

import os
env = os.environ["S3PREFIX"]  ## in case that doesn't work just define env="p" manually

portfolio=spark.sql(f"""
    select distinct * from`{env}-cdh`.curated.portfolio.portfolio
    """
)
 
portfolio.createOrReplaceTempView("portfolio")

spark.sql(f"""MERGE INTO `{env}-cdh`.derivativeactive.danalyticsmetrics AS target
USING (
  SELECT DISTINCT
    p.PortfolioName,
    SUBSTRING(d.AgreementKey, 1, 3) AS Prefix,
    d.AgreementKey
  FROM `{env}-cdh`.derivative.danalyticsmetrics AS d
  INNER JOIN portfolio AS p
  ON SUBSTRING(d.AgreementKey, 1, 3) = p.Prefix
) AS source
ON target.AgreementKey = source.AgreementKey
WHEN MATCHED THEN
  UPDATE SET target.PortfolioName = source.PortfolioName;
""")


# COMMAND ----------

spark.sql(f"""MERGE INTO `{env}-cdh`.derivativearchive.danalyticsmetrics AS target
USING (
  SELECT DISTINCT
    p.PortfolioName,
    SUBSTRING(d.AgreementKey, 1, 3) AS Prefix,
    d.AgreementKey
  FROM `{env}-cdh`.derivative.danalyticsmetrics AS d
  INNER JOIN portfolio AS p
  ON SUBSTRING(d.AgreementKey, 1, 3) = p.Prefix
) AS source
ON target.AgreementKey = source.AgreementKey
WHEN MATCHED THEN
  UPDATE SET target.PortfolioName = source.PortfolioName;
  """)


# COMMAND ----------

spark.sql(f"""MERGE INTO `{env}-cdh`.derivativeactive.danalyticsmetrics_historical AS target
USING (
  SELECT DISTINCT
    p.PortfolioName,
    SUBSTRING(d.AgreementKey, 1, 3) AS Prefix,
    d.AgreementKey
  FROM `{env}-cdh`.derivativeactive.danalyticsmetrics_historicalAS d
  INNER JOIN portfolio AS p
  ON SUBSTRING(d.AgreementKey, 1, 3) = p.Prefix
) AS source
ON target.AgreementKey = source.AgreementKey
WHEN MATCHED THEN
  UPDATE SET target.PortfolioName = source.PortfolioName;
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D360-2122
# MAGIC #### Steps 
# MAGIC * Run the following code

# COMMAND ----------

spark.sql(f"""ALTER TABLE `p-cdh`.`derivativearchive`.dunderlaw
          DROP COLUMN CompletionReasonCode
          """)
spark.sql(f"""ALTER TABLE `p-cdh`.`derivativearchive`.dunderlaw
          ADD COLUMN CompletionReasonCode INT AFTER CourtOutcome
          """)


# COMMAND ----------

# MAGIC %md
# MAGIC ### BAUC-15 
# MAGIC Re run extunionfile for every businessdate in prod.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D360-2042
# MAGIC  Include the following s3 path: 's3://d-ec1-cepal-landed-zone/dwhcore/dwh_stg/ssp/ssp_RequestCategoryType/' in crawler: 'd-ec1-cepal-daily-landed-ssp'. Perform the above in QA/Prod (already performed in dev).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA2-55
# MAGIC ## Steps 
# MAGIC * Re-run paymentschedule_profits for every businessdate
# MAGIC * Run the following code
# MAGIC

# COMMAND ----------


spark.sql(f"""UPDATE `{env}-ec1-cepal-curated-area`.paymentschedule
SET InstallmentType = CASE
    WHEN InstallmentTypeCode = '2081' THEN 'Instalment-Cash'
    WHEN InstallmentTypeCode = 'N/A' THEN 'N/A'
    WHEN InstallmentTypeCode = '3021' THEN 'Instalment-Ρύθμιση'
    ELSE InstallmentTypeCode
END
WHERE SourceSystem = 'qca'
""")

# COMMAND ----------


spark.sql(f"""UPDATE `{env}-ec1-cepal-curated-area`.paymentschedule
SET InstallmentStatus = CASE
    WHEN InstallmentStatusCode = '0' THEN "Active"
    WHEN InstallmentStatusCode = '1' THEN "Overdue"
    WHEN InstallmentStatusCode = '2' THEN "Matched"
    ELSE InstallmentStatusCode  -- Keeps the current value for any other status
END
WHERE SourceSystem = 'qca' ;
""")

# COMMAND ----------


spark.sql(f"""UPDATE `{env}-ec1-cepal-curated-area`.paymentschedule
SET InstallmentStatus = CASE
    WHEN InstallmentStatusCode = 'Active' THEN "Active"
    WHEN InstallmentStatusCode = 'Overdue' THEN "Overdue"
    WHEN InstallmentStatusCode = 'Matched' THEN "Matched"
    ELSE InstallmentStatusCode  -- Keeps the current value for any other status
END
WHERE SourceSystem = 'process';
""")
