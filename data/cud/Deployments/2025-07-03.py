# Databricks notebook source
# MAGIC %md
# MAGIC #### *DD-129* :  Implementation of Uploading DialerExternal
# MAGIC ##### Repos affected : gr.cepal.dna.de.lng-lnd-cup
# MAGIC
# MAGIC #### Steps
# MAGIC * ##### Merge the current pull request (dev/qa/prod)
# MAGIC * ##### Transfer `reference.dialer_activity_duplicates` table to Production
# MAGIC * ##### Insert in externalfilesconfiguration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table `p-cdh`.reference.dialer_activity_duplicates as
# MAGIC select * from `d-cdh`.reference.dialer_activity_duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO `p-cdh`.workflow.externalfilesconfiguration (
# MAGIC     RelativePath,
# MAGIC     UnityCatalog,
# MAGIC     UnitySchema,
# MAGIC     TableName,
# MAGIC     Flow,
# MAGIC     ValidatorScript,
# MAGIC     NotificationEmails,
# MAGIC     string_columns
# MAGIC )
# MAGIC VALUES ('DialerExternalFile', 'p-reportinputfiles', 'internal', null, 'Incremental', null, null, null);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `p-cdh`.workflow.externalfilesconfiguration (
# MAGIC     RelativePath,
# MAGIC     UnityCatalog,
# MAGIC     UnitySchema,
# MAGIC     TableName,
# MAGIC     Flow,
# MAGIC     ValidatorScript,
# MAGIC     NotificationEmails,
# MAGIC     string_columns
# MAGIC )
# MAGIC VALUES ('DialerExternalFile', 'p-reportinputfiles', 'internal', null, 'Snapshot', null, null, null);

# COMMAND ----------

# MAGIC %md
# MAGIC #### DBTC-1500
# MAGIC
# MAGIC List of all Business Dates primaryactive.collection re-run :
# MAGIC
# MAGIC 2024-01-30, 2024-01-31, 2024-02-01, 2024-02-02, 2024-02-05, 2024-02-06, 2024-02-07, 2024-02-08, 2024-02-09, 2024-02-12, 2024-02-13, 2024-02-14, 2024-02-15, 2024-02-16, 2024-02-19, 2024-02-20, 2024-02-21, 2024-02-22, 2024-02-23, 2024-02-26, 2024-02-27, 2024-02-28, 2024-02-29, 2024-03-01, 2024-03-04, 2024-03-05, 2024-03-06, 2024-03-07, 2024-03-08, 2024-03-11, 2024-03-12, 2024-03-13, 2024-03-14, 2024-03-15, 2024-03-19, 2024-03-20, 2024-03-21, 2024-03-22, 2024-03-26, 2024-03-27, 2024-03-28, 2024-04-02, 2024-04-03, 2024-04-04, 2024-04-08, 2024-04-09, 2024-04-10, 2024-04-11, 2024-04-12, 2024-04-15, 2024-04-16, 2024-04-17, 2024-04-18, 2024-04-19, 2024-04-22, 2024-04-23, 2024-04-24, 2024-04-25, 2024-04-26, 2024-04-29, 2024-04-30, 2024-05-02, 2024-05-08, 2024-05-09, 2024-05-10, 2024-05-13, 2024-05-14, 2024-05-15, 2024-05-16, 2024-05-17, 2024-05-20, 2024-05-21, 2024-05-22, 2024-05-23, 2024-05-24, 2024-05-27, 2024-05-28, 2024-05-29, 2024-05-30, 2024-05-31, 2024-06-03, 2024-06-04, 2024-06-05, 2024-06-06, 2024-06-07, 2024-06-10, 2024-06-11, 2024-06-12, 2024-06-13, 2024-06-14, 2024-06-17, 2024-06-18, 2024-06-19, 2024-06-20, 2024-06-21, 2024-06-25, 2024-06-26, 2024-06-27, 2024-06-28, 2024-07-01, 2024-07-02, 2024-07-03, 2024-07-04, 2024-07-05, 2024-07-08, 2024-07-09, 2024-07-10, 2024-07-11, 2024-07-12, 2024-07-15, 2024-07-16, 2024-07-17, 2024-07-18, 2024-07-19, 2024-07-22, 2024-07-23, 2024-07-24, 2024-07-25, 2024-07-26, 2024-07-29, 2024-07-30, 2024-07-31, 2024-08-01, 2024-08-02, 2024-08-05, 2024-08-06, 2024-08-07, 2024-08-08, 2024-08-09, 2024-08-12, 2024-08-13, 2024-08-14, 2024-08-16, 2024-08-19, 2024-08-20, 2024-08-21, 2024-08-22, 2024-08-23, 2024-08-26, 2024-08-27, 2024-08-28, 2024-08-29, 2024-08-30, 2024-09-02, 2024-09-03, 2024-09-04, 2024-09-05, 2024-09-06, 2024-09-09, 2024-09-10, 2024-09-11, 2024-09-12, 2024-09-13, 2024-09-16, 2024-09-17, 2024-09-18, 2024-09-19, 2024-09-20, 2024-09-23, 2024-09-24, 2024-09-25, 2024-09-26, 2024-09-27, 2024-09-30, 2024-10-01, 2024-10-02, 2024-10-03, 2024-10-04, 2024-10-07, 2024-10-08, 2024-10-09, 2024-10-10, 2024-10-11, 2024-10-14, 2024-10-15, 2024-10-16, 2024-10-17, 2024-10-18, 2024-10-21, 2024-10-22, 2024-10-23, 2024-10-24, 2024-10-25, 2024-10-29, 2024-10-30, 2024-10-31, 2024-11-01, 2024-11-04, 2024-11-05, 2024-11-06, 2024-11-07, 2024-11-08, 2024-11-11, 2024-11-12, 2024-11-13, 2024-11-14, 2024-11-15, 2024-11-18, 2024-11-19, 2024-11-20, 2024-11-21, 2024-11-22, 2024-11-25, 2024-11-26, 2024-11-27, 2024-11-28, 2024-11-29, 2024-12-02, 2024-12-03, 2024-12-04, 2024-12-05, 2024-12-06, 2024-12-09, 2024-12-10, 2024-12-11, 2024-12-12, 2024-12-13, 2024-12-16, 2024-12-17, 2024-12-18, 2024-12-19, 2024-12-20, 2024-12-23, 2024-12-24, 2024-12-27, 2024-12-30, 2024-12-31, 2025-01-02, 2025-01-03, 2025-01-07, 2025-01-08, 2025-01-09, 2025-01-10, 2025-01-13, 2025-01-14, 2025-01-15, 2025-01-16, 2025-01-17, 2025-01-20, 2025-01-21, 2025-01-22, 2025-01-23, 2025-01-24, 2025-01-27, 2025-01-28, 2025-01-29, 2025-01-30, 2025-01-31, 2025-02-03, 2025-02-04, 2025-02-05, 2025-02-06, 2025-02-07, 2025-02-10, 2025-02-11, 2025-02-12, 2025-02-13, 2025-02-14, 2025-02-17, 2025-02-18, 2025-02-19, 2025-02-20, 2025-02-21, 2025-02-24, 2025-02-25, 2025-02-26, 2025-02-27, 2025-02-28, 2025-03-04, 2025-03-05, 2025-03-06, 2025-03-07, 2025-03-10, 2025-03-11, 2025-03-12, 2025-03-13, 2025-03-14, 2025-03-17, 2025-03-18, 2025-03-19, 2025-03-20, 2025-03-21, 2025-03-24, 2025-03-26, 2025-03-27, 2025-03-28, 2025-03-31, 2025-04-01, 2025-04-02, 2025-04-03, 2025-04-04, 2025-04-07, 2025-04-08, 2025-04-09, 2025-04-10, 2025-04-11, 2025-04-14, 2025-04-15, 2025-04-16, 2025-04-17, 2025-04-22, 2025-04-23, 2025-04-24, 2025-04-25, 2025-04-28, 2025-04-29, 2025-04-30, 2025-05-02, 2025-05-05, 2025-05-06, 2025-05-07, 2025-05-08, 2025-05-09, 2025-05-12, 2025-05-13, 2025-05-14, 2025-05-15, 2025-05-16, 2025-05-19, 2025-05-20, 2025-05-21, 2025-05-22, 2025-05-23, 2025-05-26, 2025-05-27, 2025-05-28, 2025-05-29, 2025-05-30, 2025-06-02, 2025-06-03, 2025-06-04, 2025-06-05, 2025-06-06, 2025-06-10, 2025-06-11, 2025-06-12, 2025-06-13, 2025-06-16, 2025-06-17, 2025-06-18, 2025-06-19, 2025-06-20, 2025-06-23, 2025-06-24, 2025-06-25, 2025-06-26, 2025-06-27, 

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPBTC-124

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `p-cdh`.landed.profits_payments
# MAGIC SELECT
# MAGIC     Actual_date,
# MAGIC     ACCOUNT_NUMBER,	
# MAGIC     amount,	
# MAGIC     principal,	
# MAGIC     INTEREST,	
# MAGIC     COMMISSION,	
# MAGIC     EXPENSES,	
# MAGIC     unit_code,	
# MAGIC     TRX_UNIT,	
# MAGIC     TRX_USR,	
# MAGIC     TRX_SN,	
# MAGIC     Record_date,	
# MAGIC     REVERSED_FLG,	
# MAGIC     REVERSE_TRX_SN,	
# MAGIC     currency,	
# MAGIC     GD_REASON,	
# MAGIC     interest_url,
# MAGIC     isDownPayment,
# MAGIC     cast(BusinessDate as date),
# MAGIC     LoadingDateTime
# MAGIC FROM `d-cdh`.reference.profits_payments_archive

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move tables to qa from dev

# COMMAND ----------

spark.sql("create or replace table `qa-cdh`.reference.thesis_land_registry_mapping as select * from `d-cdh`.reference.thesis_land_registry_mapping")

# COMMAND ----------

spark.sql("create or replace table `qa-cdh`.reference.thesis_county_court_mapping as select * from `d-cdh`.reference.thesis_county_court_mapping")

# COMMAND ----------

spark.sql("create or replace table `qa-cdh`.reference.datetape_hie as select * from `d-cdh`.reference.datetape_hie")

# COMMAND ----------

# MAGIC %md
# MAGIC #### External Files Deployment CTAS statements

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`exta1_secured_ctr_code` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`exta1_secured_ctr_code` --ok
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`exta3_secured_ctr_code` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`exta3_secured_ctr_code` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extamortization`
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC as select * FROM `qa-cdh`.`externalfiles`.`extamortization` ---ok

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extassignmentpooldpocampaign` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extassignmentpooldpocampaign` 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extassignmentpoollowltvcampaign` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extassignmentpoollowltvcampaign` 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extassignmentpoolrestcampaigns` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extassignmentpoolrestcampaigns`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extauctionrecoveries` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extauctionrecoveries` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extcustomer_special_category` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extcustomer_special_category` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extdod_dpd_fcy` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extdod_dpd_fcy` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extfasttrack` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate') 
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extfasttrack` 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extkama` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extkama` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extlms_bucket_analysis` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extlms_bucket_analysis` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extlpfxb248` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extlpfxb248` 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extlpfxb302` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extlpfxb302` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extlpfxb364` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extlpfxb364` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extlsbr23t` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extlsbr23t` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extpaymentjumptotalperimeter` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extpaymentjumptotalperimeter` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extreport_unified_cl` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extreport_unified_cl` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extreport_unified_ml`
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate') 
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extreport_unified_ml` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extreport_unified_sb` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extreport_unified_sb` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extunionfile` 
# MAGIC CLUSTER BY (Snapshotdate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'Snapshotdate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extunionfile` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`externalfiles`.`extutp` 
# MAGIC CLUSTER BY (BusinessDate)
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
# MAGIC             'delta.minReaderVersion' = '3',
# MAGIC             'delta.minWriterVersion' = '7',
# MAGIC             'delta.columnMapping.mode' = 'name',
# MAGIC             'delta.dataSkippingStatsColumns' = 'BusinessDate')
# MAGIC AS SELECT * FROM `qa-cdh`.`externalfiles`.`extutp` 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.`workflow`.`externalfilescolumnconfiguration` AS
# MAGIC SELECT * FROM `qa-cdh`.`workflow`.`externalfilescolumnconfiguration` 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Refresh views

# COMMAND ----------

views_df = spark.sql("""
    SELECT table_name, table_schema, table_catalog
    FROM system.information_schema.tables
    WHERE table_catalog in ('p-cdh')
      AND table_schema in ('derivative', 'primary')
      and table_type='VIEW'
""")

# display(views_df)

for view_row in views_df.collect():
    table_name = view_row["table_name"]
    table_schema = view_row["table_schema"]
    table_catalog = view_row["table_catalog"]

    # print(table_name)

    full_view_name =f"`{table_catalog}`.{table_schema}.{table_name}"

    describe_full_view_name = spark.sql(f"""
              DESCRIBE EXTENDED {full_view_name}""")
    
    describe_full_view_name.createOrReplaceTempView("describe_full_view_name")

   # print(describe_full_view_name)

    describe_query = spark.sql(f"""
              select data_type--REGEXP_REPLACE(TRIM(data_type), '[^a-zA-Z0-9*<> ]', '') 
              AS cleaned_view_text 
              from describe_full_view_name 
              where col_name ='View Text'
              """)
    
    sql_view_creation = describe_query.collect()[0][0]

    create_or_replace_sql = f"""
          create or replace view {full_view_name} as 
          {sql_view_creation}
          """
    
    spark.sql(create_or_replace_sql)



# COMMAND ----------

views_df = spark.sql("""
    SELECT table_name, table_schema, table_catalog
    FROM system.information_schema.tables
    WHERE table_catalog in ('qa-cdh')
      AND table_schema in ('derivativearchive','primaryarchive')
      AND table_type='VIEW'
""")

for view_row in views_df.collect():
    table_name = view_row["table_name"]
    table_schema = view_row["table_schema"]
    table_catalog = view_row["table_catalog"]

    full_view_name = f"`{table_catalog}`.{table_schema}.{table_name}"

    try:
        describe_full_view_name = spark.sql(f"""
            DESCRIBE EXTENDED {full_view_name}
        """)

        describe_full_view_name.createOrReplaceTempView("describe_full_view_name")

        describe_query = spark.sql("""
            SELECT data_type
            FROM describe_full_view_name 
            WHERE col_name = 'View Text'
        """)

        sql_view_creation = describe_query.collect()[0][0]

        create_or_replace_sql = f"""
            CREATE OR REPLACE VIEW {full_view_name} AS 
            {sql_view_creation}
        """

        spark.sql(create_or_replace_sql)

        print(f"Successfully refreshed view: {full_view_name}")

    except Exception as e:
        print(f"   Failed to refresh view: {full_view_name}")


# COMMAND ----------

views_df = spark.sql("""
    SELECT table_name, table_schema, table_catalog
    FROM system.information_schema.tables
    WHERE table_catalog in ('qa-cdh')
      AND table_schema in ('derivativeactive','primaryactive')
      AND table_type='VIEW'
""")

for view_row in views_df.collect():
    table_name = view_row["table_name"]
    table_schema = view_row["table_schema"]
    table_catalog = view_row["table_catalog"]

    full_view_name = f"`{table_catalog}`.{table_schema}.{table_name}"

    try:
        describe_full_view_name = spark.sql(f"""
            DESCRIBE EXTENDED {full_view_name}
        """)

        describe_full_view_name.createOrReplaceTempView("describe_full_view_name")

        describe_query = spark.sql("""
            SELECT data_type
            FROM describe_full_view_name 
            WHERE col_name = 'View Text'
        """)

        sql_view_creation = describe_query.collect()[0][0]

        create_or_replace_sql = f"""
            CREATE OR REPLACE VIEW {full_view_name} AS 
            {sql_view_creation}
        """

        spark.sql(create_or_replace_sql)

        print(f"Successfully refreshed view: {full_view_name}")

    except Exception as e:
        print(f"   Failed to refresh view: {full_view_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Closed loans function update

# COMMAND ----------

env='p'
# Get the schema of the dexposure table
dexposure_table = spark.table(f"`{env}-cdh`.derivative.dexposure")
dexposure_schema_fields = dexposure_table.schema.fields


# Convert the schema fields into the required string format
dexposure_schema = ",\n".join([
    f"{field.name} {field.dataType.simpleString()}"
    for field in dexposure_schema_fields
])


spark.sql(f'''
CREATE OR REPLACE FUNCTION `{env}-cdh`.derivative.get_all_exposures_by_business_date(
    business_date Date
)
RETURNS TABLE 
( 
{dexposure_schema}
)
RETURN

SELECT B.*
FROM(
        SELECT  ExposureKey, MAX(BusinessDate) BusinessDate
        FROM `p-cdh`.derivative.dexposure
        WHERE BusinessDate <= business_date
        GROUP BY ExposureKey
) A
INNER JOIN   `{env}-cdh`.derivative.dexposure B
on a.ExposureKey = b.ExposureKey
and a.BusinessDate = b.BusinessDate
AND B.IsActiveFlagInt = 0
AND coalesce(B.JustificationCode,0) != 43023

UNION

SELECT * FROM `{env}-cdh`.derivative.dexposure
WHERE BusinessDate = business_date
AND IsActiveFlagInt = 1
''')

# COMMAND ----------


INSERT INTO `p-cdh`.`workflow`.`externalfilesconfiguration` VALUES
("extFilesAB","p-cdh","externalfiles","kama","Incremental",null,null,"BranchCode,CustomerID_PEL,CustomerName,CustomerAddress,CustomerCity,CustomerTK,CustomerAFM,CustomerIDNumber,CustomerBirthdate,CustomerOccupation,CustomerPhone,StatementAddress,StatementCity,StatementTK,GuarantorName,GuarantorAddress,GuarantorCity,GuarantorTK,GuarantorAFM,GuarantorIDNumber,GuarantorPhone,GuarantorBirthdate,GuarantorOccupation,ContractNo,ContractAmount,ContractDate,ContractUpdateDate,AccountNo,OpenAccountDate,LegalFrame,LegalFrameFlag,AccountStatus,PosostoEisforas,InterestRate,InterestRateType,InterestRate_Basis,InterestRateCategory,DateLiksisEpitokiou,InterestRateSpread,InterestRateSpreadSign,AmountApproved,AmountDisbursed,1stDisbursementDate,TotalPayments,TotalBalance,Bucket0,Bucket1,Bucket2,Bucket3,Bucket4,Bucket5,Bucket6,Bucket7+,DelinquentAmount,InstallmentAmount,TotalInstallments,DelinquentInstallments,MaxBucketDate,DaysInDelinquency,LastPayDate,LastPayAmount,NextDueDate,LoanDuration,GracePeriodFlag,GracePeriodEndDate,TroposEksipiretisis,ProductCode,PrenotationFlag,DateAnathesis,FlagAnathesis,AitiaEksairesis,LastDisbursementDate,BalanceBilling,BalanceBucket0Billing,CustomerSex,BucketDME,InstallmentsFrequency,Proorismos,DeferredCum,ApplicationNo,CustomerCategory,DebitAccountNo,Customer2Name,Customer2Phone,Customer3Name,Customer3Phone,Customer4Name,Customer4Phone,CommercialValue,ObjectiveValue,SeiraProsimeiosis,PosoProsimeiosis,KatigoriaAkinitou,KladosOikonomikisDrastiriotitas,Country,Nationality,Akinita_San_Collateral,Akinita_Amounts,YpoloipoTakseos_Akiniton,Metoxes_San_Collateral,Metoxes_Amounts,YpoloipoTakseos_Metoxon,Eggiiseis_San_Collateral,Eggiiseis_Amounts,YpoloipoTakseos_Eggiiseon,Eggiitikes_San_Collateral,Eggiitikes_Amounts,YpoloipoTakseos_Eggiitikon,Egg_Ell_Dimosiou_San_Collateral,Egg_Ell_Dimosiou_Amounts,YpoloipoTakseos_Egg_Ell_Dimosiou,Loipes_Eggiiseis_San_Collateral,Loipes_Eggiiseis_Amounts,YpoloipoTakseos_Loipon_Eggiiseon,Emporeumata_San_Collateral,Emporeumata_Amounts,YpoloipoTakseos_Emporeumaton,Apothikografa_San_Collateral,Apothikografa_Amounts,YpoloipoTakseos_Apothikografon,NeosTypos_San_Collateral,NeosTypos_Amounts,YpoloipoTakseos_NeouTypou,LostInfo,Next_Teliko_Epitokio,Next_Eidos_Epitokiou,Next_Epitokio_Vashs,Next_Epitokio,KodikosAkintou,Bucket7,Bucket8,Bucket9,Bucket10,Bucket11,Bucket12,Bucket13,DME_Diakanonismos,DME_Rithmisi,DME_Eggiisi_Ell_Dimosiou,Add_ApplicationNo,XrisiAkinitou,AccStatus_Change_Date,Monada_Diax_Enimerou_Pelati,Perigrafi_Monadas_Diax_Enimerou_Pelati,Monada_Diax_Anaggelmenou_Pelati,Perigrafi_Monadas_Diax_Anaggelmenou_Pelati,Ka8ysterimeno_Kefalaio,Ka8ysterimenoi_Tokoi,Ka8ysterimena_Eksoda,Kefalaiopoihsh_Tokwn,Epidotisi ell dimosiou,DaysPastDueUnrealisedInterest,CommercialFlag,UnrealisedInterestBefore31032013,EmporikhAgreementCode,Bucket NCKAG,Min bucket NCKAG_KAMAB3,LastRestructuringDate,ExposureExpiredDate,InitialExposureExpiredDate,Siglo Product Code,Abhost Account,BU,Abhost Contract,Field165,Field166,Field167,Field168,Field169,Field170,Field171,Field172"),
("extFilesAB","p-cdh","externalfiles","unionfile","Incremental",null,null,"EnglishColumn,SnapshotDate,Groupkey,GroupName,BorrowerKey,BorrowerName,BorrowerTaxIdentificationNumber,MonitoringUnitCode,ObligorRatingKey,ObligorRatingDate,ObligorCountry,UniqueExposureKey,ExposureKey,ExposureNumber,ExposureSourceSystemKey,ExposureCurrency,ExposureSegmentation,ProductType,IFRSGrossCarryingAmount,SuspendedInterest,AccruedSuspendedInterest,AccruedInterest,OnbalanceAmount,TotalRemainingForAmortization,BalanceOfPendingTransactions,OffBalanceAmount,UndrawnAmount,ImpairedFlag,ImpairmentType,IFRS9Stage,ImpairmentAllowance,FairValueAdjustmentsCR,ProvisionsL3869,TotalProvisionsOnBalance,ProvisionsOffBalance,ProvisionsUndrawnLimits,FairValueAdjustmentsFVPL,IFRS9ClassificationFlag,IFRS9POCIFlag,EliminationFlag,ServiceType,BookingEntity,OriginatingEntity,DaysPastDue,CurrentAmount,DelinquentAmount,DelinquentAmountBucket1,DelinquentAmountBucket2,DelinquentAmountBucket3,DelinquentAmountBucket4,DelinquentAmountBucket5,DelinquentAmountBucket6,DelinquentAmountBucket7,DelinquentAmountBucket8,DelinquentAmountBucket9,DelinquentAmountBucket10,DelinquentAmountBucket11,DelinquentAmountBucket12,DelinquentAmountBucket13,DelinquentAmountBucket14,BucketCategoryNMateriality,BucketCategoryWithMateriality,NPLNoMaterialityFlag,NPLWithMaterialityFlag,EBAStatus,UTPIndicator,ProbationStartDate,ECAAssetClass,FINREPAssetClass,IFRSAssetClass,ForbearanceTypeCode,ForbearanceType,ForbearanceDate,NaceCode,ECAIndustry,FINREPIndustry,IFRSIndustry,ECAPublicFlag,StateGuaranteeFlag,CollateralCapCash,CollateralCapRRE,CollateralCapCRE,CollateralCapORE,CollateralCapVessels,CollateralCapTbills,CollateralCapGGG,CollateralCapStateRec,CollateralCapInventory,CollateralCapAssRec,CollateralCapIndustrial,CollateralCapOtherCollateral,CollateralCapOtherFinGuarantee,CollateralCapTotalTangible,CollateralCapCorporateGuarantee,CollateralCapPersonalGuarantee,HFSFlag,SPVsCapitalization,DeNuncementFlag,LTV,LTVRE,ForbearanceMeasuresCount,ForbearanceOfNPEFlag,FPLUnderProbationFlag,AgreementKey,CREStocktakeFlag,FINREPAssetClassSubClassCode,FinancialDifficultyFlag,InterestRate,EffectiveInterestRate,AcMaintenServiceBranch,RelationshipUnit,ConnectedBorrowerCategory,ExposureMaturityDate,ObligorLimitEndDate,LitigationFlag,FinRepCre,FinRepRre,BucketGt1Last12MFlag,BucketGt1Last12MDate,ObligorLimit,SecuritizationCode,MainBorrowerKey,MainBorrowerTaxIdentificationNumber,TepixCovidFlag,NonCoreFlag,PastDueDaysObligor,PastDueDaysFacility,DefaultStatus,DefaultStatusReasonCode,DefaultDate,NPVLossOver1PercentFlag,EliminatedEntityCode,PBOriginationDate,CPRSLevel1,CPRSLevel2,ProjectFinanceIndicator"),
("extFilesAB","p-cdh","externalfiles","report_unified_sb","Incremental",null,null,"ApplicationNumber,CDI,PartyTaxIdentificationNumber,PartyName,ApplicationBranch,BranchManagmentDepartment,Implementation,ImplementationReason,ProductType,AuthorityLevel,ApprovalLevel,ApplicationActivationBranch,BranchName,Desicion,StepId,Description,UnifiedApplicationStatus,FinancingType,Currency,LegalType,Implementation2,UserImplemented,TemporaryQueues,CounselUser,NonKATChannel,AgreementKey,RiskRatingResult,FinancialDifficulty,FinancialDifficultyContraindication,DEK,IsCompanyClosed,DTMEStep6User,L4307Flag,MINRouteUserRevaluate,MAXRouteUserRevaluate,RestrectedFlag,WriteoffFlag,WriteoffDuration,WriteoffSequence,DecisionReason,CompanySegment,MINRouteUserDLPAnalysis,MAXRouteUserDLPAnalysis,MINRouteUserDLPnotAnalysis,MAXRouteUserDLPnotAnalysis,Code,Stakod,StakodDescription,sal,NPLHubs,ResrtucturingType,AutoCreditFlag,ReturnReason,Denounced,ImplementationApplicationNumber,Channel,ExposureKey,ContractNo,NPLSolution,ImplementationTeam,Scale,AutoPrintFlag,PrintException,L3869Flag,L3869Commonprod,RequestAutocredit,ApplicationAutocredit,InitialSignature,ExcludedParticipant,ApprovedExcludedParticipant,ApprovalDifferentTerms,ApplicationApprovalWithDifferentTerms,SignatureRequired,ApplicationSignatureRequired,TotalCreditRiskOverLimit,NewAdditionalNote,NextActions,UserDataValidatorProcessorPerApplication,ResponsibleApprovalAuthorityLevel,ConfirmRegistrationCorrectnessFlag,ConfirmDocumentsCompleteness,ApplicationDynamicChangedFlag,ApprovalConditions,ReValuationFlag,LegalAction,L4605Flag,RegistratorCode,RegistratorName,ExposureDeclarationStandingOrderException,StandaloneImplementation,ImplemenationStatus,ConsiderControlProductionContracts,ConsiderImplementationControl,ResponsibleRegisterNotice,ResponsibleCode,RegisterNotice,AssignedFlag,Result,TerminationReason,ApplicationImplementationFlag,Exposures,ImplementationConsideration,GalaxyFlag,GalaxySecuritization,Covid19,Covid19SB,Authority,AuthorityDynamicChangedFlag"),
("extFilesAB","p-cdh","externalfiles","report_unified_ml","Incremental",null,null,"ApplicationBranch,ApplicationNumber,CommonApplicationStep,AgreementKey,StepId,Decision,Implementation,ImplementationApplicationNumber,Alphanet,PartyName,Channel,LastApprovalUser,LoanRequestStatus,ContractPrint,RequestType,GracePeriod,Capitalization,PaymentFrequency,Elongation,RemainingTermOfLoan,ElongationMonths,NewRemainingTermOfLoan,facilitation,SupplementaryPrenotation,RateType,BaseRate,AdjustedApproval,ApprovalLevel,Comments,ApprovalTerms,FinancialDifficulty,FinancialDifficultyType,FinancialDifficultyContraindication,DEK,DeletionType,AgreementNumber,DecisionReason,RestructuringType,CurrentStepId,ApplicationActivationBranch,NplHub,Hub,ProductType,AutoCreditFlag,ReturnReason,UnifiedApplicationStatus,Denounced,NPLSolution,ExposureKey,SerialNumber,PartyTaxIdentificationNumber,LevelDescription,ImplemenationApplicationStep,ImplementationApplicationDivision,AutoPrintFlag,PrintException,L3869Flag,L3869Commonprod,RequestAutocredit,ApplicationAutocredit,InitialSignature,ExcludedParticipant,ApprovedExcludedParticipant,ApprovalDifferentTerms,ApplicationApprovalWithDifferentTerms,SignatureRequired,ApplicationSignatureRequired,TotalCreditRiskOverLimit,NextActions,UserDataValidatorProcessorPerApplication,ResponsibleApprovalAuthorityLevel,ConfirmRegistrationCorrectnessFlag,ConfirmDocumentsCompleteness,ApplicationDynamicChangedFlag,ApprovalConditions,ReValuationFlag,ApprovalSubRole,LegalAction,L4605Flag,RegistratorCode,RegistratorName,ExposureDeclarationStandingOrderException,StandaloneImplementation,ImplemenationStatus,ConsiderControlProductionContracts,ConsiderImplementationControl,ResponsibleRegisterNotice,ResponsibleCode,RegisterNotice,AssignedFlag,Result,TerminationReason,ApplicationImplementationFlag,Exposures,ImplementationConsideration,GalaxyFlag,GalaxySecuritization,Covid19,Covid19SB,Authority,AuthorityDynamicChangedFlag"),
("extFilesAB","p-cdh","externalfiles","report_unified_cl","Incremental",null,null,"ApplicationBranch,ApplicationNumber,CommonApplicationStep,CDI,PartyName,PartyTaxIdentificationNumber,UserFullName,Product,Origination,DEK,ExposureKey,Decision,DecisionReason,ApprovalTerms,NextActions,HraklisAgreementKey,DisbursementCompletion,ImplementationApplicationNumber,ImplemenationApplicationStep,RestructuringType,CurrentApplicationStepCode,ApplicationActivationBranch,Hub,AutoCreditFlag,ReturnReason,UnifiedApplicationStatus,NewProduct,Channel,Litigious,NPLSolution,ProductCode,TeamNumber,AuthorityLevel,ImplementationApplicationDivision,CheckContractualDocuments,AutoPrintFlag,PrintException,L3869Flag,L3869CommonProduct,RequestAutocredit,ApplicationAutocredit,InitialSignature,ExcludedParticipant,ApprovedExcludedParticipant,ApprovalDifferentTerms,ApplicationApprovalWithDifferentTerms,SignatureRequired,ApplicationSignatureRequired,TotalCreditRiskOverLimit,SupplementaryPrenotation,UserDataValidatorProcessorPerApplication,ResponsibleApprovalAuthorityLevel,ConfirmRegistrationCorrectnessFlag,ConfirmDocumentsCompleteness,ApplicationDynamicChangedFlag,ApprovalConditions,ReValuationFlag,LegalAction,L4605Flag,RegistratorCode,RegistratorName,ExposureDeclarationStandingOrderException,StandaloneImplementation,ImplemenationStatus,ConsiderControlProductionContracts,ConsiderImplementationControl,ResponsibleRegisterNotice,ResponsibleCode,RegisterNotice,AssignedFlag,Result,TerminationReason,ApplicationImplementationFlag,Exposures,ImplementationConsideration,GalaxyFlag,GalaxySecuritization,Covid19,Covid19SB,Authority,AuthorityDynamicChangedFlag"),
("extFilesAB","p-cdh","externalfiles","lsbr23t","Incremental",null,null,"SourceSystem,ExposureKey,AgreementKey,SystemAgreementKey,CDI,ImplementedCollateralOrPropertyNumber,CollateralCategory,Country,District,City,CityDescription,PostalCode,LienInitialOrder,LastEvaluationAmount,LastEvaluationDate,LastEvaluationType,LastEvaluationCurrency,OriginalEvaluationAmount,OriginalEvaluationDate,OriginalEvaluationType,OriginalEvaluationCurrency,PropertySurface,ContructionYear,ImplementedCollateralNumber,PrenotationAmount,RealEstateCollateralStatus,ExposureStatus,MarketValue,MarketValueEvaluationDate,MarketValueEvaluationType,MarketValueEvaluationCurrency,LastAutEvaluationAmount,LastAutEvaluationDate,LastAutEvaluationType,LastAutEvaluationCurrency,LastFairValueAmount,LastFairValueDate,LastFairValueType,LastFairValueCurrency,TotalBalance,NonCapitalizedInterest,Plot,CollateralStatus,ImplementedCollateralCurrency,PartyTaxRegistrationNumber,ContractNo,Address,LienInitialOrderDate,LienInitialOrderType,PriorClaimsIndicator,PriorClaimsAmount,PropertyCompletionPercentage,CollateralType,LawFirm,AmountForBank,OldHoistCode,RelationshipManagementUnit,DelayManagementUnit,BU,PartyName,Branch,AddressStreet,AddressStreetNumber,Floor,AuxiliarySpacesSurface,_c6"),
("extFilesAB","p-cdh","externalfiles","lpfxb248","Incremental",null,null,"CDI,Party,PartyTaxIdentificationNumber,EconomicActivtityBranch,RevenueYear,PartyCategory,LegalForm,RelationshipManagementUnit,DelayManagementUnit,GroupCode,GroupName,BranchCode,BranchName,Limit,ExposureStatus,LastInstallmentDate,RestructuringTypeCode,RestructuringType,LegalFrameWorkCode,ProductTypeCode,ProductType,ServiceMethod,CustomerCreditRankingABRS,CustomerCreditRankingDLP,AgreementKey,FastTrackSB,ContractSys,RequestWF,SubRequestWF,LastRequestWF,LastSubRequestWF,FinancePurpose,CapitalizedInterest,Reason,LoanAcknowledgment,ContractualTermMarginChangeIndicator,CompanyGuaranteeIndicator,StepUpDuration,InterestPeriodExpiry,InterestRate,DisposalAmount"),
("extFilesAB","p-cdh","externalfiles",null,"Incremental",null,null,"null"),
("extFilesAB","p-cdh","externalfiles",null,"Snapshot",null,null,"null"),
("extFilesAB","p-cdh","externalfiles","paymentjumptotalperimeter","Incremental",null,null,"ExposureKey,PaymentJumpMonth"),
("extFilesAB","p-cdh","externalfiles","lms_bucket_analysis","Incremental",null,null,"ExposureKey,SourceSystemIndicator,Currency"),
("extFilesAB","p-cdh","externalfiles","utp","Incremental",null,null,"AC_CODE,AC_SRC_STM_CODE,UTP_IND,IDENTN_MTH_DSC"),
("extFilesAB","p-cdh","externalfiles","customer_special_category","Incremental",null,null,"CDI,SensitiveCases"),
("extFilesAB","p-cdh","externalfiles","lpfxb302","Incremental",null,null,"CDI,DebtorName,ExposureKey,ExposureType,Product,AgreementKey,RelationshipManagementUnit,DelayManagementUnit,Users,Branch,WrittenOffTypeL4465,RelatedAccountNumber,SecuritizationCode,SecuritizationCodeEOM,CurrentAccountStatement"),
("extFilesAB","p-cdh","externalfiles","fasttrack","Incremental",null,null,"ExposureKey,CDI,GrantAccount,PositiveForInstallmentsExtensionReduction,BankNumber,RestructuringAmount,ProgramIndicatorProsarmozo,EffectiveDateOfNewInstallments,ProsarmozoDuration,ProsarmozoAmount,AccruedInterestChargeIndicator,RestructuringChannel,InterestRatesChange,InterestRateAPeriod,SpreasAPeriod,DurationAPeriod,InterestRateBPeriod,SpreasBPeriod,PeriodType,PeriodNumber,StandardDate,PeriodChanged,ExtensionInstallments,FastTrackFlag,SubmitDate,Status,ErrorDescription,TIMESTAMP"),
("extFilesAB","p-cdh","externalfiles","ExtensionStepUpImplementations","Incremental",null,null,"ExposureKey,ProductCode,CDI,MidenikoProsarmozoIndicator,DebtCapitalizationIndicator,ProsarmozoIndicator,ProsarmozoDuration,ProsarmozoIndicator_2,ProsarmozoCapitalizationFrequency_2,ProsarmozoCapitalizationFrequency_2,AnchorProductCode,Channel,StepUpIndicator,StepUpDuration,StepUpPeriodxAdjustmentDuration,Description,ApplicationStatus"),
("extFilesAB","p-cdh","externalfiles","assignmentpoolrestcampaigns","Incremental",null,null,"ExposureKey,NewAssignment,NewActive,Assignment,AssetSegment,Balance,EBAStatus,Active,ActivityTypeKey,ActivityDate,ActivityCode,Subcategory,Category,Modifications,ProsarmozoDuration,RestructuringDate,Month,InLastAssignmentAndInOldPool,Channel,AssignmentMonth,Flag"),
("extFilesAB","p-cdh","externalfiles","auctionrecoveries","Incremental",null,null,"PartyTaxRegistrationNumber,UniqueId,ProgramNumber,AuctionDate,AgreementKey,ExposureKey,ConfirmedCreditAmount,ExposureType,FirstBidder,Hastener,PortfolioName,LawFirm,CreditDate,AuctionFirstOfferPrice"),
("extFilesAB","p-cdh","externalfiles","lpfxb364","Incremental",null,null,"NewExposureKey,OldExposureKey,RestructuringTypeCode,RestructuringType,IndPortfolio,ExposureType,RestructuringProductKey,AgreementKey,CDI,PartyTaxRegistrationNumber,Forbearance"),
("extFilesAB","p-cdh","externalfiles","assignmentpooldpocampaign","Incremental",null,null,"ExposureKey,BalanceGroup,SegmentDPO,DenouncedFlag,EAP,Balance,EBAStatus,DPOModified,ModificationMonth,Segments,ActiveCurrentMonthexclImplem"),
("extFilesAB","p-cdh","externalfiles","AssignmentPoolLowLTVCampaign","Incremental",null,null,"ExposureKey,Assignment,AssignmentDate,Segment,AssignmentChannel,AssetSegment,Balance"),
("extFilesAB","p-cdh","externalfiles","recoveries_total","Incremental",null,null,"PartyName,PartyTaxRegistrationNumber,AuctionDate,UniqueId,ExecutionExpense,TotalAmount"),
("extFilesAB","p-cdh","externalfiles","amortization","Incremental",null,null,"ReportedMonth,AssetSegment,CDI,ExposureKey,Category,CoreNoCoreFlag"),
("extFilesAB","p-cdh","externalfiles","A1_SECURED_CTR_CODE","Incremental",null,null,"CLT_REL_AC_CODE,CLT_REL_AC_GNL_CTR_CODE,SUM_of_CLT_AMT"),
("extFilesAB","p-cdh","externalfiles","A3_SECURED_CTR_CODE","Incremental",null,null,"CLT_REL_AC_CODE,CLT_REL_AC_GNL_CTR_CODE,SUM_of_CLT_AMT"),
("extFilesAB","p-cdh","externalfiles","inventory","Incremental",null,null,"ExposureKey,ExposureKeyWithConfirmedDebts,Comments"),
("extFilesAB","p-cdh","externalfiles","dod_dpd_fcy","Incremental",null,null,"AC_SRC_STM_CODE,AC_CODE")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA2-369

# COMMAND ----------

dates_df = spark.sql("""
    SELECT DISTINCT CAST(BusinessDate AS STRING) as BusinessDate
    FROM `p-cdh`.derivativeactive.DExposure
    order by businessdate asc
""")
dates = [row['BusinessDate'] for row in dates_df.collect()]


# COMMAND ----------

env="p"
for run_date in dates: 
    exposurerequest = spark.sql(f'''
            select ExposureKey, RequestExpiryDate, ExposureStatusCode, RequestTypeCode, SourceSystem, BusinessDate
            from  `{env}-cdh`.primary.exposurerequest
            where BusinessDate = {run_date!r}
            and SourceSystem = 'profits'
            ''' )
    exposurerequest.createOrReplaceTempView("exposurerequest")
    exposure_request_final = spark.sql("""
            select  ExposureKey, 
                    min(RequestExpiryDate) FirstArrDate, 
                    max(RequestExpiryDate) DateLastInArrears
            from exposurerequest
            where RequestTypeCode in ('1', '2', '4')
            and ExposureStatusCode != '1'
            and RequestExpiryDate <= BusinessDate
            group by ExposureKey
    """)
    exposure_request_final.createOrReplaceTempView("exposure_request_final")
    spark.sql(f"""
            MERGE INTO `{env}-cdh`.derivativeactive.dexposure dexposure
            USING exposure_request_final
            ON dexposure.ExposureKey = exposure_request_final.ExposureKey
            AND dexposure.Businessdate = {run_date!r}
            AND dexposure.sourcesystem = "profits"
            WHEN MATCHED THEN
                UPDATE SET 
                    dexposure.DateLastInArrears = exposure_request_final.DateLastInArrears,
                    dexposure.FirstArreardate = exposure_request_final.FirstArrDate
    """)
    print(f"Updated for date {run_date}")
