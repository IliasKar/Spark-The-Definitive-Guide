# Databricks notebook source
## RUN THIS

spark.sql("""
INSERT INTO `p-cdh`.`workflow`.ExternalFilesConfiguration
(RelativePath, UnityCatalog, UnitySchema, TableName, Flow, ValidatorScript, NotificationEmails)
VALUES ('A360', 'p-cdh', 'reference', NULL, 'Incremental', NULL, NULL);
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alter datatypes of reference.servicer_report_securedperimeter
# MAGIC ALTER TABLE `p-cdh`.`reference`.`servicer_report_securedperimeter`
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC  
# MAGIC ALTER TABLE `p-cdh`.`reference`.`servicer_report_securedperimeter`
# MAGIC DROP COLUMNS TLC, GBV, CREV;
# MAGIC  
# MAGIC ALTER TABLE `p-cdh`.`reference`.`servicer_report_securedperimeter`
# MAGIC ADD COLUMN TLC DECIMAL(19,2) AFTER DebtorVAT;
# MAGIC  
# MAGIC ALTER TABLE `p-cdh`.`reference`.`servicer_report_securedperimeter`
# MAGIC ADD COLUMN GBV DECIMAL(19,2) AFTER TLC;
# MAGIC  
# MAGIC ALTER TABLE `p-cdh`.`reference`.`servicer_report_securedperimeter`
# MAGIC ADD COLUMN CREV DECIMAL(19,2) AFTER GBV;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.`reference`.`dk_PEErestructuremapping`
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.`reference`.`dk_PEErestructuremapping`
# MAGIC drop column `_c3`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changes for portfolio
# MAGIC
# MAGIC ## Steps
# MAGIC * Upload the new portfolio.json in p-ec1-cepal-common-zone/portfolio/
# MAGIC * Run the following code

# COMMAND ----------

spark.sql("""
Update `p-cdh`.primaryactive.portfolio 
SET InvestorCountryCode= 'IE'
WHERE InvestorCountry like '%ireland%'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMPT-104 
# MAGIC
# MAGIC ### Steps
# MAGIC * Run the following code
# MAGIC * Re-run case_code_qca for business date: 2024-11-21 and onwards

# COMMAND ----------

spark.sql('''alter table `p-cdh`.primaryactive.casecode
add column MinimumPaymentAmount decimal(19,4) after AccountNumberRelationship ;''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## MINIPJS-102 
# MAGIC
# MAGIC ### Steps
# MAGIC * Run the following code 

# COMMAND ----------


spark.sql("""alter table `p-cdh`.primaryactive.party
           add column TargetLegalActionCode int after L3869BorrowerHasOver50KBalance;""")

# COMMAND ----------


spark.sql("""alter table `p-cdh`.primaryactive.party 
          add column TargetLegalAction string after TargetLegalActionCode;""")


# COMMAND ----------


spark.sql("""alter table `p-cdh`.derivativearchive.dparty 
          add column TargetLegalActionCode int after L3869BorrowerHasOver50KBalance;""")

# COMMAND ----------

spark.sql("""alter table `p-cdh`.derivativearchive.dparty 
          add column TargetLegalAction string after TargetLegalActionCode;""")

# COMMAND ----------

spark.sql("""alter table `p-cdh`.derivativeactive.dparty 
          add column TargetLegalActionCode int after L3869BorrowerHasOver50KBalance;""")

# COMMAND ----------

spark.sql("""alter table `p-cdh`.derivativeactive.dparty 
          add column TargetLegalAction string after TargetLegalActionCode;""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D360-935 
# MAGIC ### Steps 
# MAGIC * Run the first three cells first
# MAGIC * Re-run auctionrealestate for all businessdates
# MAGIC * Then run the other cells
# MAGIC

# COMMAND ----------

spark.sql("""
ALTER TABLE `p-cdh`.primaryactive.auctionrealestate DROP COLUMN DebtorTaxRegistrationNumber;
""")

# COMMAND ----------

spark.sql("""ALTER TABLE `p-cdh`.primaryactive.auctionrealestate DROP COLUMN DebtorDescription;""")

# COMMAND ----------

spark.sql("""ALTER TABLE `p-cdh`.primaryactive.auctionrealestate DROP COLUMN AuctionKey;""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second step

# COMMAND ----------

spark.sql("""
CREATE TABLE `p-cdh`.derivativeactive.DAuctionRealEstate AS 
select * from `qa-cdh`.it.dauctionrealestate
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW  `p-cdh`.derivative.DAuctionRealEstate AS
select * from `p-cdh`.derivativeactive.DAuctionRealEstate
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ΜΟΝΖΑ-172 , DBTC - 547
# MAGIC
# MAGIC ### Steps
# MAGIC * Run the following code
# MAGIC * Re-Run Dexposure for all business dates in production.

# COMMAND ----------

add_column_type_1 =f"""
ALTER TABLE `p-cdh`.derivativeactive.dexposure
ADD COLUMN ExtrajudicialLetterEverFlag boolean
AFTER ApplicationDecision;"""

spark.sql(add_column_type_1 )

# COMMAND ----------


add_column_type_2 =f"""
ALTER TABLE `p-cdh`.derivativeactive.dexposure
ADD COLUMN ExtrajudicialLetterLastDate boolean
AFTER ExtrajudicialLetterEverFlag;"""

spark.sql(add_column_type_2 )

# COMMAND ----------


add_column_type_3 =f"""
ALTER TABLE `p-cdh`.derivativeactive.dexposure
ADD COLUMN CourtRegion string
AFTER ExtrajudicialLetterLastDate ;"""

spark.sql(add_column_type_3 )

# COMMAND ----------


drop_old_column_1 =f"""
ALTER TABLE `p-cdh`.derivativeactive.dexposure
DROP COLUMN ExtraJudicialLetterFlag;"""

spark.sql(drop_old_column_1 )

# COMMAND ----------


drop_old_column_2 =f"""
ALTER TABLE `p-cdh`.derivativeactive.dexposure
DROP COLUMN ExtraJudicialLetterDate;"""

spark.sql(drop_old_column_2 )

# COMMAND ----------


add_column_type_1 =f"""
ALTER TABLE `p-cdh`.derivativearchive.dexposure
ADD COLUMN ExtrajudicialLetterEverFlag boolean
AFTER ApplicationDecision;"""

spark.sql(add_column_type_1 )

# COMMAND ----------


add_column_type_2 =f"""
ALTER TABLE `p-cdh`.derivativearchive.dexposure
ADD COLUMN ExtrajudicialLetterLastDate boolean
AFTER ExtrajudicialLetterEverFlag;"""

spark.sql(add_column_type_2 )

# COMMAND ----------


add_column_type_3 =f"""
ALTER TABLE `p-cdh`.derivativearchive.dexposure
ADD COLUMN CourtRegion string
AFTER ExtrajudicialLetterLastDate ;"""

spark.sql(add_column_type_3 )

# COMMAND ----------


drop_old_column_1 =f"""
ALTER TABLE `p-cdh`.derivativearchive.dexposure
DROP COLUMN ExtraJudicialLetterFlag;"""

spark.sql(drop_old_column_1 )

# COMMAND ----------


drop_old_column_2 =f"""
ALTER TABLE `p-cdh`.derivativearchive.dexposure
DROP COLUMN ExtraJudicialLetterDate;"""

spark.sql(drop_old_column_2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA2-129 
# MAGIC
# MAGIC ### Steps 
# MAGIC * Re run application_sme for all businessdates in production

# COMMAND ----------

# MAGIC %md
# MAGIC ## D360-2166
# MAGIC ### Steps
# MAGIC * Re run assignment_qca for all businessdate in production

# COMMAND ----------

# MAGIC %md
# MAGIC ### RUN THE FOLLOWING NOTEBOOK IN ORDER TO REFRESH THE VIEWS
# MAGIC https://dbc-7a36af91-f82b.cloud.databricks.com/editor/notebooks/610302587438997?o=6854132182247269#command/4099643969417334
