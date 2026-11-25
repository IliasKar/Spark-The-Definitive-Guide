# Databricks notebook source
# MAGIC %md
# MAGIC #### PMT-621: Enhancements for AssignmentSegmentDetailed field

# COMMAND ----------

# MAGIC %md
# MAGIC #### D360-1077
# MAGIC #### Update the following CUD table:
# MAGIC #### DAuction --> HastenerVAT renamed to HastenerTaxRegistrationNumber, Conductiondate renamed to ConductionDate, Publisheddate renamed to PublishedDate
# MAGIC
# MAGIC #### *!!! Please note that these changes are due to changes in glue.app repository (cup tables: AuctionRealEstate & Auction).*
# MAGIC *(relevant pull request in glue repo: [1288] (https://eu-central-1.console.aws.amazon.com/codesuite/codecommit/repositories/gr.cepal.dna.de.glue.app/pull-requests/1288/details))*
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction RENAME COLUMN HastenerVAT TO HastenerTaxRegistrationNumber;
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction RENAME COLUMN Conductiondate TO Conduction;
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction RENAME COLUMN Conduction TO ConductionDate;
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction RENAME COLUMN Publisheddate TO Published;
# MAGIC ALTER TABLE `p-cdh`.derivative.dauction RENAME COLUMN Published TO PublishedDate;

# COMMAND ----------

# MAGIC %md
# MAGIC #### D360-1141 
# MAGIC #### Changes to the following CUD tables:
# MAGIC * #### DCollateral --> Add columns SourceSystem,  OwnershipKey & Rename PropertyStatus to RealEstateStatus, CollateralCurrency to Currency

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `p-cdh`.derivative.DCollateral SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.DCollateral RENAME COLUMN PropertyStatus TO RealEstateStatus;
# MAGIC ALTER TABLE `p-cdh`.derivative.DCollateral RENAME COLUMN CollateralCurrency TO Currency;
# MAGIC
# MAGIC ALTER TABLE `p-cdh`.derivative.DCollateral ADD COLUMN SourceSystem string AFTER DCollateralId; 
# MAGIC ALTER TABLE `p-cdh`.derivative.DCollateral ADD COLUMN OwnershipKey INT AFTER NonRealEstateKey; 
