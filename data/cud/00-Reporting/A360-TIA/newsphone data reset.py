# Databricks notebook source
# DBTITLE 1,Tables to truncate
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.landing.newsphone_auctions_backup DEEP CLONE `p-cdh`.landing.newsphone_auctions;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.landed.newsphone_auctions_backup DEEP CLONE `p-cdh`.landed.newsphone_auctions;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.landing.newsphone_basic_data_backup DEEP CLONE `p-cdh`.landing.newsphone_basic_data;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.landed.newsphone_basic_data_backup DEEP CLONE `p-cdh`.landed.newsphone_basic_data;
# MAGIC
# MAGIC TRUNCATE TABLE `p-cdh`.landing.newsphone_auctions;
# MAGIC TRUNCATE TABLE `p-cdh`.landed.newsphone_auctions;
# MAGIC TRUNCATE TABLE `p-cdh`.landing.newsphone_basic_data;
# MAGIC TRUNCATE TABLE `p-cdh`.landed.newsphone_basic_data;

# COMMAND ----------

# DBTITLE 1,Tables to delete from a sourcesystem
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.primaryactive.auctionrealestate_backup DEEP CLONE `p-cdh`.primaryactive.auctionrealestate;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.derivativeactive.dauctionrealestate_backup DEEP CLONE `p-cdh`.derivativeactive.dauctionrealestate;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.primaryactive.auction_backup DEEP CLONE `p-cdh`.primaryactive.auction;
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.derivativeactive.dauction_backup DEEP CLONE `p-cdh`.derivativeactive.dauction;
# MAGIC
# MAGIC DELETE from `p-cdh`.primaryactive.auctionrealestate WHERE SourceSystem = 'extfile';
# MAGIC DELETE from `p-cdh`.primaryactive.auction WHERE SourceSystem = 'extfile';
# MAGIC DELETE from `p-cdh`.derivativeactive.dauction WHERE SourceSystem = 'extfile';
# MAGIC DELETE from `p-cdh`.derivativeactive.dauctionrealestate WHERE SourceSystem = 'extfile';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `d-de-sandbox`.a360.tia
# MAGIC where AuctionObJID = 550738
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-de-sandbox`.a360.tia
# MAGIC where AuctionObJID = 550738
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-cdh-delivery`.tia.tia
# MAGIC where AuctionObJID = 550738
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select AuctionRealEstateKey, * from  `p-cdh`.derivativeactive.dauctionrealestate 
