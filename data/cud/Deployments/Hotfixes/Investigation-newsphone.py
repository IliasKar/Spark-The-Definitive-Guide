# Databricks notebook source
# DBTITLE 1,Landing
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM `p-cdh`.landing.newsphone_auctions
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjID ORDER BY file_date DESC) = 1
# MAGIC ) AS latest_auctions
# MAGIC

# COMMAND ----------

# DBTITLE 1,Landed
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY AuctionObjID ORDER BY file_date DESC) = 1
# MAGIC ) AS latest_auctions

# COMMAND ----------

# DBTITLE 1,MIssing AuctionObjIDs
# MAGIC %sql
# MAGIC
# MAGIC SELECT src.AuctionObjID
# MAGIC FROM (
# MAGIC   SELECT AuctionObjID, MAX(FileDate) AS max_file_date_src
# MAGIC   FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC   GROUP BY AuctionObjID
# MAGIC ) src
# MAGIC LEFT JOIN (
# MAGIC   SELECT AuctionObjID, MAX(file_date) AS max_file_date_tgt
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   GROUP BY AuctionObjID
# MAGIC ) tgt
# MAGIC ON src.AuctionObjID = tgt.AuctionObjID
# MAGIC WHERE tgt.max_file_date_tgt IS NULL
# MAGIC    OR src.max_file_date_src > tgt.max_file_date_tgt
# MAGIC

# COMMAND ----------

# DBTITLE 1,Total missing
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC WHERE AuctionObjID IN (
# MAGIC   SELECT src.AuctionObjID
# MAGIC   FROM (
# MAGIC     SELECT AuctionObjID, MAX(FileDate) AS max_file_date_src
# MAGIC     FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC     GROUP BY AuctionObjID
# MAGIC   ) src
# MAGIC   LEFT JOIN (
# MAGIC     SELECT AuctionObjID, MAX(file_date) AS max_file_date_tgt
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC     GROUP BY AuctionObjID
# MAGIC   ) tgt
# MAGIC   ON src.AuctionObjID = tgt.AuctionObjID
# MAGIC   WHERE tgt.max_file_date_tgt IS NULL
# MAGIC      OR src.max_file_date_src > tgt.max_file_date_tgt
# MAGIC )
# MAGIC
