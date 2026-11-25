# Databricks notebook source
# MAGIC %md
# MAGIC # Repair missing (AuctionObjID, FileDate) pairs
# MAGIC newsphone_basic_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_basicdata AS (
# MAGIC   SELECT *
# MAGIC   FROM `p-cdh`.landed.newsphone_basic_data
# MAGIC   WHERE BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_basic_data
# MAGIC   )
# MAGIC ),
# MAGIC source_counts AS (
# MAGIC   SELECT
# MAGIC     UniqueCode,
# MAGIC     COUNT(DISTINCT FileDate) AS source_filedate_count
# MAGIC   FROM `p-de-sandbox`.a360.newsphone_basicdata_reload_final
# MAGIC   WHERE FileDate <> '2024-01-10'
# MAGIC   GROUP BY UniqueCode
# MAGIC ),
# MAGIC target_counts AS (
# MAGIC   SELECT
# MAGIC     UniqueCode,
# MAGIC     COUNT(DISTINCT file_date) AS target_filedate_count
# MAGIC   FROM latest_basicdata
# MAGIC   GROUP BY UniqueCode
# MAGIC )
# MAGIC SELECT
# MAGIC   s.UniqueCode,
# MAGIC   s.source_filedate_count,
# MAGIC   COALESCE(t.target_filedate_count, 0) AS target_filedate_count
# MAGIC FROM source_counts s
# MAGIC LEFT JOIN target_counts t
# MAGIC   ON s.UniqueCode = t.UniqueCode
# MAGIC WHERE COALESCE(t.target_filedate_count, 0) < s.source_filedate_count
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC     FROM `p-cdh`.landed.newsphone_basic_data npa
# MAGIC     WHERE npa.BusinessDate = (
# MAGIC         SELECT MAX(BusinessDate)
# MAGIC         FROM `p-cdh`.landed.newsphone_basic_data
# MAGIC     )
# MAGIC     AND npa.UniqueCode = 'BIbxdJEcsa'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM `p-de-sandbox`.a360.newsphone_basicdata_reload_final npbr
# MAGIC where npbr.UniqueCode = 'BIbxdJEcsa'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT     
# MAGIC     npbr.UniqueCode,
# MAGIC     npbr.Status,
# MAGIC     npbr.Court_Type,
# MAGIC     npbr.ConductionDate,
# MAGIC     npbr.PublishDate,
# MAGIC     npbr.InitialPrice,
# MAGIC     npbr.Type,
# MAGIC     npbr.DebtorVatNumber,
# MAGIC     npbr.DebtorDescr,
# MAGIC     TRY_CAST(npbr.MultipleDeptors AS BOOLEAN) AS MultipleDeptors,
# MAGIC     npbr.HastenerDescr,
# MAGIC     npbr.Servicer,
# MAGIC     npbr.HastenerVat,
# MAGIC     npbr.HastenerCountry,
# MAGIC     npbr.RelatedAuctionCode,
# MAGIC     TRY_CAST(npbr.HasJudgment AS BOOLEAN) AS HasJudgment,
# MAGIC     npbr.FileDate AS file_date,
# MAGIC     CONCAT('BasicData_', REPLACE(npbr.FileDate, '-', ''), '.csv') AS file_name,
# MAGIC     CURRENT_DATE - 1 AS BusinessDate,  
# MAGIC     CURRENT_TIMESTAMP AS LoadingDateTime
# MAGIC FROM `p-de-sandbox`.a360.newsphone_basicdata_reload_final npbr
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY npbr.UniqueCode ORDER BY npbr.FileDate DESC) = 1
# MAGIC AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM `p-cdh`.landed.newsphone_basic_data npa
# MAGIC     WHERE npa.BusinessDate = (
# MAGIC         SELECT MAX(BusinessDate)
# MAGIC         FROM `p-cdh`.landed.newsphone_basic_data
# MAGIC     )
# MAGIC     AND npa.UniqueCode = npbr.UniqueCode
# MAGIC     AND npa.file_date = npbr.FileDate
# MAGIC )
# MAGIC AND npbr.FileName <> 'BasicData_InitialFile'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with q as (
# MAGIC
# MAGIC SELECT     
# MAGIC     npbr.UniqueCode,
# MAGIC     npbr.Status,
# MAGIC     npbr.Court_Type,
# MAGIC     npbr.ConductionDate,
# MAGIC     npbr.PublishDate,
# MAGIC     npbr.InitialPrice,
# MAGIC     npbr.Type,
# MAGIC     npbr.DebtorVatNumber,
# MAGIC     npbr.DebtorDescr,
# MAGIC     TRY_CAST(npbr.MultipleDeptors AS BOOLEAN) AS MultipleDeptors,
# MAGIC     npbr.HastenerDescr,
# MAGIC     npbr.Servicer,
# MAGIC     npbr.HastenerVat,
# MAGIC     npbr.HastenerCountry,
# MAGIC     npbr.RelatedAuctionCode,
# MAGIC     TRY_CAST(npbr.HasJudgment AS BOOLEAN) AS HasJudgment,
# MAGIC     npbr.FileDate AS file_date,
# MAGIC     CONCAT('BasicData_', REPLACE(npbr.FileDate, '-', ''), '.csv') AS file_name,
# MAGIC     CURRENT_DATE - 1 AS BusinessDate,  
# MAGIC     CURRENT_TIMESTAMP AS LoadingDateTime
# MAGIC FROM `p-de-sandbox`.a360.newsphone_basicdata_reload_final npbr
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM `p-cdh`.landed.newsphone_basic_data npa
# MAGIC     WHERE npa.BusinessDate = (
# MAGIC         SELECT MAX(BusinessDate)
# MAGIC         FROM `p-cdh`.landed.newsphone_basic_data
# MAGIC     )
# MAGIC     AND npa.UniqueCode = npbr.UniqueCode
# MAGIC     AND npa.file_date = npbr.FileDate
# MAGIC )
# MAGIC
# MAGIC     )
# MAGIC     select count(*) from q
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-de-sandbox`.a360.newsphone_basicdata_reload_final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `p-cdh`.landed.newsphone_basic_data  (
# MAGIC     UniqueCode,
# MAGIC     Status,
# MAGIC     Court_Type,
# MAGIC     ConductionDate,
# MAGIC     PublishDate,
# MAGIC     InitialPrice,
# MAGIC     Type,
# MAGIC     DebtorVatNumber,
# MAGIC     DebtorDescr,
# MAGIC     MultipleDeptors,
# MAGIC     HastenerDescr,
# MAGIC     Servicer,
# MAGIC     HastenerVat,
# MAGIC     HastenerCountry,
# MAGIC     RelatedAuctionCode,
# MAGIC     HasJudgment,
# MAGIC     file_date,
# MAGIC     file_name,
# MAGIC     BusinessDate,
# MAGIC     LoadingDateTime
# MAGIC )
# MAGIC SELECT     
# MAGIC     npbr.UniqueCode,
# MAGIC     npbr.Status,
# MAGIC     npbr.Court_Type,
# MAGIC     npbr.ConductionDate,
# MAGIC     npbr.PublishDate,
# MAGIC     npbr.InitialPrice,
# MAGIC     npbr.Type,
# MAGIC     npbr.DebtorVatNumber,
# MAGIC     npbr.DebtorDescr,
# MAGIC     try_cast(npbr.MultipleDeptors AS BOOLEAN),
# MAGIC     npbr.HastenerDescr,
# MAGIC     npbr.Servicer,
# MAGIC     npbr.HastenerVat,
# MAGIC     npbr.HastenerCountry,
# MAGIC     npbr.RelatedAuctionCode,
# MAGIC     try_cast(npbr.HasJudgment AS BOOLEAN),
# MAGIC     npbr.FileDate AS file_date,
# MAGIC     CONCAT('BasicData_', REPLACE(npbr.FileDate, '-', ''), '.csv') AS file_name,
# MAGIC     CURRENT_DATE - 1 AS BusinessDate,  
# MAGIC     CURRENT_TIMESTAMP AS LoadingDateTime
# MAGIC FROM `p-de-sandbox`.a360.newsphone_basicdata_reload_final npbr
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM `p-cdh`.landed.newsphone_basic_data npa
# MAGIC     WHERE npa.UniqueCode = npbr.UniqueCode
# MAGIC );
