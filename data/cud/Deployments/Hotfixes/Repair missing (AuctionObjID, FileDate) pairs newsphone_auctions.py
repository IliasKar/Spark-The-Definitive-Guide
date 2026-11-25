# Databricks notebook source
# MAGIC %md
# MAGIC # Repair missing (AuctionObjID, FileDate) pairs
# MAGIC newsphone_auctions
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with q as (
# MAGIC SELECT 
# MAGIC    TRY_CAST(src.AuctionObjID AS INT) AS AuctionObjID,
# MAGIC    TRY_CAST(src.GroupObjID AS INT) AS GroupObjID,
# MAGIC    TRY_CAST(src.AuctionID AS STRING) AS AuctionID,
# MAGIC    src.Source,
# MAGIC    src.UniqueCode,
# MAGIC    TRY_CAST(src.AuctionDate AS DATE) AS AuctionDate,
# MAGIC    TRY_CAST(src.PublishedDate AS DATE) AS PublishedDate,
# MAGIC    TRY_CAST(src.eAuctionPostedDate AS DATE) AS eAuctionPostedDate,
# MAGIC    src.FirstOfferPrice,
# MAGIC    src.TitleGR,
# MAGIC    src.Area,
# MAGIC    src.District,
# MAGIC    src.Municipality,
# MAGIC    src.Prefecture,
# MAGIC    src.GeoCoordinatesX,
# MAGIC    src.GeoCoordinatesY,
# MAGIC    src.GeoLevel,
# MAGIC    src.Address,
# MAGIC    TRY_CAST(src.PostCode AS INT) AS PostCode,
# MAGIC    src.CategoryGR,
# MAGIC    src.SubCategoryGR,
# MAGIC    src.LandeaUrl,
# MAGIC    TRY_CAST(src.FloorRawValue AS INT) AS FloorRawValue,
# MAGIC    TRY_CAST(src.ConstructionYear AS INT) AS ConstructionYear,
# MAGIC    TRY_CAST(src.SurfaceSqM AS DOUBLE) AS SurfaceSqM,
# MAGIC    src.Tags,
# MAGIC    src.TagValues,
# MAGIC    TRY_CAST(src.Active AS BOOLEAN) AS Active,
# MAGIC    TRY_CAST(src.TNMergeID AS INT) AS TNMergeID,
# MAGIC    src.HastenerDescr,
# MAGIC    src.HastenerVat,
# MAGIC    src.DebtorDescr,
# MAGIC    src.DebtorVatNumber,
# MAGIC    src.AuctionStatusDescr,
# MAGIC    TRY_CAST(src.FinalPrice AS DOUBLE) AS FinalPrice,
# MAGIC    src.AuctioneerName,
# MAGIC    src.AuctioneerEmail,
# MAGIC    src.AuctioneerPhone,
# MAGIC    src.SeizureReportNumber,
# MAGIC    TRY_CAST(src.SeizureReportYear AS INT) AS SeizureReportYear,
# MAGIC    src.KAEK,
# MAGIC    src.FileDate AS file_date,
# MAGIC    CONCAT('auctions_', REPLACE(COALESCE(src.FileDate, src.FileDate), '-', ''), '.csv') AS file_name,
# MAGIC    CURRENT_DATE - 1 AS BusinessDate,
# MAGIC    CURRENT_TIMESTAMP AS LoadingDateTime,
# MAGIC    CAST(NULL AS STRING) AS _corrupt_record 
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final src
# MAGIC WHERE src.FileName <> 'auctions_InitialFile'
# MAGIC AND NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions tgt
# MAGIC   WHERE tgt.BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   )
# MAGIC   AND tgt.AuctionObjID = src.AuctionObjID
# MAGIC   AND tgt.file_date = src.FileDate
# MAGIC   )
# MAGIC )
# MAGIC select * from q
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,max file_date per AuctionObjID
# MAGIC %sql
# MAGIC with q as (
# MAGIC SELECT 
# MAGIC    TRY_CAST(src.AuctionObjID AS INT) AS AuctionObjID,
# MAGIC    TRY_CAST(src.GroupObjID AS INT) AS GroupObjID,
# MAGIC    TRY_CAST(src.AuctionID AS STRING) AS AuctionID,
# MAGIC    src.Source,
# MAGIC    src.UniqueCode,
# MAGIC    TRY_CAST(src.AuctionDate AS DATE) AS AuctionDate,
# MAGIC    TRY_CAST(src.PublishedDate AS DATE) AS PublishedDate,
# MAGIC    TRY_CAST(src.eAuctionPostedDate AS DATE) AS eAuctionPostedDate,
# MAGIC    src.FirstOfferPrice,
# MAGIC    src.TitleGR,
# MAGIC    src.Area,
# MAGIC    src.District,
# MAGIC    src.Municipality,
# MAGIC    src.Prefecture,
# MAGIC    src.GeoCoordinatesX,
# MAGIC    src.GeoCoordinatesY,
# MAGIC    src.GeoLevel,
# MAGIC    src.Address,
# MAGIC    TRY_CAST(src.PostCode AS INT) AS PostCode,
# MAGIC    src.CategoryGR,
# MAGIC    src.SubCategoryGR,
# MAGIC    src.LandeaUrl,
# MAGIC    TRY_CAST(src.FloorRawValue AS INT) AS FloorRawValue,
# MAGIC    TRY_CAST(src.ConstructionYear AS INT) AS ConstructionYear,
# MAGIC    TRY_CAST(src.SurfaceSqM AS DOUBLE) AS SurfaceSqM,
# MAGIC    src.Tags,
# MAGIC    src.TagValues,
# MAGIC    TRY_CAST(src.Active AS BOOLEAN) AS Active,
# MAGIC    TRY_CAST(src.TNMergeID AS INT) AS TNMergeID,
# MAGIC    src.HastenerDescr,
# MAGIC    src.HastenerVat,
# MAGIC    src.DebtorDescr,
# MAGIC    src.DebtorVatNumber,
# MAGIC    src.AuctionStatusDescr,
# MAGIC    TRY_CAST(src.FinalPrice AS DOUBLE) AS FinalPrice,
# MAGIC    src.AuctioneerName,
# MAGIC    src.AuctioneerEmail,
# MAGIC    src.AuctioneerPhone,
# MAGIC    src.SeizureReportNumber,
# MAGIC    TRY_CAST(src.SeizureReportYear AS INT) AS SeizureReportYear,
# MAGIC    src.KAEK,
# MAGIC    src.FileDate AS file_date,
# MAGIC    CONCAT('auctions_', REPLACE(COALESCE(src.FileDate, src.FileDate), '-', ''), '.csv') AS file_name,
# MAGIC    CURRENT_DATE - 1 AS BusinessDate,
# MAGIC    CURRENT_TIMESTAMP AS LoadingDateTime,
# MAGIC    CAST(NULL AS STRING) AS _corrupt_record 
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final src
# MAGIC WHERE src.FileName <> 'auctions_InitialFile'
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY src.AuctionObjID ORDER BY src.FileDate DESC) = 1
# MAGIC AND NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions tgt
# MAGIC   WHERE tgt.BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   )
# MAGIC   AND tgt.AuctionObjID = src.AuctionObjID
# MAGIC   AND tgt.file_date = src.FileDate
# MAGIC   )
# MAGIC )
# MAGIC select * from q
# MAGIC where file_date = '2025-03-31'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -------RUN THIS IN PROD--------

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO `p-cdh`.landed.newsphone_auctions( 
# MAGIC     AuctionObJID,
# MAGIC     GroupObjID,
# MAGIC     AuctionID,
# MAGIC     Source,
# MAGIC     UniqueCode,
# MAGIC     AuctionDate,
# MAGIC     PublishedDate,
# MAGIC     eAuctionPostedDate,
# MAGIC     FirstOfferPrice,
# MAGIC     TitleGR,
# MAGIC     Area,
# MAGIC     District,
# MAGIC     Municipality,
# MAGIC     Prefecture,
# MAGIC     GeoCoordinatesX,
# MAGIC     GeoCoordinatesY,
# MAGIC     GeoLevel,
# MAGIC     Address,
# MAGIC     PostCode,
# MAGIC     CategoryGR,
# MAGIC     SubCategoryGR,
# MAGIC     LandeaUrl,
# MAGIC     FloorRawValue,
# MAGIC     ConstructionYear,
# MAGIC     SurfaceSqM,
# MAGIC     Tags,
# MAGIC     TagValues,
# MAGIC     Active,
# MAGIC     TNMergeID,
# MAGIC     HastenerDescr,
# MAGIC     HastenerVat,
# MAGIC     DebtorDescr,
# MAGIC     DebtorVatNumber,
# MAGIC     AuctionStatusDescr,
# MAGIC     FinalPrice,
# MAGIC     AuctioneerName,
# MAGIC     AuctioneerEmail,
# MAGIC     AuctioneerPhone,
# MAGIC     SeizureReportNumber,
# MAGIC     SeizureReportYear,
# MAGIC     KAEK,
# MAGIC     file_date,
# MAGIC     file_name,
# MAGIC     BusinessDate,
# MAGIC     LoadingDateTime,
# MAGIC     _corrupt_record
# MAGIC       )  
# MAGIC SELECT 
# MAGIC    TRY_CAST(src.AuctionObjID AS INT) AS AuctionObjID,
# MAGIC    TRY_CAST(src.GroupObjID AS INT) AS GroupObjID,
# MAGIC    src.AuctionID,
# MAGIC    src.Source,
# MAGIC    src.UniqueCode,
# MAGIC    TRY_CAST(src.AuctionDate AS DATE) AS AuctionDate,
# MAGIC    TRY_CAST(src.PublishedDate AS DATE) AS PublishedDate,
# MAGIC    TRY_CAST(src.eAuctionPostedDate AS DATE) AS eAuctionPostedDate,
# MAGIC    src.FirstOfferPrice,
# MAGIC    src.TitleGR,
# MAGIC    src.Area,
# MAGIC    src.District,
# MAGIC    src.Municipality,
# MAGIC    src.Prefecture,
# MAGIC    src.GeoCoordinatesX,
# MAGIC    src.GeoCoordinatesY,
# MAGIC    src.GeoLevel,
# MAGIC    src.Address,
# MAGIC    TRY_CAST(src.PostCode AS INT) AS PostCode,
# MAGIC    src.CategoryGR,
# MAGIC    src.SubCategoryGR,
# MAGIC    src.LandeaUrl,
# MAGIC    TRY_CAST(src.FloorRawValue AS INT) AS FloorRawValue,
# MAGIC    TRY_CAST(src.ConstructionYear AS INT) AS ConstructionYear,
# MAGIC    TRY_CAST(src.SurfaceSqM AS DOUBLE) AS SurfaceSqM,
# MAGIC    src.Tags,
# MAGIC    src.TagValues,
# MAGIC    TRY_CAST(src.Active AS BOOLEAN) AS Active,
# MAGIC    TRY_CAST(src.TNMergeID AS INT) AS TNMergeID,
# MAGIC    src.HastenerDescr,
# MAGIC    src.HastenerVat,
# MAGIC    src.DebtorDescr,
# MAGIC    src.DebtorVatNumber,
# MAGIC    src.AuctionStatusDescr,
# MAGIC    TRY_CAST(src.FinalPrice AS DOUBLE) AS FinalPrice,
# MAGIC    src.AuctioneerName,
# MAGIC    src.AuctioneerEmail,
# MAGIC    src.AuctioneerPhone,
# MAGIC    src.SeizureReportNumber,
# MAGIC    TRY_CAST(src.SeizureReportYear AS INT) AS SeizureReportYear,
# MAGIC    src.KAEK,
# MAGIC    src.FileDate AS file_date,
# MAGIC    CONCAT('auctions_', REPLACE(COALESCE(src.FileDate, src.FileDate), '-', ''), '.csv') AS file_name,
# MAGIC    CURRENT_DATE -1 AS BusinessDate, --put the max Businessdate from `p-cdh`.landed.newsphone_auctions
# MAGIC    CURRENT_TIMESTAMP AS LoadingDateTime,
# MAGIC    CAST(NULL AS STRING) AS _corrupt_record 
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final src
# MAGIC WHERE src.FileName <> 'auctions_InitialFile'
# MAGIC AND NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions tgt
# MAGIC   WHERE tgt.BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   )
# MAGIC   AND tgt.AuctionObjID = src.AuctionObjID
# MAGIC   AND tgt.file_date = src.FileDate
# MAGIC   )
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -------RUN THIS IN PROD--------

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT 
# MAGIC    TRY_CAST(src.AuctionObjID AS INT) AS AuctionObjID,
# MAGIC    TRY_CAST(src.GroupObjID AS INT) AS GroupObjID,
# MAGIC    src.AuctionID,
# MAGIC    src.Source,
# MAGIC    src.UniqueCode,
# MAGIC    TRY_CAST(src.AuctionDate AS DATE) AS AuctionDate,
# MAGIC    TRY_CAST(src.PublishedDate AS DATE) AS PublishedDate,
# MAGIC    TRY_CAST(src.eAuctionPostedDate AS DATE) AS eAuctionPostedDate,
# MAGIC    src.FirstOfferPrice,
# MAGIC    src.TitleGR,
# MAGIC    src.Area,
# MAGIC    src.District,
# MAGIC    src.Municipality,
# MAGIC    src.Prefecture,
# MAGIC    src.GeoCoordinatesX,
# MAGIC    src.GeoCoordinatesY,
# MAGIC    src.GeoLevel,
# MAGIC    src.Address,
# MAGIC    TRY_CAST(src.PostCode AS INT) AS PostCode,
# MAGIC    src.CategoryGR,
# MAGIC    src.SubCategoryGR,
# MAGIC    src.LandeaUrl,
# MAGIC    TRY_CAST(src.FloorRawValue AS INT) AS FloorRawValue,
# MAGIC    TRY_CAST(src.ConstructionYear AS INT) AS ConstructionYear,
# MAGIC    TRY_CAST(src.SurfaceSqM AS DOUBLE) AS SurfaceSqM,
# MAGIC    src.Tags,
# MAGIC    src.TagValues,
# MAGIC    TRY_CAST(src.Active AS BOOLEAN) AS Active,
# MAGIC    TRY_CAST(src.TNMergeID AS INT) AS TNMergeID,
# MAGIC    src.HastenerDescr,
# MAGIC    src.HastenerVat,
# MAGIC    src.DebtorDescr,
# MAGIC    src.DebtorVatNumber,
# MAGIC    src.AuctionStatusDescr,
# MAGIC    TRY_CAST(src.FinalPrice AS DOUBLE) AS FinalPrice,
# MAGIC    src.AuctioneerName,
# MAGIC    src.AuctioneerEmail,
# MAGIC    src.AuctioneerPhone,
# MAGIC    src.SeizureReportNumber,
# MAGIC    TRY_CAST(src.SeizureReportYear AS INT) AS SeizureReportYear,
# MAGIC    src.KAEK,
# MAGIC    src.FileDate AS file_date,
# MAGIC    CONCAT('auctions_', REPLACE(COALESCE(src.FileDate, src.FileDate), '-', ''), '.csv') AS file_name,
# MAGIC    CURRENT_DATE -1 AS BusinessDate, --put the max Businessdate from `p-cdh`.landed.newsphone_auctions
# MAGIC    CURRENT_TIMESTAMP AS LoadingDateTime,
# MAGIC    CAST(NULL AS STRING) AS _corrupt_record 
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final src
# MAGIC WHERE src.FileName <> 'auctions_InitialFile'
# MAGIC AND NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions tgt
# MAGIC   WHERE tgt.BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   )
# MAGIC   AND tgt.AuctionObjID = src.AuctionObjID
# MAGIC   AND tgt.file_date = src.FileDate
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-cdh`.landed.newsphone_auctions
# MAGIC  WHERE AuctionObJID = 316899
# MAGIC  order by BusinessDate desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from `p-cdh`.landed.newsphone_auctions
# MAGIC  --WHERE BusinessDate = (
# MAGIC   -- SELECT MAX(BusinessDate)
# MAGIC   --FROM `p-cdh`.landed.newsphone_auctions  )
# MAGIC where file_date = '2024-07-17' --and GroupObjID = 396507

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `p-cdh`.landed.newsphone_auctions
# MAGIC where file_date = '2024-07-17'
# MAGIC
# MAGIC --where AuctionObJID=483061 and file_date is not null
# MAGIC --group by businessdate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT file_date, COUNT(DISTINCT AuctionObJID) as distinct_count
# MAGIC FROM `p-cdh`.landing.newsphone_auctions
# MAGIC  GROUP BY file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT FileDate, COUNT(DISTINCT AuctionObJID) as distinct_count
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC GROUP BY FileDate

# COMMAND ----------

from pyspark.sql.functions import col 

query = """
SELECT 
    a.FileDate, 
    a.distinct_count as distinct_count_reload_final, 
    b.distinct_count as distinct_count_auctions
FROM (
    SELECT FileDate, COUNT(DISTINCT AuctionObJID) as distinct_count
    FROM `p-de-sandbox`.a360.newsphone_reload_final
    GROUP BY FileDate
) a
JOIN (
    SELECT file_date, COUNT(DISTINCT AuctionObJID) as distinct_count
    FROM `p-cdh`.landing.newsphone_auctions
    GROUP BY file_date
) b
ON a.FileDate = b.file_date
"""

result_df = spark.sql(query)
display(result_df.filter(
    col("distinct_count_reload_final") != col("distinct_count_auctions")
))

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC where FileDate = '2024-07-17'
# MAGIC --where AuctionObjID = 455706

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   AuctionObjID,
# MAGIC   ARRAY_AGG(DISTINCT FileDate) AS FileDates
# MAGIC FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC GROUP BY AuctionObjID
# MAGIC ORDER BY AuctionObjID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with missing as (
# MAGIC
# MAGIC WITH latest_auctions AS (
# MAGIC   SELECT *
# MAGIC   FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   WHERE BusinessDate = (
# MAGIC     SELECT MAX(BusinessDate)
# MAGIC     FROM `p-cdh`.landed.newsphone_auctions
# MAGIC   )
# MAGIC ),
# MAGIC source_counts AS (
# MAGIC   SELECT
# MAGIC     AuctionObjID,
# MAGIC     COUNT(DISTINCT FileDate) AS source_filedate_count
# MAGIC   FROM `p-de-sandbox`.a360.newsphone_reload_final
# MAGIC   GROUP BY AuctionObjID
# MAGIC ),
# MAGIC target_counts AS (
# MAGIC   SELECT
# MAGIC     AuctionObjID,
# MAGIC     COUNT(DISTINCT file_date) AS target_filedate_count
# MAGIC   FROM latest_auctions
# MAGIC   GROUP BY AuctionObjID
# MAGIC )
# MAGIC SELECT
# MAGIC   s.AuctionObjID,
# MAGIC   s.source_filedate_count,
# MAGIC   COALESCE(t.target_filedate_count, 0) AS target_filedate_count
# MAGIC FROM source_counts s
# MAGIC LEFT JOIN target_counts t
# MAGIC   ON s.AuctionObjID = t.AuctionObjID
# MAGIC WHERE COALESCE(t.target_filedate_count, 0) < s.source_filedate_count
# MAGIC )
# MAGIC select * from missing
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   BusinessDate,
# MAGIC   COUNT(DISTINCT AuctionObjID) AS distinct_auction_count,
# MAGIC   COUNT(DISTINCT file_date) AS distinct_file_date_count
# MAGIC FROM `p-cdh`.landed.newsphone_auctions
# MAGIC GROUP BY BusinessDate
# MAGIC ORDER BY BusinessDate;
# MAGIC
# MAGIC
