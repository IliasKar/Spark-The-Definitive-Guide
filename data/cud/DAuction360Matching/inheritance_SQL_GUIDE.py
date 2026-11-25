# Databricks notebook source
# MAGIC %md
# MAGIC ## LOCK THE RECENT GUIDES FOR INHERITANCE
# MAGIC
# MAGIC The purpose of this query is to find all the recent guides where the UI has updated records and lock them up for inheritance.
# MAGIC
# MAGIC The query consists of the following steps:
# MAGIC
# MAGIC 1. The `max_tmp` CTE (Common Table Expression) is used to select all the records from the `dauction360matching` table where the `LastUpdatedBy` column is 'A360user'. It also calculates a modified version of the `AuctionMatching360ID` column by removing the `SourceRealEstateKey` from the end of the string to generate the id of the guide row.
# MAGIC
# MAGIC 2. The `ranked_tmp` CTE is used to assign a row number to each record within each `IncomingRealEstateGroupCode` group, based on the descending order of the `IngestionTimestamp` column.
# MAGIC
# MAGIC 3. The `guides_to_update` CTE is used to select the guides that need to be updated. It joins the `ranked_tmp` CTE with a subquery that selects the distinct records from the `dauction360matching` table where the `SourceRealEstateKey` is NULL. It also groups the records by `AuctionMatching360ID` and selects the record with the maximum `ingestiontimestamp`. The resulting records are then joined with the `guides` table on the `AuctionMatching360ID` column. Finally, it selects the records where the row number is 1 and orders them by the `IngestionTimestamp` column in descending order.
# MAGIC
# MAGIC 4. The final step is to insert the selected records from the `guides_to_update` CTE into the `dauction360matching` table. The columns to be inserted are listed explicitly, and some columns are assigned specific values, such as the current timestamp for `IngestionTimestamp` and 'locked' for `UIMatchingAuctionStatus`. The `UIAssignee`, `UIApprovalFlag`, `UIApprovalDate`, `LastUpdatedBy`, and `LastUpdatedDate` columns are also assigned specific values. The `Comment` column is copied from the `guides_to_update` CTE.
# MAGIC
# MAGIC After this cell you are ready to run the inheritance Query in order to update the rest of the guides of the the same thread and unlock the locked ones to enable the Front end form for the users to continue editing. The cell to run the inheritance is the cell # 4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- find all the recent guides where the UI has updated records and lock them up for inheritance
# MAGIC WITH max_tmp AS (
# MAGIC     SELECT
# MAGIC         a.*,
# MAGIC         SUBSTRING(a.AuctionMatching360ID, 1, LENGTH(a.AuctionMatching360ID) - LENGTH(a.SourceRealEstateKey)) AS modified_AuctionMatching360ID
# MAGIC     FROM `p-cdh`.derivativeactive.dauction360matching a
# MAGIC     JOIN (
# MAGIC         SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
# MAGIC         FROM `p-cdh`.derivativeactive.dauction360matching
# MAGIC         GROUP BY AuctionMatching360ID
# MAGIC     ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC        AND a.ingestiontimestamp = b.max_timestamp
# MAGIC           WHERE a.LastUpdatedBy = 'A360user'
# MAGIC ),
# MAGIC ranked_tmp AS (
# MAGIC     SELECT
# MAGIC         max_tmp.*,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY max_tmp.IncomingRealEstateGroupCode ORDER BY max_tmp.IngestionTimestamp DESC) AS rn
# MAGIC     FROM max_tmp
# MAGIC     WHERE max_tmp.SourceRealEstateKey <> ''
# MAGIC ),
# MAGIC guides_to_update AS (
# MAGIC    SELECT guides.*
# MAGIC    FROM ranked_tmp
# MAGIC    JOIN (SELECT DISTINCT a.* FROM `p-cdh`.derivativeactive.dauction360matching a
# MAGIC       JOIN (
# MAGIC          SELECT AuctionMatching360ID, MAX(ingestiontimestamp) AS max_timestamp
# MAGIC          FROM `p-cdh`.derivativeactive.dauction360matching
# MAGIC          WHERE SourceRealEstateKey IS NULL
# MAGIC          GROUP BY AuctionMatching360ID
# MAGIC       ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC          AND a.ingestiontimestamp = b.max_timestamp) guides ON guides.AuctionMatching360ID = ranked_tmp.modified_AuctionMatching360ID
# MAGIC    WHERE rn = 1
# MAGIC    ORDER BY ranked_tmp.IngestionTimestamp DESC 
# MAGIC )
# MAGIC insert into `p-cdh`.derivativeactive.dauction360matching
# MAGIC select    
# MAGIC AuctionMatching360ID, IncomingCalculatedAuctionObjKey,
# MAGIC             IncomingTNMergeCode,IncomingKAEK,
# MAGIC             IncomingOriginSourceSystem,IncomingAuctionObjKey,
# MAGIC             IncomingAuctionKey,IncomingOriginAuctionKey,
# MAGIC             IncomingRealEstateKey,IncomingRealEstateGroupCode,
# MAGIC             IncomingOwnerTaxRegistrationNumber,IncomingOriginOwnerTaxRegistrationNumber,
# MAGIC             IncomingOwnerDescription,IncomingRealEstatePostalCode,
# MAGIC             IncomingAuctionStatus,IncomingRealEstateAddress,
# MAGIC             IncomingRealEstateSurfaceSqm,IncomingRealEstateFloorRawValue,
# MAGIC             IncomingRealEstateTitle,IncomingRealEstateCategory,
# MAGIC             IncomingRealEstateSubCategory,IncomingRealEstateMunicipality,
# MAGIC             IncomingRealEstateArea,IncomingRealEstateTags,
# MAGIC             IncomingRealEstateTagValues,IncomingSeizureReportNumber,
# MAGIC             IncomingHastenerDescr,IncomingInitiator,
# MAGIC             IncomingHastener,IncomingInitiatorCepalAB,
# MAGIC             IncomingPortfolioCategory,IncomingIsHastenerServicer,
# MAGIC             IncomingAuctionDate,IncomingisLastAuction,
# MAGIC             IncomingOwnerTaxRegistrationNumberIsCustomer,IncomingRealEstateGroupCodeStatus,
# MAGIC             SourceSystem,SourceOwnerTaxRegistrationNumber,
# MAGIC             SourceOwnerFirstName,SourceOwnerLastName,
# MAGIC             SourceRealEstateKey,SourceRealEstateAddress1,
# MAGIC             SourceRealEstateAddress2,SourceRealEstateSubCategoryCode,
# MAGIC             SourceRealEstateCategoryCode,SourceRealEstateStatus,
# MAGIC             SourceRealEstateCity,SourceRealEstateDescription,
# MAGIC             SourceKAEK,SourceRealEstateLandArea,SourceRealEstateNetArea,
# MAGIC             SourceRealEstateMunicipality,SourceRealEstateActive,
# MAGIC             SourceRealEstateUsageCodeType,SourceRealEstatePostalCode,
# MAGIC 			SourceRealEstateBuildingFloorRawValue,
# MAGIC 			SourceRealEstateBuildingOtherAuxiliaryArea,
# MAGIC 			SourceRealEstateBuildingStorageArea,
# MAGIC             SourceRealEstateBuildingParkingArea,
# MAGIC 			SourceRealEstateBuildingNetArea,
# MAGIC 			SourceRealEstateBuildingNetArea_sum,
# MAGIC 			SourceAggregatedRealEstateBuildingFloorRawValue,
# MAGIC 			SourceRealEstateBuildingNumber,
# MAGIC             SourceOwnershipPercentage,SourceOwnershipTypeCode,
# MAGIC             MatchingPercentage,MatchingAlgorithmVersion,
# MAGIC             MatchingAlgorithmVersionDate,MatchingMissingFlag,
# MAGIC             current_timestamp() as IngestionTimestamp,
# MAGIC             'locked' as UIMatchingAuctionStatus,
# MAGIC             SourceRealEstateBuildingFloorRawValueDescription,
# MAGIC             UIAssignee,
# MAGIC             UIApprovalFlag,
# MAGIC             UIApprovalDate,
# MAGIC             'system' as LastUpdatedBy, 
# MAGIC             LastUpdatedDate,
# MAGIC             Comment   from guides_to_update
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## MAIN INHERITANCE SCRIPT
# MAGIC
# MAGIC The purpose of this cell is to perform the inheritance process on all locked guide threads and unlock the guides.
# MAGIC
# MAGIC The cell consists of the following steps:
# MAGIC
# MAGIC 1. The `max_tmp` CTE (Common Table Expression) is used to select the maximum `ingestiontimestamp` for each `AuctionMatching360ID` where the `IncomingRealEstateGroupCode` is not null and the `UIMatchingAuctionStatus` is not 'Deprecated'.
# MAGIC
# MAGIC 2. The `all_cands` CTE is used to select all the records from the `dauction360matching` table that meet the following conditions:
# MAGIC    - The `SourceRealEstateKey` is not null and not empty.
# MAGIC    - The `IncomingRealEstateGroupCode` is not null.
# MAGIC    - The `UIMatchingAuctionStatus` is not 'Deprecated'.
# MAGIC    - The `AuctionMatching360ID` is not present in the subquery result, which selects distinct `AuctionMatching360ID` values concatenated with `SourceRealEstateKey` from the `pc` table.
# MAGIC    - The `IncomingRealEstateGroupCode` is in the result of the subquery, which selects distinct `IncomingRealEstateGroupCode` values from the `guides` table.
# MAGIC
# MAGIC 3. The `candidates` CTE is used to select the records from the `all_cands` CTE and join them with the `pc` table on the `SourceSystem`, `IncomingRealEstateGroupCode`, and `SourceRealEstateKey` columns. It selects specific columns from both tables and assigns specific values to some columns. The resulting records are filtered based on the condition that the `UIMatchingAuctionStatus` is not null and the `IncomingRealEstateGroupCode` is in the result of the subquery, which selects distinct `IncomingRealEstateGroupCode` values from the `guides` table.
# MAGIC
# MAGIC 4. The `guides_formatted` CTE is used to select specific columns from the `guides` table and assign specific values to some columns. This CTE is used to include the original guides that were not found in the `pc` table.
# MAGIC
# MAGIC 5. The `final` CTE is used to combine the records from the `candidates` and `guides_formatted` CTEs using the UNION operator.
# MAGIC
# MAGIC 6. The final SELECT statement selects all columns from the `final` CTE.
# MAGIC
# MAGIC This cell performs the inheritance process by selecting the relevant records, updating specific columns, and inserting the updated records into the `dauction360matching` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH max_tmp AS (
# MAGIC     SELECT
# MAGIC         AuctionMatching360ID,
# MAGIC         MAX(ingestiontimestamp) AS max_timestamp
# MAGIC     FROM
# MAGIC         `p-cdh`.derivativeactive.dauction360matching
# MAGIC     WHERE
# MAGIC         IncomingRealEstateGroupCode IS NOT NULL
# MAGIC         AND UIMatchingAuctionStatus != 'Deprecated'
# MAGIC     GROUP BY
# MAGIC         AuctionMatching360ID
# MAGIC ),
# MAGIC guides AS (
# MAGIC     SELECT
# MAGIC         a.*
# MAGIC     FROM
# MAGIC         `p-cdh`.derivativeactive.dauction360matching a
# MAGIC     JOIN
# MAGIC         max_tmp b
# MAGIC     ON
# MAGIC         a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC         AND a.ingestiontimestamp = b.max_timestamp
# MAGIC     WHERE
# MAGIC         a.IncomingRealEstateGroupCode IS NOT NULL
# MAGIC         AND a.UIMatchingAuctionStatus = 'locked'
# MAGIC ),
# MAGIC all_records AS (
# MAGIC     SELECT
# MAGIC         a.*
# MAGIC     FROM
# MAGIC         `p-cdh`.derivativeactive.dauction360matching a
# MAGIC     JOIN
# MAGIC         max_tmp b
# MAGIC     ON
# MAGIC         a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC         AND a.ingestiontimestamp = b.max_timestamp
# MAGIC     WHERE
# MAGIC         a.SourceRealEstateKey IS NOT NULL
# MAGIC         AND a.SourceRealEstateKey <> ''
# MAGIC         AND a.IncomingRealEstateGroupCode IS NOT NULL
# MAGIC         AND a.UIMatchingAuctionStatus != 'Deprecated'
# MAGIC ),
# MAGIC pc AS (
# MAGIC     SELECT
# MAGIC         all_records.*, guides.AuctionMatching360ID as AuctionMatching360ID_g
# MAGIC     FROM
# MAGIC         all_records
# MAGIC     JOIN
# MAGIC         guides
# MAGIC     ON
# MAGIC         guides.IncomingRealEstateKey = all_records.IncomingRealEstateKey
# MAGIC         AND guides.IncomingOwnerTaxRegistrationNumber = all_records.IncomingOwnerTaxRegistrationNumber
# MAGIC     WHERE
# MAGIC         all_records.AuctionMatching360ID = CONCAT(guides.AuctionMatching360ID, all_records.SourceRealEstateKey)
# MAGIC ),
# MAGIC all_cands AS (
# MAGIC     SELECT
# MAGIC         a.*
# MAGIC     FROM
# MAGIC         `p-cdh`.derivativeactive.dauction360matching a
# MAGIC     JOIN
# MAGIC         max_tmp b
# MAGIC     ON
# MAGIC         a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC         AND a.ingestiontimestamp = b.max_timestamp
# MAGIC     WHERE
# MAGIC         a.SourceRealEstateKey IS NOT NULL
# MAGIC         AND a.SourceRealEstateKey <> ''
# MAGIC         AND a.IncomingRealEstateGroupCode IS NOT NULL
# MAGIC         AND a.UIMatchingAuctionStatus != 'Deprecated'
# MAGIC         AND a.AuctionMatching360ID NOT IN (
# MAGIC             SELECT DISTINCT CONCAT(pc.AuctionMatching360ID_g, pc.SourceRealEstateKey) FROM pc
# MAGIC         )
# MAGIC         AND a.IncomingRealEstateGroupCode IN (
# MAGIC             SELECT DISTINCT guides.IncomingRealEstateGroupCode FROM guides
# MAGIC         )
# MAGIC ),
# MAGIC candidates AS (
# MAGIC     SELECT
# MAGIC         oc.AuctionMatching360ID, oc.IncomingCalculatedAuctionObjKey,
# MAGIC         oc.IncomingTNMergeCode, oc.IncomingKAEK,
# MAGIC         oc.IncomingOriginSourceSystem, oc.IncomingAuctionObjKey,
# MAGIC         oc.IncomingAuctionKey, oc.IncomingOriginAuctionKey,
# MAGIC         oc.IncomingRealEstateKey, oc.IncomingRealEstateGroupCode,
# MAGIC         oc.IncomingOwnerTaxRegistrationNumber, oc.IncomingOriginOwnerTaxRegistrationNumber,
# MAGIC         oc.IncomingOwnerDescription, oc.IncomingRealEstatePostalCode,
# MAGIC         oc.IncomingAuctionStatus, oc.IncomingRealEstateAddress,
# MAGIC         oc.IncomingRealEstateSurfaceSqm, oc.IncomingRealEstateFloorRawValue,
# MAGIC         oc.IncomingRealEstateTitle, oc.IncomingRealEstateCategory,
# MAGIC         oc.IncomingRealEstateSubCategory, oc.IncomingRealEstateMunicipality,
# MAGIC         oc.IncomingRealEstateArea, oc.IncomingRealEstateTags,
# MAGIC         oc.IncomingRealEstateTagValues, oc.IncomingSeizureReportNumber,
# MAGIC         oc.IncomingHastenerDescr, oc.IncomingInitiator,
# MAGIC         oc.IncomingHastener, oc.IncomingInitiatorCepalAB,
# MAGIC         oc.IncomingPortfolioCategory, oc.IncomingIsHastenerServicer,
# MAGIC         oc.IncomingAuctionDate, oc.IncomingisLastAuction,
# MAGIC         oc.IncomingOwnerTaxRegistrationNumberIsCustomer, oc.IncomingRealEstateGroupCodeStatus,
# MAGIC         oc.SourceSystem, oc.SourceOwnerTaxRegistrationNumber,
# MAGIC         oc.SourceOwnerFirstName, oc.SourceOwnerLastName,
# MAGIC         oc.SourceRealEstateKey, oc.SourceRealEstateAddress1,
# MAGIC         oc.SourceRealEstateAddress2, oc.SourceRealEstateSubCategoryCode,
# MAGIC         oc.SourceRealEstateCategoryCode, oc.SourceRealEstateStatus,
# MAGIC         oc.SourceRealEstateCity, oc.SourceRealEstateDescription,
# MAGIC         oc.SourceKAEK, try_cast(oc.SourceRealEstateLandArea AS DOUBLE) AS SourceRealEstateLandArea, 
# MAGIC         try_cast(oc.SourceRealEstateNetArea AS DOUBLE) AS SourceRealEstateNetArea,
# MAGIC         oc.SourceRealEstateMunicipality, oc.SourceRealEstateActive,
# MAGIC         oc.SourceRealEstateUsageCodeType, oc.SourceRealEstatePostalCode,
# MAGIC         try_cast(oc.SourceRealEstateBuildingFloorRawValue AS DOUBLE) AS SourceRealEstateBuildingFloorRawValue,
# MAGIC         try_cast(oc.SourceRealEstateBuildingOtherAuxiliaryArea AS DOUBLE) AS SourceRealEstateBuildingOtherAuxiliaryArea,
# MAGIC         try_cast(oc.SourceRealEstateBuildingStorageArea AS DOUBLE) AS SourceRealEstateBuildingStorageArea,
# MAGIC         try_cast(oc.SourceRealEstateBuildingParkingArea AS DOUBLE) AS SourceRealEstateBuildingParkingArea,
# MAGIC         try_cast(oc.SourceRealEstateBuildingNetArea AS DOUBLE) AS SourceRealEstateBuildingNetArea,
# MAGIC         try_cast(oc.SourceRealEstateBuildingNetArea_sum AS DOUBLE) AS SourceRealEstateBuildingNetArea_sum,
# MAGIC         oc.SourceAggregatedRealEstateBuildingFloorRawValue, 
# MAGIC         oc.SourceRealEstateBuildingNumber,
# MAGIC         try_cast(oc.SourceOwnershipPercentage AS DOUBLE) AS SourceOwnershipPercentage,
# MAGIC         oc.SourceOwnershipTypeCode,
# MAGIC         oc.MatchingPercentage, oc.MatchingAlgorithmVersion,
# MAGIC         oc.MatchingAlgorithmVersionDate, oc.MatchingMissingFlag,
# MAGIC         current_timestamp() AS IngestionTimestamp,
# MAGIC         pc.UIMatchingAuctionStatus,
# MAGIC         oc.SourceRealEstateBuildingFloorRawValueDescription,
# MAGIC         pc.UIAssignee,
# MAGIC         pc.UIApprovalFlag,
# MAGIC         pc.UIApprovalDate,
# MAGIC         'A360user (system inheritance)' AS LastUpdatedBy, 
# MAGIC         pc.LastUpdatedDate,
# MAGIC         pc.Comment
# MAGIC     FROM
# MAGIC         all_cands oc
# MAGIC     LEFT JOIN 
# MAGIC         pc
# MAGIC     ON
# MAGIC         pc.SourceSystem = oc.SourceSystem
# MAGIC         AND oc.IncomingRealEstateGroupCode = pc.IncomingRealEstateGroupCode
# MAGIC         AND pc.SourceRealEstateKey = oc.SourceRealEstateKey
# MAGIC     WHERE
# MAGIC         pc.UIMatchingAuctionStatus IS NOT NULL 
# MAGIC         AND oc.IncomingRealEstateGroupCode IN (
# MAGIC             SELECT DISTINCT guides.IncomingRealEstateGroupCode FROM guides
# MAGIC         )
# MAGIC ), 
# MAGIC guides_formatted AS (
# MAGIC     SELECT
# MAGIC         AuctionMatching360ID, IncomingCalculatedAuctionObjKey,
# MAGIC         IncomingTNMergeCode, IncomingKAEK,
# MAGIC         IncomingOriginSourceSystem, IncomingAuctionObjKey,
# MAGIC         IncomingAuctionKey, IncomingOriginAuctionKey,
# MAGIC         IncomingRealEstateKey, IncomingRealEstateGroupCode,
# MAGIC         IncomingOwnerTaxRegistrationNumber, IncomingOriginOwnerTaxRegistrationNumber,
# MAGIC         IncomingOwnerDescription, IncomingRealEstatePostalCode,
# MAGIC         IncomingAuctionStatus, IncomingRealEstateAddress,
# MAGIC         IncomingRealEstateSurfaceSqm, IncomingRealEstateFloorRawValue,
# MAGIC         IncomingRealEstateTitle, IncomingRealEstateCategory,
# MAGIC         IncomingRealEstateSubCategory, IncomingRealEstateMunicipality,
# MAGIC         IncomingRealEstateArea, IncomingRealEstateTags,
# MAGIC         IncomingRealEstateTagValues, IncomingSeizureReportNumber,
# MAGIC         IncomingHastenerDescr, IncomingInitiator,
# MAGIC         IncomingHastener, IncomingInitiatorCepalAB,
# MAGIC         IncomingPortfolioCategory, IncomingIsHastenerServicer,
# MAGIC         IncomingAuctionDate, IncomingisLastAuction,
# MAGIC         IncomingOwnerTaxRegistrationNumberIsCustomer, IncomingRealEstateGroupCodeStatus,
# MAGIC         SourceSystem, SourceOwnerTaxRegistrationNumber,
# MAGIC         SourceOwnerFirstName, SourceOwnerLastName,
# MAGIC         SourceRealEstateKey, SourceRealEstateAddress1,
# MAGIC         SourceRealEstateAddress2, SourceRealEstateSubCategoryCode,
# MAGIC         SourceRealEstateCategoryCode, SourceRealEstateStatus,
# MAGIC         SourceRealEstateCity, SourceRealEstateDescription,
# MAGIC         SourceKAEK, try_cast(SourceRealEstateLandArea AS DOUBLE) AS SourceRealEstateLandArea,
# MAGIC         try_cast(SourceRealEstateNetArea AS DOUBLE) AS SourceRealEstateNetArea,
# MAGIC         SourceRealEstateMunicipality, SourceRealEstateActive,
# MAGIC         SourceRealEstateUsageCodeType, SourceRealEstatePostalCode,
# MAGIC         try_cast(SourceRealEstateBuildingFloorRawValue AS DOUBLE) AS SourceRealEstateBuildingFloorRawValue,
# MAGIC         try_cast(SourceRealEstateBuildingOtherAuxiliaryArea AS DOUBLE) AS SourceRealEstateBuildingOtherAuxiliaryArea,
# MAGIC         try_cast(SourceRealEstateBuildingStorageArea AS DOUBLE) AS SourceRealEstateBuildingStorageArea,
# MAGIC         try_cast(SourceRealEstateBuildingParkingArea AS DOUBLE) AS SourceRealEstateBuildingParkingArea,
# MAGIC         try_cast(SourceRealEstateBuildingNetArea AS DOUBLE) AS SourceRealEstateBuildingNetArea,
# MAGIC         try_cast(SourceRealEstateBuildingNetArea_sum AS DOUBLE) AS SourceRealEstateBuildingNetArea_sum,
# MAGIC         SourceAggregatedRealEstateBuildingFloorRawValue, 
# MAGIC         SourceRealEstateBuildingNumber,
# MAGIC         try_cast(SourceOwnershipPercentage AS DOUBLE) AS SourceOwnershipPercentage,
# MAGIC         SourceOwnershipTypeCode,
# MAGIC         MatchingPercentage, MatchingAlgorithmVersion,
# MAGIC         MatchingAlgorithmVersionDate, MatchingMissingFlag,
# MAGIC         current_timestamp() AS IngestionTimestamp,
# MAGIC         '' AS UIMatchingAuctionStatus,
# MAGIC         SourceRealEstateBuildingFloorRawValueDescription,
# MAGIC         UIAssignee,
# MAGIC         UIApprovalFlag,
# MAGIC         UIApprovalDate,
# MAGIC         'system' AS LastUpdatedBy, 
# MAGIC         LastUpdatedDate,
# MAGIC         Comment
# MAGIC     FROM guides
# MAGIC ),
# MAGIC final AS (
# MAGIC     SELECT * FROM candidates 
# MAGIC     UNION 
# MAGIC     SELECT * FROM guides_formatted
# MAGIC )
# MAGIC INSERT INTO `p-cdh`.derivativeactive.dauction360matching          
# MAGIC SELECT * FROM final;
