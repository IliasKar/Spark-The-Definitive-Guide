# Databricks notebook source
# DBTITLE 1,Add records to Archive
# MAGIC %sql
# MAGIC INSERT INTO `p-cdh`.derivative.dauction360matching_archive
# MAGIC SELECT *
# MAGIC FROM `p-cdh`.derivative.dauction360matching live
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM `p-cdh`.derivative.dauction360matching_archive archive
# MAGIC     WHERE 
# MAGIC         archive.AuctionMatching360ID = live.AuctionMatching360ID AND
# MAGIC         archive.IncomingCalculatedAuctionObjKey = live.IncomingCalculatedAuctionObjKey AND
# MAGIC         archive.IncomingTNMergeCode = live.IncomingTNMergeCode AND
# MAGIC         archive.IncomingKAEK = live.IncomingKAEK AND
# MAGIC         archive.IncomingOriginSourceSystem = live.IncomingOriginSourceSystem AND
# MAGIC         archive.IncomingAuctionObjKey = live.IncomingAuctionObjKey AND
# MAGIC         archive.IncomingAuctionKey = live.IncomingAuctionKey AND
# MAGIC         archive.IncomingOriginAuctionKey = live.IncomingOriginAuctionKey AND
# MAGIC         archive.IncomingRealEstateKey = live.IncomingRealEstateKey AND
# MAGIC         archive.IncomingRealEstateGroupCode = live.IncomingRealEstateGroupCode AND
# MAGIC         archive.IncomingOwnerTaxRegistrationNumber = live.IncomingOwnerTaxRegistrationNumber AND
# MAGIC         archive.IncomingOriginOwnerTaxRegistrationNumber = live.IncomingOriginOwnerTaxRegistrationNumber AND
# MAGIC         archive.IncomingOwnerDescription = live.IncomingOwnerDescription AND
# MAGIC         archive.IncomingRealEstatePostalCode = live.IncomingRealEstatePostalCode AND
# MAGIC         archive.IncomingAuctionStatus = live.IncomingAuctionStatus AND
# MAGIC         archive.IncomingRealEstateAddress = live.IncomingRealEstateAddress AND
# MAGIC         archive.IncomingRealEstateSurfaceSqm = live.IncomingRealEstateSurfaceSqm AND
# MAGIC         archive.IncomingRealEstateFloorRawValue = live.IncomingRealEstateFloorRawValue AND
# MAGIC         archive.IncomingRealEstateTitle = live.IncomingRealEstateTitle AND
# MAGIC         archive.IncomingRealEstateCategory = live.IncomingRealEstateCategory AND
# MAGIC         archive.IncomingRealEstateSubCategory = live.IncomingRealEstateSubCategory AND
# MAGIC         archive.IncomingRealEstateMunicipality = live.IncomingRealEstateMunicipality AND
# MAGIC         archive.IncomingRealEstateArea = live.IncomingRealEstateArea AND
# MAGIC         archive.IncomingRealEstateTags = live.IncomingRealEstateTags AND
# MAGIC         archive.IncomingRealEstateTagValues = live.IncomingRealEstateTagValues AND
# MAGIC         archive.IncomingSeizureReportNumber = live.IncomingSeizureReportNumber AND
# MAGIC         archive.IncomingHastenerDescr = live.IncomingHastenerDescr AND
# MAGIC         archive.IncomingInitiator = live.IncomingInitiator AND
# MAGIC         archive.IncomingHastener = live.IncomingHastener AND
# MAGIC         archive.IncomingInitiatorCepalAB = live.IncomingInitiatorCepalAB AND
# MAGIC         archive.IncomingPortfolioCategory = live.IncomingPortfolioCategory AND
# MAGIC         archive.IncomingIsHastenerServicer = live.IncomingIsHastenerServicer AND
# MAGIC         archive.IncomingAuctionDate = live.IncomingAuctionDate AND
# MAGIC         archive.IncomingisLastAuction = live.IncomingisLastAuction AND
# MAGIC         archive.IncomingOwnerTaxRegistrationNumberIsCustomer = live.IncomingOwnerTaxRegistrationNumberIsCustomer AND
# MAGIC         archive.IncomingRealEstateGroupCodeStatus = live.IncomingRealEstateGroupCodeStatus AND
# MAGIC         archive.SourceSystem = live.SourceSystem AND
# MAGIC         archive.SourceOwnerTaxRegistrationNumber = live.SourceOwnerTaxRegistrationNumber AND
# MAGIC         archive.SourceOwnerFirstName = live.SourceOwnerFirstName AND
# MAGIC         archive.SourceOwnerLastName = live.SourceOwnerLastName AND
# MAGIC         archive.SourceRealEstateKey = live.SourceRealEstateKey AND
# MAGIC         archive.SourceRealEstateAddress1 = live.SourceRealEstateAddress1 AND
# MAGIC         archive.SourceRealEstateAddress2 = live.SourceRealEstateAddress2 AND
# MAGIC         archive.SourceRealEstateSubCategoryCode = live.SourceRealEstateSubCategoryCode AND
# MAGIC         archive.SourceRealEstateCategoryCode = live.SourceRealEstateCategoryCode AND
# MAGIC         archive.SourceRealEstateStatus = live.SourceRealEstateStatus AND
# MAGIC         archive.SourceRealEstateCity = live.SourceRealEstateCity AND
# MAGIC         archive.SourceRealEstateDescription = live.SourceRealEstateDescription AND
# MAGIC         archive.SourceKAEK = live.SourceKAEK AND
# MAGIC         archive.SourceRealEstateNetArea = live.SourceRealEstateNetArea AND
# MAGIC         archive.SourceRealEstateMunicipality = live.SourceRealEstateMunicipality AND
# MAGIC         archive.SourceRealEstateActive = live.SourceRealEstateActive AND
# MAGIC         archive.SourceRealEstateUsageCodeType = live.SourceRealEstateUsageCodeType AND
# MAGIC         archive.SourceRealEstatePostalCode = live.SourceRealEstatePostalCode AND
# MAGIC         archive.SourceRealEstateBuildingFloorRawValue = live.SourceRealEstateBuildingFloorRawValue AND
# MAGIC         archive.SourceRealEstateBuildingOtherAuxiliaryArea = live.SourceRealEstateBuildingOtherAuxiliaryArea AND
# MAGIC         archive.SourceRealEstateBuildingStorageArea = live.SourceRealEstateBuildingStorageArea AND
# MAGIC         archive.SourceRealEstateBuildingParkingArea = live.SourceRealEstateBuildingParkingArea AND
# MAGIC         archive.SourceRealEstateBuildingNetArea = live.SourceRealEstateBuildingNetArea AND
# MAGIC         archive.SourceRealEstateBuildingNetArea_sum = live.SourceRealEstateBuildingNetArea_sum AND
# MAGIC         archive.SourceAggregatedRealEstateBuildingFloorRawValue = live.SourceAggregatedRealEstateBuildingFloorRawValue AND
# MAGIC         archive.SourceRealEstateBuildingNumber = live.SourceRealEstateBuildingNumber AND
# MAGIC         archive.SourceOwnershipPercentage = live.SourceOwnershipPercentage AND
# MAGIC         archive.SourceOwnershipTypeCode = live.SourceOwnershipTypeCode AND
# MAGIC         archive.MatchingPercentage = live.MatchingPercentage AND
# MAGIC         archive.MatchingAlgorithmVersion = live.MatchingAlgorithmVersion AND
# MAGIC         archive.MatchingAlgorithmVersionDate = live.MatchingAlgorithmVersionDate AND
# MAGIC         archive.MatchingMissingFlag = live.MatchingMissingFlag AND
# MAGIC         archive.IngestionTimestamp = live.IngestionTimestamp AND
# MAGIC         archive.UIMatchingAuctionStatus = live.UIMatchingAuctionStatus AND
# MAGIC         archive.UIAssignee = live.UIAssignee AND
# MAGIC         archive.UIApprovalFlag = live.UIApprovalFlag AND
# MAGIC         archive.UIApprovalDate = live.UIApprovalDate AND
# MAGIC         archive.LastUpdatedBy = live.LastUpdatedBy AND
# MAGIC         archive.LastUpdatedDate = live.LastUpdatedDate AND
# MAGIC         archive.Comment = live.Comment
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Recreate the table as its max timestamp
# MAGIC %sql
# MAGIC -- Step 1: Use a CTE to find the records with the max ingestion timestamp
# MAGIC WITH max_tmp AS ( 
# MAGIC     SELECT a.*
# MAGIC     FROM `p-cdh`.derivative.dauction360matching a
# MAGIC     JOIN (
# MAGIC         SELECT 
# MAGIC             AuctionMatching360ID, 
# MAGIC             MAX(ingestiontimestamp) AS max_timestamp
# MAGIC         FROM `p-cdh`.derivative.dauction360matching
# MAGIC         GROUP BY AuctionMatching360ID
# MAGIC     ) b ON a.AuctionMatching360ID = b.AuctionMatching360ID
# MAGIC        AND a.ingestiontimestamp = b.max_timestamp
# MAGIC )
# MAGIC
# MAGIC -- Step 2: Re-create the table with the filtered records
# MAGIC CREATE OR REPLACE TABLE `p-cdh`.derivative.dauction360matching AS
# MAGIC SELECT * 
# MAGIC FROM max_tmp;
