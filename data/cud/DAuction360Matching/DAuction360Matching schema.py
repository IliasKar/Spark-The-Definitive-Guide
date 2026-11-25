# Databricks notebook source
# DBTITLE 1,Environment Vars
import boto3
import os

#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
env_prefix = os.environ["S3PREFIX"] 

# COMMAND ----------

### AuctionMatching360  table ###

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, BooleanType, TimestampType, DateType, FloatType, DoubleType

schema = StructType([
    StructField("AuctionMatching360ID", StringType(), False),
    StructField("IngestionTimestamp", TimestampType(), False),
    StructField("IncomingAuctionKey", StringType(), False),
    StructField("IncomingOriginAuctionKey", StringType(), False),
    StructField("IncomingRealEstateKey", StringType(), True),
    StructField("IncomingRealEstateGroupCode", IntegerType(), True),
    StructField("IncomingRealEstateGroupCodeStatus", BooleanType(), True),
    StructField("IncomingOwnerTaxRegistrationNumber", StringType(), True),
    StructField("IncomingOriginOwnerTaxRegistrationNumber", StringType(), True),
    StructField("IncomingOwnerDescription", StringType(), True),
    StructField("IncomingAuctionObjKey", StringType(), True),
    StructField("IncomingisLastAuction", StringType(), True),
    StructField("IncomingRealEstatePostalCode", IntegerType(), True),
    StructField("IncomingRealEstateAddress", StringType(), True),
    StructField("IncomingRealEstateSurfaceSqm", FloatType(), True),
    StructField("IncomingRealEstateFloorRawValue", StringType(), True),
    StructField("IncomingRealEstateTitle", StringType(), True),
    StructField("IncomingKAEK", StringType(), True),
    StructField("IncomingRealEstateCategory", StringType(), True),
    StructField("IncomingRealEstateSubCategory", StringType(), True),
    StructField("IncomingRealEstateMunicipality", StringType(), True),
    StructField("IncomingRealEstateArea", StringType(), True),
    StructField("IncomingRealEstateTags", StringType(), True),
    StructField("IncomingRealEstateTagValues", StringType(), True),
    StructField("IncomingSeizureReportNumber", StringType(), True),
    StructField("IncomingAuctionDate", DateType(), True),
    StructField("IncomingAuctionStatus", StringType(), True),
    StructField("IncomingInitiator", StringType(), True),
    StructField("IncomingHastener", StringType(), True),
    StructField("IncomingHastenerDescr", StringType(), True),
    StructField("IncomingPortfolioCategory", StringType(), True),
    StructField("IncomingIsHastenerServicer", StringType(), True),
    StructField("IncomingInitiatorCepalAB", StringType(), True),
    StructField("IncomingOwnerTaxRegistrationNumberIsCustomer", StringType(), True),
    StructField("SourceSystem", StringType(), True),
    StructField("SourceKAEK", StringType(), True),
    StructField("SourceRealEstateKey", StringType(), True),
    StructField("SourceOwnerTaxRegistrationNumber", StringType(), True),
    StructField("SourceOwnerLastName", StringType(), True),
    StructField("SourceOwnerFirstName", StringType(), True),
    StructField("SourceOwnershipTypeCode", StringType(), True),
    StructField("SourceOwnershipPercentage", DoubleType(), True),
    StructField("SourceRealEstatePostalCode", StringType(), True),
    StructField("SourceRealEstateAddress1", StringType(), True),
    StructField("SourceRealEstateAddress2", StringType(), True),
    StructField("SourceRealEstateNetArea", DoubleType(), True),
    StructField("SourceRealEstateBuildingNumber", StringType(), True),
    StructField("SourceRealEstateBuildingFloorRawValue", DoubleType(), True),
    StructField("SourceRealEstateDescription", StringType(), True),
    StructField("SourceRealEstateCategoryCode", StringType(), True),
    StructField("SourceRealEstateSubCategoryCode", StringType(), True),
    StructField("SourceRealEstateUsageCodeType", StringType(), True),
    StructField("SourceRealEstateMunicipality", StringType(), True),
    StructField("SourceRealEstateCity", StringType(), True),
    StructField("MatchingPercentage", FloatType(), True),
    StructField("MatchingAlgorithmVersion", DoubleType(), True),
    StructField("MatchingAlgorithmVersionDate", TimestampType(), True),
    StructField("MatchingMissingFlag", BooleanType(), False),
    StructField("UIAssignee", StringType(), True),
    StructField("UIApprovalFlag", BooleanType(), True),
    StructField("UIApprovalDate", TimestampType(), True),
    StructField("LastUpdatedBy", StringType(), False),
    StructField("LastUpdatedDate", TimestampType(), False),
    StructField("UIMatchingAuctionStatus", StringType(), False),
    StructField("Comment", StringType(), True)
])

# COMMAND ----------

#Create the AuctionMatching360 table on Curated zone

#Create an empty DataFrame with the specified schema
empty_data = []
df = spark.createDataFrame(empty_data, schema)

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"`{env_prefix}-cdh`.`derivativeactive`.`dauction360matching`")