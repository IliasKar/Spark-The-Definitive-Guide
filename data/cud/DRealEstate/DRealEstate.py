# Databricks notebook source
import logging

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Import Libaries and Functions

# COMMAND ----------

import os

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create widgets with default values. Change them if needed from UI
# MAGIC * run_date : '2024-02-28'
# MAGIC * unity : True | False
# MAGIC
# MAGIC In case of Unity Catalog, the appropriate cluster should be attached

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-02-28')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"] 

# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()

run_date = dbutils.widgets.get("run_date")
unity = dbutils.widgets.get("unity")

table_name = 'DRealEstate'

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Initialize the schema retrieved from Redis

# COMMAND ----------

schema=(
    "RealEstateKeyId string,"
    "SourceSystem string,"
    "IngestionTimestamp timestamp,"
    "BatchId string,"
    "RealEstateKey string,"
    "RealEstateCepalKey string,"
    "RealEstateStatusCode string,"
    "RealEstateStatus string,"
    "RealEstateComments string,"
    "RealEstateActive string,"
    "RealEstateDescription string,"
    "RealEstateSubCategoryCode string,"
    "RealEstateSubCategory string,"
    "RealEstateCategoryCode string,"
    "RealEstateCategory string,"
    "RealEstateAddress1 string,"
    "RealEstateAddress2 string,"
    "RealEstateEntranceAddress string,"
    "RealEstatePostalCode string,"
    "RealEstateGeolocation string,"
    "RealEstateLocationLambda double,"
    "RealEstateLocationPhi double,"
    "RealEstateEncumbrances string,"
    "RealEstateEncumbrancesStatus string,"
    "RealEstateEncumbrancesDetails string,"
    "RealEstateLandArea double,"
    "RealEstateContractArea double,"
    "RealEstateNetArea double,"
    "RealEstateGlaArea double,"
    "RealEstateOccupancyTypeCode string,"
    "RealEstateOccupancyType string,"
    "RealEstateOccupancyRate double,"
    "RealEstateSurfaceArea double,"
    "RealEstatePlottedArea double,"
    "RealEstatePlottedDate date,"
    "RealEstateAcquisitionValue decimal(19,4)," 
    "RealEstateBookValue decimal(19,4),"
    "RealEstateApprovedIASValue decimal(19,4),"
    "RealEstateFiscalValue decimal(19,4),"
    "RealEstateMarketValue decimal(19,4),"
    "RealEstateEvaluationDate date,"
    "RealEstateEvaluationTypeCode string,"
    "RealEstateEvaluationType string,"
    "RealEstateForcedSaleValue decimal(19,4),"
    "RealEstateSaleAmount decimal(19,4),"
    "RealEstateSaleVATAmount decimal(19,4),"
    "RealEstateSaleTotalAmount decimal(19,4),"
    "RealEstateSaleDate date,"
    "RealEstateDateEntered date,"
    "RealEstateFirstOfferPrice decimal(19,4),"
    "RealEstateMinimumPrice decimal(19,4),"
    "RealEstateMaximumPrice decimal(19,4),"
    "RealEstateAuctionAmount decimal(19,4),"
    "RealEstateConstructionPercentage string,"
    "RealEstateLegalIssuesFlag boolean,"
    "RealEstateValueDate date,"
    "RealEstateGroupMarketValueDate date,"
    "RealEstateValueTypeCode string,"
    "RealEstateValueType string,"
    "RealEstateGroupMarketValue decimal(19,2),"
    "RealEstateGroupMarketValueTypeCode string,"
    "RealEstateGroupMarketValueType string,"
    "RealEstateConstructionYear string,"
    "RealEstateLandRegistryCode string,"
    "RealEstateLandRegistryOffice string,"
    "RealEstateLandRegistry string,"
    "RealEstateUsageCodeTypeCode string,"
    "RealEstateUsageCodeType string,"
    "RealEstateSiteCode string,"
    "RealEstatePrimaryLandLocation string,"
    "RealEstateTotalTax decimal(19,2),"
    "RealEstateOperatingCost decimal(19,2),"
    "RealEstateArea string,"
    "RealEstateCountry string,"
    "RealEstateCity string,"
    "RealEstateIsEAPFlag boolean,"
    "RealEstateDistrict string,"
    "RealEstateDivision string,"
    "RealEstateMunicipality string,"
    "RealEstateCounty string,"
    "RealEstateMarketabilityCode string,"
    "RealEstateKaekCode string,"
    "RealEstateKaekOffice string,"
    "RealEstateHotelFlag boolean,"
    "RealEstateHotelStars string,"
    "RealEstateHotelRooms int,"
    "RealEstateSpecialConditions string,"
    "RealEstateAuctionDate timestamp,"
    "RealEstateAuctionCode string,"
    "RealEstatePlaceCode string,"
    "RealEstateConstructionStatusCode string,"
    "RealEstateConstructionStatus string,"
    "RealEstateLandOwnership string,"
    "RealEstateIsEligibleFlag boolean,"
    "CreatedDate string,"
    "CreatedBy string,"
    "LastUpdatedDate string,"
    "LastUpdatedBy string,"
    "LocationTrackingCode string,"
    "LocationTracking string,"
    "Address string,"
    "OccupancyType string,"
    "OtherArea double,"
    "PropertyStatus string,"
    "BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path = f"`{env}-cdh`.derivativeactive.{table_name}"
 
# ================== Create Delta Table ==================
logging.info("Creating Delta Table...")
if spark.catalog.tableExists(derivative_table_path) is False:
    create_delta_table(
        spark,
        derivative_table_path,
        partition_columns,
        schema,  # type: ignore
    )
else:
    logging.info("Table already exists.")
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path,
    run_date,  # type: ignore
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create wanted Delta Tables with selected fields & Temporary Views for each delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ###### realestatebuilding

# COMMAND ----------

# DBTITLE 1,realestatebuilding
realestatebuilding = spark.sql(f'''
SELECT *
FROM  `{env}-cdh`.primary.realestatebuilding
WHERE BusinessDate = {run_date!r}
and RealEstateKey is not null
''')

realestatebuilding.createOrReplaceTempView("realestatebuilding")

# COMMAND ----------

OtherArea = spark.sql(f'''
    with OtherArea as (
        select  SourceSystem,RealEstateKey,RealEstateBuildingKey,RealEstateBuildingStorageArea,RealEstateBuildingParkingSpace
        from `{env}-cdh`.primary.realestatebuilding
        where BusinessDate={run_date!r}
    )
    select SourceSystem,RealEstateKey
    ,sum(coalesce(RealEstateBuildingStorageArea,0)) as RealEstateBuildingStorageArea
    ,sum(coalesce(RealEstateBuildingParkingSpace,0)) as RealEstateBuildingParkingSpace
    from OtherArea
    group by SourceSystem,RealEstateKey

''')

OtherArea.createOrReplaceTempView("OtherArea")


# COMMAND ----------

realestatevaluation_active = spark.sql(f'''
SELECT RealEstateKey, SourceSystem, RealEstateValuationType1, RealEstateValuationType2, MarketValue as ValuationMarketValue, cast(ValuationDate as date) as ValuationEvaluationDate
FROM  `{env}-cdh`.primaryactive.realestatevaluation
WHERE BusinessDate = {run_date!r}
and IsActiveFlag = true
''')

realestatevaluation_active.createOrReplaceTempView("realestatevaluation_active")

# COMMAND ----------

realestatebuilding_thesis = spark.sql(f'''
SELECT *
FROM  realestatebuilding
WHERE SourceSystem='thesis'
''')

realestatebuilding_thesis.createOrReplaceTempView("realestatebuilding_thesis")

# COMMAND ----------

df_thesis_RealEstateKeyId_primary_key_fields = [
            realestatebuilding_thesis.RealEstateKey,
            realestatebuilding_thesis.SourceSystem
        ]

# COMMAND ----------

thesis = (
            realestatebuilding_thesis.withColumn(
                "RealEstateKeyId", sha2(concat_ws(";", *df_thesis_RealEstateKeyId_primary_key_fields), 256)
            )
)
thesis.createOrReplaceTempView("thesis")    

# COMMAND ----------

# DBTITLE 1,df_thesis
df_thesis = spark.sql(f'''
select distinct
    RealEstateKeyId
    ,thesis.SourceSystem
    ,from_utc_timestamp(current_timestamp(), "Europe/Athens") as IngestionTimestamp
    ,BatchId
    ,thesis.RealEstateKey
    ,RealEstateCepalKey
    ,RealEstateStatusCode
    ,RealEstateStatus
    ,RealEstateComments
    ,RealEstateActive
    ,RealEstateDescription
    ,RealEstateSubCategoryCode
    ,RealEstateSubCategory
    ,RealEstateCategoryCode
    ,RealEstateCategory
    ,RealEstateAddress1
    ,RealEstateAddress2
    ,RealEstateEntranceAddress
    ,RealEstatePostalCode
    ,RealEstateGeolocation
    ,RealEstateLocationLambda
    ,RealEstateLocationPhi
    ,RealEstateEncumbrances
    ,RealEstateEncumbrancesStatus
    ,RealEstateEncumbrancesDetails
    ,RealEstateLandArea
    ,RealEstateContractArea
    ,RealEstateNetArea
    ,RealEstateGlaArea
    ,RealEstateOccupancyTypeCode
    ,RealEstateOccupancyType
    ,RealEstateOccupancyRate
    ,RealEstateSurfaceArea
    ,RealEstatePlottedArea
    ,RealEstatePlottedDate
    ,RealEstateAcquisitionValue
    ,RealEstateBookValue
    ,RealEstateApprovedIASValue
    ,RealEstateFiscalValue
    ,coalesce(val.ValuationMarketValue, thesis.RealEstateMarketValue) as RealEstateMarketValue
    ,coalesce(val.ValuationEvaluationDate, thesis.RealEstateEvaluationDate) as RealEstateEvaluationDate
    ,coalesce(val.RealEstateValuationType2, thesis.RealEstateEvaluationTypeCode) as RealEstateEvaluationTypeCode
    ,coalesce(val.RealEstateValuationType1, thesis.RealEstateEvaluationType) as RealEstateEvaluationType
    ,RealEstateForcedSaleValue
    ,RealEstateSaleAmount
    ,RealEstateSaleVATAmount
    ,RealEstateSaleTotalAmount
    ,RealEstateSaleDate
    ,RealEstateDateEntered
    ,RealEstateFirstOfferPrice
    ,RealEstateMinimumPrice
    ,RealEstateMaximumPrice
    ,RealEstateAuctionAmount
    ,RealEstateConstructionPercentage
    ,RealEstateLegalIssuesFlag
    ,RealEstateValueDate
    ,RealEstateGroupMarketValueDate
    ,RealEstateValueTypeCode
    ,RealEstateValueType
    ,RealEstateGroupMarketValue
    ,RealEstateGroupMarketValueTypeCode
    ,RealEstateGroupMarketValueType
    ,RealEstateConstructionYear
    ,RealEstateLandRegistryCode
    ,RealEstateLandRegistryOffice
    ,RealEstateLandRegistry
    ,RealEstateUsageCodeTypeCode
    ,RealEstateUsageCodeType
    ,RealEstateSiteCode
    ,RealEstatePrimaryLandLocation
    ,RealEstateTotalTax
    ,RealEstateOperatingCost
    ,RealEstateArea
    ,RealEstateCountry
    ,RealEstateCity
    ,RealEstateIsEAPFlag
    ,RealEstateDistrict
    ,RealEstateDivision
    ,RealEstateMunicipality
    ,RealEstateCounty
    ,RealEstateMarketabilityCode
    ,RealEstateKaekCode
    ,RealEstateKaekOffice
    ,RealEstateHotelFlag
    ,RealEstateHotelStars
    ,RealEstateHotelRooms
    ,RealEstateSpecialConditions
    ,RealEstateAuctionDate
    ,RealEstateAuctionCode
    ,RealEstatePlaceCode
    ,RealEstateConstructionStatusCode
    ,RealEstateConstructionStatus
    ,RealEstateLandOwnership
    ,RealEstateIsEligibleFlag 
    ,CreatedDate
    ,CreatedBy
    ,LastUpdatedDate
    ,LastUpdatedBy
    ,LocationTrackingCode
    ,LocationTracking
    ,CONCAT(RealEstateAddress1 , CASE WHEN coalesce(RealEstateAddress2,'') <>'' THEN CONCAT(' ',RealEstateAddress2) ELSE '' END) as Address
    ,CASE   WHEN RealEstateOccupancyType in ('Πρώτη κατοικία') THEN '1'
            WHEN RealEstateOccupancyType in ('Δεύτερη κατοικία','Εξοχική κατοικία') THEN '4'
            WHEN RealEstateOccupancyType in ('Κατάστημα Λιανικής', 'Επαγγελματική Χρήση', 'Για επένδυση','Λοιπές Χρήσεις' ) THEN '5'
            WHEN RealEstateOccupancyType ='N/A' THEN 'ND'
            WHEN RealEstateOccupancyType is not null THEN '5'
            ELSE 'ND'  END as OccupancyType   
    ,coalesce(OtherArea.RealEstateBuildingStorageArea,0) + coalesce(OtherArea.RealEstateBuildingParkingSpace,0) as OtherArea
    ,case   when RealEstateStatus = 'Ενεργό' then 'Active'
            when RealEstateStatus = 'Ανενεργό' then 'Inactive'
        else null
    end as PropertyStatus
    ,BusinessDate
from thesis
left join OtherArea
on thesis.SourceSystem= OtherArea.SourceSystem
and thesis.RealEstateKey= OtherArea.RealEstateKey
left join realestatevaluation_active val
on thesis.RealEstateKey = val.RealEstateKey
and thesis.SourceSystem = val.SourceSystem
''')

df_thesis.createOrReplaceTempView("df_thesis")             

# COMMAND ----------

realestatebuilding_arotron = spark.sql(f'''
SELECT *
FROM  realestatebuilding
WHERE SourceSystem='arotron'
''')

realestatebuilding_arotron.createOrReplaceTempView("realestatebuilding_arotron")

# COMMAND ----------

df_arotron_RealEstateKeyId_primary_key_fields = [
            realestatebuilding_arotron.RealEstateKey,
            realestatebuilding_arotron.SourceSystem
        ]

# COMMAND ----------

arotron = (
            realestatebuilding_arotron.withColumn(
                "RealEstateKeyId", sha2(concat_ws(";", *df_arotron_RealEstateKeyId_primary_key_fields), 256)
            )
)
arotron.createOrReplaceTempView("arotron")    

# COMMAND ----------

# DBTITLE 1,df_arotron
df_arotron = spark.sql(f'''
select distinct
    RealEstateKeyId
    ,arotron.SourceSystem
    ,from_utc_timestamp(current_timestamp(), "Europe/Athens") as IngestionTimestamp
    ,BatchId
    ,arotron.RealEstateKey
    ,RealEstateCepalKey
    ,RealEstateStatusCode
    ,RealEstateStatus
    ,RealEstateComments
    ,RealEstateActive
    ,RealEstateDescription
    ,RealEstateSubCategoryCode
    ,RealEstateSubCategory
    ,RealEstateCategoryCode
    ,RealEstateCategory
    ,RealEstateAddress1
    ,RealEstateAddress2
    ,RealEstateEntranceAddress
    ,RealEstatePostalCode
    ,RealEstateGeolocation
    ,RealEstateLocationLambda
    ,RealEstateLocationPhi
    ,RealEstateEncumbrances
    ,RealEstateEncumbrancesStatus
    ,RealEstateEncumbrancesDetails
    ,RealEstateLandArea
    ,RealEstateContractArea
    ,RealEstateNetArea
    ,RealEstateGlaArea
    ,RealEstateOccupancyTypeCode
    ,RealEstateOccupancyType
    ,RealEstateOccupancyRate
    ,RealEstateSurfaceArea
    ,RealEstatePlottedArea
    ,RealEstatePlottedDate
    ,RealEstateAcquisitionValue
    ,RealEstateBookValue
    ,RealEstateApprovedIASValue
    ,RealEstateFiscalValue
    ,coalesce(val.ValuationMarketValue, arotron.RealEstateMarketValue) as RealEstateMarketValue
    ,coalesce(val.ValuationEvaluationDate, arotron.RealEstateEvaluationDate) as RealEstateEvaluationDate
    ,coalesce(val.RealEstateValuationType2, arotron.RealEstateEvaluationTypeCode) as RealEstateEvaluationTypeCode
    ,coalesce(val.RealEstateValuationType1, arotron.RealEstateEvaluationType) as RealEstateEvaluationType
    ,RealEstateForcedSaleValue
    ,RealEstateSaleAmount
    ,RealEstateSaleVATAmount
    ,RealEstateSaleTotalAmount
    ,RealEstateSaleDate
    ,RealEstateDateEntered
    ,RealEstateFirstOfferPrice
    ,RealEstateMinimumPrice
    ,RealEstateMaximumPrice
    ,RealEstateAuctionAmount
    ,RealEstateConstructionPercentage
    ,RealEstateLegalIssuesFlag
    ,RealEstateValueDate
    ,RealEstateGroupMarketValueDate
    ,RealEstateValueTypeCode
    ,RealEstateValueType
    ,RealEstateGroupMarketValue
    ,RealEstateGroupMarketValueTypeCode
    ,RealEstateGroupMarketValueType
    ,RealEstateConstructionYear
    ,RealEstateLandRegistryCode
    ,RealEstateLandRegistryOffice
    ,RealEstateLandRegistry
    ,RealEstateUsageCodeTypeCode
    ,RealEstateUsageCodeType
    ,RealEstateSiteCode
    ,RealEstatePrimaryLandLocation
    ,RealEstateTotalTax
    ,RealEstateOperatingCost
    ,RealEstateArea
    ,RealEstateCountry
    ,RealEstateCity
    ,RealEstateIsEAPFlag
    ,RealEstateDistrict
    ,RealEstateDivision
    ,RealEstateMunicipality
    ,RealEstateCounty
    ,RealEstateMarketabilityCode
    ,RealEstateKaekCode
    ,RealEstateKaekOffice
    ,RealEstateHotelFlag
    ,RealEstateHotelStars
    ,RealEstateHotelRooms
    ,RealEstateSpecialConditions
    ,RealEstateAuctionDate
    ,RealEstateAuctionCode
    ,RealEstatePlaceCode
    ,RealEstateConstructionStatusCode
    ,RealEstateConstructionStatus
    ,RealEstateLandOwnership
    ,RealEstateIsEligibleFlag 
    ,CreatedDate
    ,CreatedBy
    ,LastUpdatedDate
    ,LastUpdatedBy
    ,LocationTrackingCode
    ,LocationTracking
    ,CONCAT(RealEstateAddress1 , CASE WHEN coalesce(RealEstateAddress2,'')<>'' THEN CONCAT(' ',RealEstateAddress2) ELSE '' END) as Address    
    ,CASE WHEN RealEstateOccupancyType in ('Πρώτη κατοικία') THEN '1'
        WHEN RealEstateOccupancyType in ('Δεύτερη κατοικία','Εξοχική κατοικία') THEN '4'
        WHEN RealEstateOccupancyType in ('Κατάστημα Λιανικής', 'Επαγγελματική Χρήση', 'Για επένδυση','Λοιπές Χρήσεις' ) THEN '5'
        WHEN RealEstateOccupancyType ='N/A' THEN 'ND'
        WHEN RealEstateOccupancyType is not null THEN '5'
        ELSE 'ND'  END as OccupancyType
    ,coalesce(OtherArea.RealEstateBuildingStorageArea,0) + coalesce(OtherArea.RealEstateBuildingParkingSpace,0) as OtherArea
    ,case   when RealEstateStatus = 'Ενεργό' then 'Active'
            when RealEstateStatus = 'Ανενεργό' then 'Inactive'
        else null
    end as PropertyStatus
    ,BusinessDate
from arotron
left join OtherArea
on arotron.SourceSystem= OtherArea.SourceSystem
and arotron.RealEstateKey= OtherArea.RealEstateKey
left join realestatevaluation_active val
on arotron.RealEstateKey = val.RealEstateKey
and arotron.SourceSystem = val.SourceSystem
''')

df_arotron.createOrReplaceTempView("df_arotron")                                      

# COMMAND ----------

realestatebuilding_qca = spark.sql(f'''
SELECT *
FROM  realestatebuilding
WHERE SourceSystem='qca'
''')

realestatebuilding_qca.createOrReplaceTempView("realestatebuilding_qca")

# COMMAND ----------

df_qca_RealEstateKeyId_primary_key_fields = [
            realestatebuilding_qca.RealEstateKey,
            realestatebuilding_qca.SourceSystem
        ]

# COMMAND ----------

qca = (
            realestatebuilding_qca.withColumn(
                "RealEstateKeyId", sha2(concat_ws(";", *df_qca_RealEstateKeyId_primary_key_fields), 256)
            )
)
qca.createOrReplaceTempView("qca")    

# COMMAND ----------

# DBTITLE 1,df_qca
df_qca = spark.sql(
f'''
select distinct
    RealEstateKeyId
    ,qca.SourceSystem
    ,from_utc_timestamp(current_timestamp(), "Europe/Athens") as IngestionTimestamp
    ,BatchId
    ,qca.RealEstateKey
    ,RealEstateCepalKey
    ,RealEstateStatusCode
    ,RealEstateStatus
    ,RealEstateComments
    ,RealEstateActive
    ,RealEstateDescription
    ,RealEstateSubCategoryCode
    ,RealEstateSubCategory
    ,RealEstateCategoryCode
    ,RealEstateCategory
    ,RealEstateAddress1
    ,RealEstateAddress2
    ,RealEstateEntranceAddress
    ,RealEstatePostalCode
    ,RealEstateGeolocation
    ,RealEstateLocationLambda
    ,RealEstateLocationPhi
    ,RealEstateEncumbrances
    ,RealEstateEncumbrancesStatus
    ,RealEstateEncumbrancesDetails
    ,RealEstateLandArea
    ,RealEstateContractArea
    ,RealEstateNetArea
    ,RealEstateGlaArea
    ,RealEstateOccupancyTypeCode
    ,RealEstateOccupancyType
    ,RealEstateOccupancyRate
    ,RealEstateSurfaceArea
    ,RealEstatePlottedArea
    ,RealEstatePlottedDate
    ,RealEstateAcquisitionValue
    ,RealEstateBookValue
    ,RealEstateApprovedIASValue
    ,RealEstateFiscalValue
    ,coalesce(val.ValuationMarketValue, qca.RealEstateMarketValue) as RealEstateMarketValue
    ,coalesce(val.ValuationEvaluationDate, qca.RealEstateEvaluationDate) as RealEstateEvaluationDate
    ,coalesce(val.RealEstateValuationType2, qca.RealEstateEvaluationTypeCode) as RealEstateEvaluationTypeCode
    ,coalesce(val.RealEstateValuationType1, qca.RealEstateEvaluationType) as RealEstateEvaluationType
    ,RealEstateForcedSaleValue
    ,RealEstateSaleAmount
    ,RealEstateSaleVATAmount
    ,RealEstateSaleTotalAmount
    ,RealEstateSaleDate
    ,RealEstateDateEntered
    ,RealEstateFirstOfferPrice
    ,RealEstateMinimumPrice
    ,RealEstateMaximumPrice
    ,RealEstateAuctionAmount
    ,RealEstateConstructionPercentage
    ,RealEstateLegalIssuesFlag
    ,RealEstateValueDate
    ,RealEstateGroupMarketValueDate
    ,RealEstateValueTypeCode
    ,RealEstateValueType
    ,RealEstateGroupMarketValue
    ,RealEstateGroupMarketValueTypeCode
    ,RealEstateGroupMarketValueType
    ,RealEstateConstructionYear
    ,RealEstateLandRegistryCode
    ,RealEstateLandRegistryOffice
    ,RealEstateLandRegistry
    ,RealEstateUsageCodeTypeCode
    ,RealEstateUsageCodeType
    ,RealEstateSiteCode
    ,RealEstatePrimaryLandLocation
    ,RealEstateTotalTax
    ,RealEstateOperatingCost
    ,RealEstateArea
    ,RealEstateCountry
    ,RealEstateCity
    ,RealEstateIsEAPFlag
    ,RealEstateDistrict
    ,RealEstateDivision
    ,RealEstateMunicipality
    ,RealEstateCounty
    ,RealEstateMarketabilityCode
    ,RealEstateKaekCode
    ,RealEstateKaekOffice
    ,RealEstateHotelFlag
    ,RealEstateHotelStars
    ,RealEstateHotelRooms
    ,RealEstateSpecialConditions
    ,RealEstateAuctionDate
    ,RealEstateAuctionCode
    ,RealEstatePlaceCode
    ,RealEstateConstructionStatusCode
    ,RealEstateConstructionStatus
    ,RealEstateLandOwnership
    ,RealEstateIsEligibleFlag 
    ,CreatedDate
    ,CreatedBy
    ,LastUpdatedDate
    ,LastUpdatedBy
    ,LocationTrackingCode
    ,LocationTracking
    ,CONCAT(RealEstateAddress1 , CASE WHEN coalesce(RealEstateAddress2,'')<>'' THEN CONCAT(' ',RealEstateAddress2) ELSE '' END) as Address 
    ,CASE WHEN RealEstateOccupancyType in ('Πρώτη κατοικία') THEN '1'
        WHEN RealEstateOccupancyType in ('Δεύτερη κατοικία','Εξοχική κατοικία') THEN '4'
        WHEN RealEstateOccupancyType in ('Κατάστημα Λιανικής', 'Επαγγελματική Χρήση', 'Για επένδυση','Λοιπές Χρήσεις' ) THEN '5'
        WHEN RealEstateOccupancyType ='N/A' THEN 'ND'
        WHEN RealEstateOccupancyType is not null THEN '5'
        ELSE 'ND'  END as OccupancyType
    ,coalesce(OtherArea.RealEstateBuildingStorageArea,0) + coalesce(OtherArea.RealEstateBuildingParkingSpace,0) as OtherArea
    ,case   when RealEstateStatus = 'Ενεργό' then 'Active'
            when RealEstateStatus = 'Ανενεργό' then 'Inactive'
        else null
    end as PropertyStatus
    ,BusinessDate
from qca
left join OtherArea
on qca.SourceSystem= OtherArea.SourceSystem
and qca.RealEstateKey= OtherArea.RealEstateKey
left join realestatevaluation_active val
on qca.RealEstateKey = val.RealEstateKey
and qca.SourceSystem = val.SourceSystem
 ''')

df_qca.createOrReplaceTempView("df_qca")             

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Final query with resulted DataFrame

# COMMAND ----------

# DBTITLE 1,final_df
final_df = spark.sql('''
    SELECT *
    FROM df_thesis
    UNION
    SELECT *
    FROM df_arotron
    UNION
    SELECT *
    FROM df_qca
''')

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write DataFrame to Delta Lake based on 'unity' value

# COMMAND ----------

if unity == "True":
    final_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    final_df.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
