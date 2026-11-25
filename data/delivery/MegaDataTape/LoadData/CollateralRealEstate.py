# Databricks notebook source

# COMMAND ----------

# ##Initialization

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
dbutils.widgets.text("portfolio_exact_match", "false", "Exact Match")

# COMMAND ----------

from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    trim,
    when,
    months_between,
    dateadd,
    datediff,
)

import os
import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname
from helpers import get_validated_eom_run_date, build_portfolio_filter, delete_from_unity_table

env = get_env_prefix()
env_name = get_env_fullname()

partition_columns = ["ReportingDate"]
run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
portfolio_filter, portfolio_param, portfolio_display = build_portfolio_filter(dbutils.widgets.get("portfolio"), portfolio_exact_match=dbutils.widgets.get("portfolio_exact_match").strip().lower() == "true")
target_area = f"`{env}-cdh-delivery`.`mega-datatape`"

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
print(f"Executing: {notebook_name} | run_date: {run_date} | portfolio: {portfolio_display}")

# COMMAND ----------

# ##Import Tables

# COMMAND ----------

dexposure = spark.sql(f"""
                      SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                      """)

dexposure.createOrReplaceTempView("dexposure")

dparty_full = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r}
                   AND {portfolio_filter}
                      """)

dparty_full.createOrReplaceTempView("dparty_full")


dcollateral = spark.sql(f"""
                        select * 
                        from `{env}-cdh`.derivative.dcollateral
                        where BusinessDate = {run_date!r}
                        AND {portfolio_filter}
                      """)

dcollateral.createOrReplaceTempView("dcollateral")

drealestate = spark.sql(f"""
                        select *
                        from `{env}-cdh`.derivative.drealestate
                        where BusinessDate = {run_date!r} --and SourceSystem = 'thesis'
                      """)

drealestate.createOrReplaceTempView("drealestate")

dnonrealestate = spark.sql(f"""   -- EK 20260309
                        select *
                        from primary.nonrealestate
                        where BusinessDate = {run_date!r}
                      """)

dnonrealestate.createOrReplaceTempView("dnonrealestate")

realestateinfo = spark.sql(f"""
                           select re.* 
                           from `{env}-cdh`.primary.realestateinfo as re
                           left join dcollateral as dcol      -- EK 20260309 new join
                           ON dcol.RealEstateKey = re.RealEstateKey
                           AND re.OwnershipKey = dcol.OwnershipKey
                           where re.BusinessDate = {run_date!r} and re.SourceSystem = 'thesis'
                           and (re.OwnershipStatus = 'Active'
                           or dcol.PortfolioName IN ('Galaxy II Funding DAC',       -- EK 20260309 also include Inactive Property Status for these portfolios
                                                    'Galaxy IV Funding DAC', 
                                                    'Orion X Securitisation DAC', 
                                                    'Cosmos Retail', 
                                                    'Cosmos Wholesale', 
                                                    'Cetus',
                                                    'Hermes Portfolio A', 
                                                    'Hermes Portfolio B', 
                                                    'Hermes Portfolio C')) 
                           """)

realestateinfo.createOrReplaceTempView("realestateinfo")

dktranslationmapping = spark.sql(f"""
                                 select * 
                                 from `{env}-cdh`.reference.dktranslationmapping
                                 """)

dktranslationmapping.createOrReplaceTempView("dktranslationmapping")

# COMMAND ----------

# ##CollateralRealEstate

# COMMAND ----------

Agreement_HighBalanceFlag = spark.sql(f"""
SELECT
    agreementkey,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN IsHighBalanceFlag = true THEN 1 END) > 0 THEN true
        ELSE false  
    END AS HighBalanceFlag
FROM dexposure
GROUP BY agreementkey
""")

Agreement_HighBalanceFlag.createOrReplaceTempView('Agreement_HighBalanceFlag')


Collateral_HighBalanceFlag=spark.sql(f'''
select  dcol.DCollateralKey,
CASE 
        WHEN COUNT(DISTINCT CASE WHEN HighBalanceFlag = true THEN 1 END) > 0 THEN true
        ELSE false  
    END AS HighBalanceFlag
FROM dcollateral dcol
LEFT JOIN Agreement_HighBalanceFlag agg
ON agg.AgreementKey = dcol.AgreementKey
group by dcol.DCollateralKey
''')
 
Collateral_HighBalanceFlag.createOrReplaceTempView('Collateral_HighBalanceFlag')

Agreement_Subservicer = spark.sql(f"""
SELECT
    agreementkey,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN Subservicer IS NULL THEN 1 END) > 0 
             AND COUNT(DISTINCT CASE WHEN Subservicer = 'EUCLID DV01 - ASSIGN FOR SERVICING' THEN 1 END) > 0 THEN 'Common'
        WHEN COUNT(DISTINCT CASE WHEN Subservicer IS NULL THEN 1 END) > 0 THEN 'Cepal'
        WHEN COUNT(DISTINCT CASE WHEN Subservicer = 'EUCLID DV01 - ASSIGN FOR SERVICING' THEN 1 END) > 0 THEN 'DV01'
        ELSE Null  
    END AS SubservicerFlag
FROM dexposure
GROUP BY agreementkey
""")

Agreement_Subservicer.createOrReplaceTempView('Agreement_Subservicer')


Collateral_SubServicer=spark.sql(f'''
select  dcol.DCollateralKey,
CASE WHEN COUNT(CASE WHEN SubservicerFlag ='Cepal' THEN 1 END) > 0 
             AND COUNT(CASE WHEN SubservicerFlag = 'DV01' THEN 1 END) > 0 THEN 'Common'
        WHEN COUNT(CASE WHEN SubservicerFlag = 'DV01' THEN 1 END) > 0 
             AND COUNT(CASE WHEN SubservicerFlag ='Cepal' THEN 1 END) = 0 THEN 'DV01'
        ELSE 'Cepal'
END AS SubservicerFlag
FROM dcollateral dcol
LEFT JOIN Agreement_Subservicer agg
ON agg.AgreementKey = dcol.AgreementKey
group by dcol.DCollateralKey
''')
 
Collateral_SubServicer.createOrReplaceTempView('Collateral_SubServicer')

DistinctCollateralREInput = spark.sql(f"""
select distinct dcol.AgreementKey, dre.RealEstateCepalKey, dcol.InitialLegalOrder, dcol.PortfolioName
from dcollateral dcol
LEFT JOIN drealestate dre
ON dcol.RealEstateKey = dre.RealEstateKey
""")
 
DistinctCollateralREInput.createOrReplaceTempView('DistinctCollateralREInput')
 
CollateralREInput_1 = spark.sql(f"""
select dist.RealEstateCepalKey, dist.InitialLegalOrder, dist.PortfolioName, count(*) as Count
from DistinctCollateralREInput dist
group by all
having count(*)>1
""")
 
CollateralREInput_1.createOrReplaceTempView('CollateralREInput_1')

# CommonFlag = spark.sql(f"""
# SELECT
# r.RealEstateKey,
# dre.RealEstateCepalKey,
# CASE WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey 
# and ci.PortfolioName = 'Cetus'  THEN 'true' ELSE 'false' END AS CommonFlag_Cetus,
# CASE WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey 
# and ci.PortfolioName like 'Hermes%'  THEN 'true' ELSE 'false' END AS CommonFlag_Hermes		
# FROM dcollateral r
# LEFT JOIN drealestate dre
# ON r.RealEstateKey = dre.RealEstateKey			
# LEFT JOIN CollateralREInput_1  ci  
# ON ci.RealEstateCepalKey = dre.RealEstateCepalKey 
# AND ci.InitialLegalOrder = r.InitialLegalOrder
# """)

# CommonFlag.createOrReplaceTempView('CommonFlag')

CommonFlag = spark.sql(f"""  -- EK 20260309
SELECT
r.RealEstateKey,
dre.RealEstateCepalKey, 
r.PortfolioName,
r.InitialLegalOrder,
CASE 
    WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey 
         AND ci.PortfolioName = 'Cetus'
    THEN TRUE
    WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey
         AND ci.PortfolioName LIKE 'Hermes%'
    THEN TRUE
    ELSE FALSE
END AS CommonFlag 
FROM dcollateral r
LEFT JOIN drealestate dre
ON r.RealEstateKey = dre.RealEstateKey			
LEFT JOIN CollateralREInput_1  ci  
ON ci.RealEstateCepalKey = dre.RealEstateCepalKey 
AND ci.InitialLegalOrder = r.InitialLegalOrder
AND ci.PortfolioName = r.PortfolioName
""")

CommonFlag.createOrReplaceTempView('CommonFlag')

CollateralMapping = spark.sql(f"""
SELECT distinct
dcol.BusinessDate as ReportingDate,
dcol.PortfolioName,
dre.RealEstateCepalKey,
re.OwnershipKey,
dex.ExposureKey, 
dcol.AgreementKey,
dcol.DCollateralKey,
dcol.CollateralAmount,
dcol.CollateralType,  
dcol.CollateralTypeDescription,  
dcol.InitialLegalOrder,
case when CollateralType ='RE' THEN REMortgageRegistrationDate ELSE NULL END REMortgageRegistrationDate,
COALESCE(case when CollateralType ='RE' THEN dcol.RealEstateCollateralStatus ELSE dcol.CollateralStatus END , 'Inactive') CollateralStatus ,
dre.RealEstateActive,
case when dcol.CollateralType ='RE' then (
case when dcol.subcategory = 'Προσημείωση' THEN 'Prenotation' 
else 'Mortgage' end) 
else Null END AS LienType,
dex.CDI,
dcol.Currency,
case when dcol.PortfolioName ='EUCLID' then sub.SubservicerFlag
else null end as SubservicerFlag,
high.HighBalanceFlag,
cm.CommonFlag, -- EK 20260309
dp.CDI as GuarantorID -- EK 20260309
FROM dexposure dex
INNER JOIN dcollateral dcol
ON dex.AgreementKey = dcol.AgreementKey
LEFT JOIN realestateinfo re
ON dcol.RealEstateKey = re.RealEstateKey
--AND re.OwnerPartyKey = dcol.OwnerPartyKey
AND re.OwnershipKey = dcol.OwnershipKey
LEFT JOIN drealestate dre
ON dcol.RealEstateKey = dre.RealEstateKey
LEFT JOIN Collateral_SubServicer sub 
ON sub.DCollateralKey = dcol.DCollateralKey
left join Collateral_HighBalanceFlag high 
on high.DCollateralKey = dcol.DCollateralKey
--LEFT JOIN exposure ex
--on dex.exposurekey = ex.exposurekey
LEFT JOIN CommonFlag cm -- EK 20260309
ON cm.RealEstateKey = dcol.RealEstateKey 
AND cm.RealEstateCepalKey = dre.RealEstateCepalKey 
AND cm.PortfolioName = dcol.PortfolioName 
AND cm.InitialLegalOrder = dcol.InitialLegalOrder 
LEFT JOIN dnonrealestate nonre -- EK 20260309
ON CAST(nonre.CollateralKey AS STRING) = dcol.DCollateralKey 
and CAST(nonre.NonRealEstateKey AS STRING) = dcol.NonRealEstateKey 
LEFT JOIN dparty_full dp -- EK 20260309
ON nonre.OwnerPartyKey = dp.PartyKeyThesis
WHERE dcol.CollateralType is not null
""")

CollateralMapping.createOrReplaceTempView('CollateralMapping')

PropertyOwnership = spark.sql(f"""
SELECT distinct
re.BusinessDate as ReportingDate,
re.OwnershipKey,
dcol.DCollateralKey,
dre.RealEstateCepalKey,
dp.CDI AS OwnerID, 
trn.FieldValueENG, 
case when trn.FieldValueENG = 'Full' then 'Y' else 'N' end  AS FullOwnershipFlag, 
re.OwnershipPercentage
FROM  realestateinfo re
INNER JOIN dcollateral dcol
ON dcol.RealEstateKey = re.RealEstateKey
AND re.OwnerPartyKey = dcol.OwnerPartyKey
LEFT JOIN dparty_full dp
ON re.OwnerPartyKey = dp.PartyKeyThesis
LEFT JOIN drealestate dre
ON dre.RealEstateKey = re.RealEstateKey
LEFT JOIN dktranslationmapping trn
ON trn.FieldValue = re.OwnershiptypeCode
where dcol.PortfolioName is not null
""")

PropertyOwnership.createOrReplaceTempView('PropertyOwnership')

CollateralRealEstate = spark.sql(f"""SELECT 
                                 colm.ReportingDate,	
                                 colm.PortfolioName,	
                                 colm.RealEstateCepalKey,	
                                 colm.OwnershipKey,	
                                 colm.ExposureKey,	
                                 colm.AgreementKey,	
                                 colm.DCollateralKey,	
                                 colm.CollateralAmount,	
                                 colm.CollateralType,	
                                 colm.CollateralTypeDescription,	
                                 colm.InitialLegalOrder,	
                                 colm.REMortgageRegistrationDate,	
                                 colm.CollateralStatus,	
                                 colm.RealEstateActive,	
                                 colm.LienType,	
                                 colm.CDI,	
                                 colm.Currency,
                                 colm.SubservicerFlag,
                                 colm.HighBalanceFlag,		
                                 prop.OwnerID,	
                                 prop.FieldValueENG,	
                                 prop.FullOwnershipFlag,	
                                 prop.OwnershipPercentage,
                                 colm.CommonFlag, -- EK 20260309
                                 colm.GuarantorID -- EK 20260309
                                 FROM CollateralMapping colm
                                 LEFT JOIN PropertyOwnership prop
                                 ON colm.OwnershipKey= prop.OwnershipKey 	
                                 AND colm.DCollateralKey= prop.DCollateralKey  
""")

target_table_path_CollateralRealEstate = f"{target_area}.CollateralRealEstate"

if spark.catalog.tableExists(target_table_path_CollateralRealEstate):
    delete_from_unity_table(
    spark,
    target_table_path_CollateralRealEstate,
    run_date,
    portfolio_filter,
)

CollateralRealEstate = CollateralRealEstate.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

CollateralRealEstate.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_CollateralRealEstate) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_CollateralRealEstate} SYNC METADATA")
