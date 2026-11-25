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

dparty = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} and sourcesystem='profits'
                   AND {portfolio_filter}
                      """)

dparty.createOrReplaceTempView("dparty")

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

realestateinfo = spark.sql(f"""
                           select * 
                           from `{env}-cdh`.primary.realestateinfo
                           where BusinessDate = {run_date!r} and SourceSystem = 'thesis'
                           """)

realestateinfo.createOrReplaceTempView("realestateinfo")

dktranslationmapping = spark.sql(f"""
                                 select * 
                                 from `{env}-cdh`.reference.dktranslationmapping
                                 """)

dktranslationmapping.createOrReplaceTempView("dktranslationmapping")

# COMMAND ----------

# ##ExternalLienDB 

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

CommonFlag = spark.sql(f"""
SELECT
r.RealEstateKey,
dre.RealEstateCepalKey,
CASE WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey 
and ci.PortfolioName = 'Cetus'  THEN 'true' ELSE 'false' END AS CommonFlag_Cetus,
CASE WHEN ci.RealEstateCepalKey = dre.RealEstateCepalKey 
and ci.PortfolioName like 'Hermes%'  THEN 'true' ELSE 'false' END AS CommonFlag_Hermes		
FROM dcollateral r
LEFT JOIN drealestate dre
ON r.RealEstateKey = dre.RealEstateKey			
LEFT JOIN CollateralREInput_1  ci  
ON ci.RealEstateCepalKey = dre.RealEstateCepalKey 
AND ci.InitialLegalOrder = r.InitialLegalOrder
""")

CommonFlag.createOrReplaceTempView('CommonFlag')

ExternalLienDB = spark.sql(f"""
SELECT distinct
dcol.BusinessDate as ReportingDate,
dcol.PortfolioName,
re.RELienKey,
re.OwnershipKey, 
dcol.DCollateralKey, 
dre.RealEstateCepalKey, 
cast(null as string) AS LegalID,
case when trim(dparty.TaxRegistrationNumber) = '' THEN NULL ELSE trim(dparty.TaxRegistrationNumber)  END AS TaxRegistrationNumber,  
dparty.LastNameGR, 
re.RELienInitialOrder, 
re.RELienMortgageAmount,
case when dcol.PortfolioName ='Cetus' then try_cast(cm.CommonFlag_Cetus as boolean)
when dcol.PortfolioName like 'Hermes%' then try_cast(cm.CommonFlag_Hermes as boolean)
end as CommonFlag,
dcol.Currency,
high.HighBalanceFlag
FROM dcollateral dcol
INNER JOIN realestateinfo re
ON dcol.RealEstateKey = re.RealEstateKey
AND re.OwnerPartyKey = dcol.OwnerPartyKey
INNER JOIN drealestate dre
ON dre.RealEstateKey = dcol.RealEstateKey
LEFT JOIN dparty_full dparty
on dparty.PartyKeyThesis = re.RELienCreditorKey
LEFT JOIN (
select DISTINCT realestateinfo.RELienCreditorName, realestateinfo.RealEstateKey, dparty.TaxRegistrationNumber, realestateinfo.BusinessDate
from realestateinfo
inner join dparty
on dparty.PartyKeyThesis = realestateinfo.OwnerPartyKey) entity
ON entity.RealEstateKey = dcol.RealEstateKey
LEFT JOIN dktranslationmapping trn
ON trn.FieldValue = re.OwnershipTypeCode
LEFT JOIN CommonFlag cm
ON cm.RealEstateKey = dcol.RealEstateKey
left join Collateral_HighBalanceFlag high 
on high.DCollateralKey = dcol.DCollateralKey
WHERE re.RELienKey IS NOT NULL
AND ((dcol.PortfolioName ='Cetus' and try_cast(cm.CommonFlag_Cetus as boolean) = true) OR 
     (dcol.PortfolioName like 'Hermes%' and try_cast(cm.CommonFlag_Hermes as boolean) = true) OR 
     (dcol.PortfolioName NOT IN ('Cetus') AND dcol.PortfolioName NOT LIKE 'Hermes%')) 
""")

target_table_path_ExternalLienDB = f"{target_area}.ExternalLienDB"

if spark.catalog.tableExists(target_table_path_ExternalLienDB):
    delete_from_unity_table(
    spark,
    target_table_path_ExternalLienDB,
    run_date,
    portfolio_filter,
)

ExternalLienDB = ExternalLienDB.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

ExternalLienDB.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_ExternalLienDB) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_ExternalLienDB} SYNC METADATA")
