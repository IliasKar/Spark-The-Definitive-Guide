# Databricks notebook source
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

staticpropertyoriginalvaluation = spark.sql(f"""
                                            select *
                                            from `{env}-cdh`.reference.staticpropertyoriginalvaluation
                                            """)

staticpropertyoriginalvaluation.createOrReplaceTempView("staticpropertyoriginalvaluation")

staticpropertydata = spark.sql(f"""
                                            select *
                                            from `{env}-cdh`.reference.staticpropertydata
                                            """)

staticpropertydata.createOrReplaceTempView("staticpropertydata")

realestatevaluation = spark.sql(f"""
                                select * 
                                from `{env}-cdh`.primary.realestatevaluation
                                where BusinessDate = {run_date!r} and SourceSystem = 'thesis'
                                and MarketValue is not NULL
                                and ValuationDate is not NULL 
                               """)

realestatevaluation.createOrReplaceTempView("realestatevaluation")

realestatevaluation_calc1 = spark.sql(f"""
                                SELECT * 
                                FROM(
                                  select 
                                   RealEstateKey
                                  ,RealEstateKey
                                  ,RealEstateValuationType1
                                  ,RealEstateValuationType2
                                  ,ValuationDate
                                  ,MarketValue
                                  ,RealEstateValuationKey
                                  ,IsActiveFlag
                                  ,RealEstateValuatorKey
                                  ,BusinessDate 
                                   , rank() over (partition by RealEstateKey,RealEstateValuationType1,RealEstateValuationType2,ValuationDate,MarketValue order by IsActiveFlag desc, RealEstateValuationKey asc ) rnk 
                                  from realestatevaluation
                                )
                                where rnk = 1
                               """)

realestatevaluation_calc1.createOrReplaceTempView("realestatevaluation_calc1")


realestatevaluation_duplicate = spark.sql(f"""
                                select RealEstateKey, RealEstateValuatorKey ,RealEstateValuationType1 ,MarketValue , ValuationDate ,count(*) NumberOfProperties
                                from  realestatevaluation_calc1 
                                group by RealEstateKey, RealEstateValuatorKey , RealEstateValuationType1 ,MarketValue , ValuationDate
                                having count(*)>1
                               """)

realestatevaluation_duplicate.createOrReplaceTempView("realestatevaluation_duplicate")


realestatevaluation_calc = spark.sql(f"""
                                select distinct a.* , 
                                  CASE WHEN b.RealEstateKey is not null then  concat(a.RealEstateValuationType1,'_', a.RealEstateValuationType2)
                                                                        ELSE a.RealEstateValuationType1 end as RealEstateValuationTypeExtended
                                from  realestatevaluation_calc1  a
                                left join realestatevaluation_duplicate b
                                  on a.RealEstateKey = b.RealEstateKey 
                                  and a.RealEstateValuatorKey = b.RealEstateValuatorKey 
                                  and a.RealEstateValuationType1 =b.RealEstateValuationType1
                                  and a.MarketValue =b.MarketValue
                                  and a.ValuationDate =b.ValuationDate
                               """)

realestatevaluation_calc.createOrReplaceTempView("realestatevaluation_calc")

geographicalarea = spark.sql(f"""
                    select * 
                    from `{env}-cdh`.reference.geographicalarea
                    WHERE PostalCode <> '534 38'
                     """)

geographicalarea.createOrReplaceTempView("geographicalarea")

monzadatatapeclosing = spark.sql(f"""
                                 select * 
                                 from `{env}-cdh`.reference.monzadatatapeclosing
                                 """)

monzadatatapeclosing.createOrReplaceTempView("monzadatatapeclosing")

# COMMAND ----------

# ##Collateral

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

CollateralTapeMixed = spark.sql(f"""
SELECT distinct
dcol.BusinessDate as ReportingDate,
dcol.PortfolioName,
dcol.DCollateralKey,
dcol.CollateralType, 
dcol.CollateralTypeDescription, 
dcol.CollateralAmount,
dcol.Currency,
case when dcol.PortfolioName ='EUCLID' then sub.SubservicerFlag
else null end as SubservicerFlag,
high.HighBalanceFlag
FROM dcollateral dcol
left join Collateral_SubServicer sub 
on sub.DCollateralKey = dcol.DCollateralKey
left join Collateral_HighBalanceFlag high 
on high.DCollateralKey = dcol.DCollateralKey
""")

CollateralTapeMixed.createOrReplaceTempView("CollateralTapeMixed")

realestatevaluation_calc_final = spark.sql("""
                 SELECT DISTINCT rev.*, dcol.PortfolioName as ColPortfolioName
                 FROM dcollateral dcol
                 INNER JOIN realestatevaluation_calc rev
                    ON rev.RealEstateKey = dcol.RealEstateKey
                LEFT JOIN dparty_full dp
                    ON rev.RealEstateValuatorKey = dp.PartyKeyThesis
                WHERE ( 
                    (   dcol.PortfolioName not in ('Cosmos Wholesale'
                                             ,'Cosmos Retail' )   
                        and not (rev.RealEstateValuationType1 = 'Migration' and rev.RealEstateValuationType2 is null)
                    	AND  LastNameGR not in( 'Cosmos TDT_0_10', 'Cosmos TDT_0_50')
                    )
                    OR dcol.PortfolioName in ('Cosmos Wholesale' ,'Cosmos Retail' ) 
                )     
""")

realestatevaluation_calc_final.createOrReplaceTempView("realestatevaluation_calc_final");


latest_valuation = spark.sql("""
 
            SELECT 
            a.RealEstateKey,
            a.RealEstateValuatorKey,
            a.MarketValue LatestValuationAmount,
            a.RealEstateValuationType1 LatestValuationType,
            a.RealEstateValuationType2 LatestValuationType2,
            a.RealEstateValuationTypeExtended LatestValuationTypeExtended,
            a.ValuationDate LatestValuationDate,
            a.ColPortfolioName
            FROM (
            SELECT DISTINCT 
                v.RealEstateKey
            ,	v.MarketValue 
            ,	v.ValuationDate
            ,   v.RealEstateValuatorKey
            ,	v.RealEstateValuationType1
            ,	v.RealEstateValuationType2
            ,   v.RealEstateValuationTypeExtended
            ,   v.ColPortfolioName
            ,	RANK() OVER(PARTITION BY v.RealEstateKey,v.ColPortfolioName ORDER BY v.RealEstateValuationKey DESC) AS Rank
            FROM realestatevaluation_calc_final v
            INNER JOIN ( 
            SELECT
                RealEstateKey
            ,   ColPortfolioName
            ,	MAX(ValuationDate) ValuationDate
            FROM realestatevaluation_calc_final 
            GROUP BY RealEstateKey,ColPortfolioName
            ) v0 
            ON v.ValuationDate = v0.ValuationDate 
            AND v.RealEstateKey = v0.RealEstateKey
            AND v.ColPortfolioName = v0.ColPortfolioName

            ) a
            WHERE a.Rank = 1
 
                                    
 
""")

latest_valuation.createOrReplaceTempView("latest_valuation")

CollateralTapeRE = spark.sql(f"""
SELECT distinct
dre.BusinessDate as ReportingDate,
dcol.PortfolioName,
dcol.DCollateralKey,
dre.RealEstateCepalKey,
dre.RealEstateSubCategory, 
dre.RealEstateAddress1, 
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(dre.RealEstateCity)), CHAR(9), ' '), CHAR(10), ' '), CHAR(11), ' '), CHAR(12), ' '), CHAR(13), ' ')))     AS RealEstateCity,
dre.RealEstatePostalcode,
ga.AreaENG,
CASE WHEN dre.RealEstatePostalCode IS NOT NULL AND ga.AreaENG <> 'Cyprus' THEN 'Greece' ELSE  ga.AreaENG END Country,
dre.RealEstateConstructionYear,
CASE 
  WHEN dcol.PortfolioName = 'EUCLID' 
    THEN CAST(`{env}-cdh`.derivative.string_to_double(NULLIF(cast(dat.Total_Original_Appraisal_Value as string), 'NULL')) AS STRING)
  WHEN dcol.PortfolioName LIKE '%MONZA%' 
    THEN CAST(`{env}-cdh`.derivative.string_to_double(NULLIF(cast(monza.InitialValuation as string), 'NULL')) AS STRING)
  ELSE 
    CAST(`{env}-cdh`.derivative.string_to_double(NULLIF(cast(pro.MarketValue as string), 'NULL')) AS STRING)
END AS OriginalValuationAmount,
CASE 
    WHEN dcol.PortfolioName = 'EUCLID' 
        THEN try_cast(dat.Original_Appraisal_Date_dt as TIMESTAMP)
    WHEN dcol.PortfolioName like '%MONZA%' 
        THEN monza.InitialValuationDate
    ELSE pro.ValuationDate 
END AS OriginalValuationDate, 
CASE 
    WHEN dcol.PortfolioName = 'EUCLID' THEN 
        CASE 
            WHEN COALESCE(latest.LatestValuationType, dre.Realestatevaluetype) = 'ΑΓΟΡΑΙΑ' 
            THEN 'Market Value'
            ELSE COALESCE(latest.LatestValuationType, dre.Realestatevaluetype)
        END
WHEN dcol.PortfolioName like '%MONZA%' THEN monza.VALUATIONMETHOD
ELSE pro.ValuationTypeENG 
END AS OriginalValuationType,
latest.LatestValuationAmount AS LatestValuationAmount,
cast(latest.LatestValuationDate as date) AS LatestValuationDate, 
case when dcol.PortfolioName in (
                                'Cosmos Wholesale'
                               ,'Cosmos Retail'
                                    )   
                    then latest.LatestValuationType
                    else latest.LatestValuationTypeExtended end AS LatestValuationType, 
dre.RealEstateNetArea,
dre.RealEstateLandArea,
dre.OtherArea, 
dre.RealEstateActive,
(case when dcol.PortfolioName like 'Hermes%' then cast(null as string) 
when dcol.PortfolioName ='Cetus' then dre.RealEstateGeolocation end) as RealEstateGeolocation,
dre.RealEstateCategory
FROM dcollateral dcol --FROM drealestate dre 
LEFT JOIN drealestate dre
ON dcol.RealEstateKey = dre.RealEstateKey
LEFT JOIN staticpropertyoriginalvaluation pro
ON pro.PropertyKey = dre.RealEstateCepalKey
LEFT JOIN staticpropertydata dat
ON dat.cepal_key = dre.RealEstateCepalKey
LEFT JOIN monzadatatapeclosing monza
ON concat('PIR',monza.RealEstateID)=dre.RealEstateCepalKey
LEFT JOIN geographicalarea ga
ON left(replace(ga.PostalCode, ' ',''),3)  = left(replace(dre.RealEstatePostalCode, ' ', ''),3)     -- EK 20260309
--LEFT JOIN dcollateral dcol
--ON dcol.RealEstateKey = dre.RealEstateKey
LEFT JOIN latest_valuation latest
ON LATEST.RealEstateKey = dre.RealEstateKey
AND LATEST.ColPortfolioName = dcol.PortfolioName
""")

CollateralTapeRE.createOrReplaceTempView("CollateralTapeRE")

Collateral = spark.sql(f"""
                       select
                       mixed.ReportingDate,
                       mixed.PortfolioName,     
                       mixed.DCollateralKey,
                       mixed.CollateralType,
                       mixed.CollateralTypeDescription,
                       mixed.CollateralAmount,
                       mixed.Currency, 
                       mixed.SubservicerFlag,
                       mixed.HighBalanceFlag,
                       re.RealEstateCepalKey,	
                       re.RealEstateSubCategory,	
                       re.RealEstateAddress1,	
                       re.RealEstateCity,	
                       re.RealEstatePostalcode,	
                       re.AreaENG,	
                       re.Country,	
                       re.RealEstateConstructionYear,	
                       CAST(re.OriginalValuationAmount AS STRING) AS OriginalValuationAmount,	
                       CAST(re.OriginalValuationDate AS STRING) AS OriginalValuationDate,
                       re.OriginalValuationType,
                       re.LatestValuationAmount,	
                       re.LatestValuationDate,	
                       re.LatestValuationType,	
                       re.RealEstateNetArea,	
                       re.RealEstateLandArea,	
                       re.OtherArea,	
                       re.RealEstateActive,	
                       re.RealEstateGeolocation,	
                       re.RealEstateCategory 
                       from CollateralTapeRE re 
                       left join  CollateralTapeMixed mixed 
                       on re.DCollateralKey = mixed.DCollateralKey  
                       where mixed.Collateraltype in ('RE', 'Non-RE')
""")

Collateral.createOrReplaceTempView('Collateral')

# COMMAND ----------

target_table_path_Collateral = f"{target_area}.Collateral"

if spark.catalog.tableExists(target_table_path_Collateral):
    delete_from_unity_table(
    spark,
    target_table_path_Collateral,
    run_date,
    portfolio_filter,
)

Collateral = Collateral.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

Collateral.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_Collateral) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_Collateral} SYNC METADATA")
