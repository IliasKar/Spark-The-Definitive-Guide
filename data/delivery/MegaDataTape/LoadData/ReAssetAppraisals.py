# Databricks notebook source

# COMMAND ----------

# ##Initialization

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
dbutils.widgets.text("portfolio_exact_match", "false", "Exact Match")

# COMMAND ----------

from pyspark.sql.functions import (
    coalesce, col, concat_ws, current_timestamp, from_utc_timestamp,
    lit, sha2, substring, trim, when, months_between, dateadd, datediff,
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
                      WHERE BusinessDate <= {run_date!r} AND sourcesystem = 'profits'
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
                        where BusinessDate = {run_date!r}
                      """)

drealestate.createOrReplaceTempView("drealestate")

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

# COMMAND ----------

# ##ReAssetAppraisals

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
select  dcol.RealEstateKey,
CASE 
        WHEN COUNT(DISTINCT CASE WHEN HighBalanceFlag = true THEN 1 END) > 0 THEN true
        ELSE false  
    END AS HighBalanceFlag
FROM dcollateral dcol
LEFT JOIN Agreement_HighBalanceFlag agg
ON agg.AgreementKey = dcol.AgreementKey
group by dcol.RealEstateKey
''')

Collateral_HighBalanceFlag.createOrReplaceTempView('Collateral_HighBalanceFlag')

REAssetAppraisals = spark.sql(f"""
SELECT distinct
re.BusinessDate as ReportingDate,
col.PortfolioName,
re.RealEstateValuationKey AS AppraisalID,
dre.RealEstateCepalKey,
case when col.PortfolioName in ('Cosmos Wholesale','Cosmos Retail') THEN
    CASE WHEN re.RealEstateValuationType1 = 'Auction Result' THEN 'Auction result'
         ELSE re.RealEstateValuationType1 END
ELSE
    CASE WHEN re.RealEstateValuationTypeExtended = 'Auction Result' THEN 'Auction result'
         ELSE re.RealEstateValuationTypeExtended END
END AS AppraisalType,
cast(re.ValuationDate as date) AS AppraisalDate,
re.MarketValue AS AppraisalAmount,
dp.TaxRegistrationNumber,
case when TRIM(dp.FirstNameGR) != '' AND dp.FirstNameGR IS NOT NULL THEN CONCAT(dp.FirstNameGR,' ', dp.LastNameGR) ELSE dp.LastNameGR END AS AppraisalCompanyName,
high.HighBalanceFlag
FROM realestatevaluation_calc re
INNER JOIN dcollateral col
    on col.RealEstateKey = re.RealEstateKey
LEFT JOIN dparty_full dp
    ON re.RealEstateValuatorKey = dp.PartyKeyThesis
LEFT JOIN drealestate dre
    ON re.RealEstateKey = dre.RealEstateKey
left join Collateral_HighBalanceFlag high 
on high.RealEstateKey = re.RealEstateKey
WHERE 
    (col.PortfolioName not in ('Cosmos Wholesale','Cosmos Retail')
     and not (RealEstateValuationType1 = 'Migration' and RealEstateValuationType2 is null)
     AND LastNameGR not in('Cosmos TDT_0_10', 'Cosmos TDT_0_50'))
OR col.PortfolioName in ('Cosmos Wholesale','Cosmos Retail')
""")

target_table_path_REAssetAppraisals = f"{target_area}.REAssetAppraisals"

if spark.catalog.tableExists(target_table_path_REAssetAppraisals):
    delete_from_unity_table(spark, target_table_path_REAssetAppraisals, run_date, portfolio_filter)

REAssetAppraisals = REAssetAppraisals.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

REAssetAppraisals.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_REAssetAppraisals)

spark.sql(f"REPAIR TABLE {target_table_path_REAssetAppraisals} SYNC METADATA")
