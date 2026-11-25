# Databricks notebook source
import logging
import os

from pyspark.sql.functions import coalesce, concat_ws, lit, sha2, substring, when, col, regexp_replace, trim
from pyspark.sql import DataFrame

import delta_table_creator, transform
from delta import DeltaTable  # type: ignore

# COMMAND ----------

# dbutils.widgets.text("run_date", '2024-08-27')
# dbutils.widgets.text("unity", 'True')

# COMMAND ----------

# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"] 
# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
unity = dbutils.widgets.get("unity") 

table_name = 'DBPHAPS'

# COMMAND ----------

# DBTITLE 1,SCHEMA
schema = (
"BPHAPSId string"
",SourceSystem string"
",SourcePortfolioName string"
",IngestionTimestamp timestamp"
",BatchId string"
",BPHAPSKey string"
",DebtorKey double"
",CDI string"
",HAPSPeriod timestamp"
",HAPSTarget double"
",DebtorName string"
",TaxRegistrationNumber string"
",GroupCode double"
",GroupName string"
",Unit string"
",ExposureSales double"
",LGFlag boolean"
",AssetSegment string"
",BPSegment string"
",BPApproach string"
",InitialGBV double"
",CutOffDate timestamp"
",Comments string"
",LGRecoveriesFlag boolean"
",HAPSAdjustment double"
",AdjustmentComments string"
",BusinessDate date"
)

# COMMAND ----------

# DBTITLE 1,CREATE AND DELETE TABLE
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

df_biteam_whl_glx_cs_haps_targets_per_borrower_vw = spark.sql(f'''
SELECT
  Portfolio
, BorrowerID
, DebtorCode
, HAPSPeriod
, HAPSTarget
, BorrowerName
, TAXID
, GroupID
, GroupName
, Unit
, LoanSales
, LGFlag
, `Asset Segment` as AssetSegment
, BPSegment
, BPApproach
, InitialGBV
, CutOffDate
, Comments
, cast({run_date!r} as date) as BusinessDate
FROM `{env}-cdh`.reference.biteam_whl_glx_cs_haps_targets_per_borrower
''')

df_biteam_whl_glx_cs_haps_targets_per_borrower_vw.createOrReplaceTempView("df_biteam_whl_glx_cs_haps_targets_per_borrower_vw")

# COMMAND ----------

df_non_lgrecoveries = spark.sql(f'''
select * 
from df_biteam_whl_glx_cs_haps_targets_per_borrower_vw 
where Comments not like '%LG recoveries%' or Comments is null
''')

df_non_lgrecoveries.createOrReplaceTempView("df_non_lgrecoveries")

# COMMAND ----------

df_lgrecoveries = spark.sql(f'''
select *
from df_biteam_whl_glx_cs_haps_targets_per_borrower_vw 
where Comments like '%LG recoveries%'
'''
)

df_lgrecoveries.createOrReplaceTempView("df_lgrecoveries")

# COMMAND ----------

df_adjustment_fields = spark.sql(f''' 

select n.*
,lg.HAPSTarget as HAPSAdjustment
,lg.Comments as AdjustmentComments
from df_non_lgrecoveries n 
inner join df_lgrecoveries lg
on  n.BorrowerID=lg.BorrowerID
and n.HAPSPeriod=lg.HAPSPeriod
''')

df_adjustment_fields.createOrReplaceTempView("df_adjustment_fields")

# COMMAND ----------

df_haps = spark.sql(f''' 

select a.*
,true as LGRecoveriesFlag
from df_adjustment_fields a     

union

select w.*
,null as HAPSAdjustment
,null as AdjustmentComments
,case   when w.Comments like '%LG recoveries%' then true
        else false 
end as LGRecoveriesFlag
from df_biteam_whl_glx_cs_haps_targets_per_borrower_vw w
left join df_adjustment_fields a
on a.BorrowerID=w.BorrowerID
and a.HAPSPeriod=w.HAPSPeriod
where a.BorrowerID is null
''')

df_haps.createOrReplaceTempView("df_haps")

# COMMAND ----------

final_df = spark.sql(f'''
select 
        sha2(concat_ws(";", BorrowerID, HAPSPeriod, "biteam"),256)                  as BPHAPSId
    ,   'biteam'                as SourceSystem
    ,   Portfolio               as SourcePortfolioName
    ,   CAST(from_utc_timestamp(current_timestamp(), 'Europe/Athens') AS TIMESTAMP) as IngestionTimestamp
    ,   cast(NUll as string)    as BatchId
    ,   concat(BorrowerID,';',HAPSPeriod) as BPHAPSKey
    ,   BorrowerID              as DebtorKey
    ,   DebtorCode              as CDI
    ,   HAPSPeriod
    ,   HAPSTarget
    ,   BorrowerName            as DebtorName
    ,   TAXID                   as TaxRegistrationNumber
    ,   GroupID                 as GroupCode
    ,   GroupName
    ,   Unit
    ,   LoanSales               as ExposureSales
    ,   cast(LGFlag as boolean) as LGFlag
    ,   AssetSegment
    ,   BPSegment
    ,   BPApproach
    ,   InitialGBV
    ,   CutOffDate
    ,   Comments
    ,   LGRecoveriesFlag
    ,   HAPSAdjustment
    ,   AdjustmentComments
    ,   BusinessDate
from df_haps
''')

final_df.createOrReplaceTempView("final_df")

# COMMAND ----------

if unity == "True":
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(derivative_table_path) # in order to write in delta_lake
else:
    final_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .save(derivative_table_path) # in order to write in delta_lake
