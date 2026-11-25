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
                      AND ExposureStatus in ("Overdue","Normal")
                      AND (SubProductDescription IS NULL 
                                OR SubProductDescription NOT IN ('ΥΠΟ ΑΙΡΕΣΗ ΔΙΑΓΡΑΦΗ - RET - ΡΥΘΜΙΣΕΙΣ'))
                      AND {portfolio_filter}
                      """)

dexposure.createOrReplaceTempView("dexposure")                  

paymentschedule = spark.sql(f"""
                            select * 
                            from `{env}-cdh`.derivative.dpaymentschedule
                            where BusinessDate = {run_date!r} 
                            and SourceSystem IN ('profits','qca')
                            AND {portfolio_filter}
                            """)

paymentschedule.createOrReplaceTempView("paymentschedule")

restructuring = spark.sql(f"""
                          select * 
                          from `{env}-cdh`.primary.restructuring
                          where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                          """)

restructuring.createOrReplaceTempView("restructuring")

product = spark.sql(f"""
                          select * 
                          from `{env}-cdh`.primary.product
                          where BusinessDate = {run_date!r}
                          and ProductDescription NOT IN ('ΥΠΟ ΑΙΡΕΣΗ ΔΙΑΓΡΑΦΗ - RET - ΡΥΘΜΙΣΕΙΣ')
                          """)

product.createOrReplaceTempView("product")

# COMMAND ----------

# ##Installments

# COMMAND ----------

Installments = spark.sql(f"""
SELECT distinct
pay.BusinessDate as ReportingDate,
pay.PortfolioName,
pay.DPaymentScheduleKey, 
concat(res.NewAgreementKey,' | ',res.restructuringdate) AS PaymentAgreementID,
cast(pay.InstallmentDate as date) as InstallmentDate,
pay.InstallmentAmount,
CASE WHEN pay.InstallmentAmountPaid > 0 AND 
DATEADD(DAY, pay.OverdueDays, pay.InstallmentDate) <= pay.BusinessDate
THEN CAST(DATEADD(DAY, pay.OverdueDays, pay.InstallmentDate) as date)
END AS InstallmentPaidDate, 
CASE WHEN pay.InstallmentAmountPaid > 0 
AND DATEADD(DAY, pay.OverdueDays, pay.InstallmentDate) <= pay.BusinessDate
THEN pay.InstallmentAmountPaid
END AS InstallmentAmountPaid, 
CASE WHEN pay.InstallmentAmountPaid > 0 
AND DATEADD(DAY, pay.OverdueDays, pay.InstallmentDate) <= pay.BusinessDate 
THEN 'Y' 
ELSE 'N' 
END AS InstallmentPaid, 
case when dex.ExposureRestructureType <> 'Conditional Woff' and dex.ExposureRestructureStatus =  'Active'
then  dex.ExposureRestructureOrigination END AS ExposureRestructureOrigination,  
dex.ExposureKey,
case when pay.portfolioname='EUCLID' and dex.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
when pay.portfolioname='EUCLID' and dex.SubServicer is null then 'Cepal'
else null end as SubServicerFlag,
dex.IsHighBalanceFlag as HighBalanceFlag
FROM paymentschedule pay
left join dexposure dex
ON pay.ExposureKey = dex.ExposureKey
inner join product pro
ON pro.ProductCode = dex.ProductCode
left join restructuring res
on res.NewExposurekey = dex.Exposurekey
""")

target_table_path_Installments = f"{target_area}.Installments"

if spark.catalog.tableExists(target_table_path_Installments):
    delete_from_unity_table(
    spark,
    target_table_path_Installments,
    run_date,
    portfolio_filter,
)

Installments = Installments.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

Installments.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_Installments) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_Installments} SYNC METADATA")
