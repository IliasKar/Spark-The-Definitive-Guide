# Databricks notebook source
# MAGIC %run ./02-DS_utils/WAL_NPV_Calculation

# COMMAND ----------

Business_date = dbutils.widgets.get("BusinessDate")

# Import the module
from aws_utils.aws_utils import *

env = get_env_prefix()

table_name_cepalbp="CreditMemoEnhancementCepalBP"
table_name_investorbp="CreditMemoEnhancementInvestorBP"
target_area = f"`{env}-cdh-delivery`.`business-plan`"

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

# COMMAND ----------

# Define the CepalBP subquery
cepalbp_query = f"""
SELECT 
    CASE 
        WHEN a.PortfolioName = 'Cosmos Wholesale' THEN 35
        WHEN a.PortfolioName = 'Galaxy IV Funding DAC' THEN 21
        WHEN a.PortfolioName = 'Symbol' THEN 15
        WHEN a.PortfolioName = 'Neptune' THEN 16
        ELSE p.PortfolioId 
    END AS PortfolioId,
    a.PortfolioName,
    a.CDI AS `key`, 
    calendar.CurrentMonthLastWorkingDate AS `Date`,
    a.Month AS Month,
    a.businessDate AS `BusinessDate`, 
    SUM(a.GrossRecoveries) AS `Amount`,
    MAX(b.Duration) AS CepalBPDuration, 
    MAX(c.GBV) AS GBV
FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty a
LEFT JOIN (
    SELECT BusinessDate, CDI, MAX(datediff(Month, BusinessDate)) AS Duration
    FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty
    WHERE keytype='CDI' AND PortfolioName IN ('Symbol', 'Neptune')
    GROUP BY BusinessDate, CDI
) b ON a.CDI = b.CDI AND a.BusinessDate = b.BusinessDate
LEFT JOIN (
    SELECT BusinessDate, PortfolioName, cdi, SUM(gbv) AS GBV
    FROM `{env}-cdh`.derivative.dexposure
    GROUP BY BusinessDate, PortfolioName, cdi
) c ON a.CDI = c.CDI AND a.BusinessDate = c.BusinessDate
LEFT JOIN `{env}-cdh`.primaryactive.portfolio p 
    ON substring(a.CDI, 1, 3) = p.Prefix
LEFT JOIN  `{env}-cdh`.reference.ref_calendar calendar
    ON cast(concat(cast(substring(a.Month,1,4)  as string),'-', cast(substring(a.Month,6,2)  as string),'-','01') as date)=calendar.FirstDateOfMonth
WHERE keytype='CDI' AND a.BusinessDate = {Business_date!r}
GROUP BY  PortfolioId,
         a.PortfolioName,
         key,
         Date,
         a.BusinessDate,
         a.Month
"""

# Execute the CepalBP subquery
df_cepalbp = spark.sql(cepalbp_query)

# Display the result
display(df_cepalbp)

# COMMAND ----------

# %sql
# SELECT p.Portfolioid as `PortfolioId`,
#        a.CDI AS `key`,
#        a.businessDate AS `Date`,
#        SUM(a.GrossRecoveries) AS `Amount`
# FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty a
# LEFT JOIN (
#     SELECT a.BusinessDate,
#            CDI,
#            MAX(DATEDIFF(a.Month, a.BusinessDate)) AS Duration
#     FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty a
#     WHERE keytype = 'CDI'
#       AND a.PortfolioName IN ('Symbol', 'Neptune')
#     GROUP BY a.BusinessDate, CDI
# ) b ON a.CDI = b.CDI AND a.BusinessDate = b.BusinessDate
# LEFT JOIN (
#     SELECT a.BusinessDate,
#            a.PortfolioName,
#            a.cdi,
#            SUM(a.gbv) AS GBV
#     FROM `{env}-cdh`.derivative.dexposure a
#     GROUP BY a.BusinessDate, a.PortfolioName, a.cdi
# ) c ON a.CDI = c.CDI AND a.BusinessDate = c.BusinessDate
# LEFT JOIN (
#     SELECT *,
#            CASE WHEN d.CollectionType = 'Payments' THEN d.Amount END AS PaymentAmountPreonboarding,
#            CASE WHEN d.CollectionType = 'Liquidations' THEN d.Amount END AS LiquidationAmountPreonboarding,
#            CASE WHEN d.CollectionType = 'Disbursements' THEN d.Amount END AS DisbursementAmountPreonboarding,
#            CASE WHEN d.CollectionType = 'Payment from AB' THEN d.Amount END AS PaymentAmountFromABPreonboarding
#     FROM `{env}-cdh`.reference.collections_preonboarding d
# ) d ON a.cdi = d.DebtorCode AND REPLACE(d.DebtorCode, 'NULL', NULL) IS NOT NULL
# left join  `{env}-cdh`.primaryactive.portfolio p on           p.Prefix= substring(a.CDI,1,3)
# WHERE keytype = 'CDI'
#   AND a.PortfolioName IN ('Symbol', 'Neptune')
# --  AND a.BusinessDate = '2024-07-02'
# GROUP BY a.PortfolioName, a.CDI, a.businessDate, p.Portfolioid;

# COMMAND ----------

data_per_portfolio = df_cepalbp

# COMMAND ----------

irr = spark.sql(f"""
select * from `{env}-cdh`.reference.portfolio_discount_rate
""")

# COMMAND ----------

output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

from pyspark.sql import functions as F

# Combine all DataFrames in the output_table dictionary into a single DataFrame
combined_df = None
for portfolio_id, df in output_table.items():
    df = df.withColumn("PortfolioId", F.lit(portfolio_id))
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)

# Join the combined_df with df_cepalbp on PortfolioId
result_df_cepal = df_cepalbp.join(
    combined_df,
    on=['PortfolioId', 'key'],  # Join on both 'PortfolioId' and 'key'
    how='left'
)
# add the calculated column
result_df_cepal = result_df_cepal.withColumn("CepalBPNVP/GBV", F.try_divide(result_df_cepal.NPV , result_df_cepal.GBV))
# Display the result
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate for InvestorBP WAL and NPV

# COMMAND ----------

query_investorbp_haps = f"""
SELECT
    CASE 
        WHEN a.SourcePortfolioName = 'CosmosWHL' THEN 35
        WHEN a.SourcePortfolioName = 'GalaxyIV' THEN 21
        ELSE p.PortfolioId 
    END AS PortfolioId,
    CASE
        WHEN a.SourcePortfolioName = 'CosmosWHL' THEN 'Cosmos Wholesale'
        WHEN a.SourcePortfolioName = 'GalaxyIV' THEN 'Galaxy IV Funding DAC'
        ELSE a.SourcePortfolioName
    END AS PortfolioName,
    a.CDI as `key`,
    CAST(a.HAPSPeriod AS DATE) as `Date`,
    substring(cast(date (a.HAPSPeriod) as string),1,7) as Month,
    a.businessDate as `BusinessDate`,
    SUM(a.HAPSTarget) AS `Amount`,
    MAX(b.Duration) AS InvestorBPDuration,
    MAX(c.GBV) AS GBV
FROM
    `{env}-cdh`.derivative.DBPHAPS a
LEFT JOIN (
    SELECT
        BusinessDate,
        CDI,
        MAX(datediff(HAPSPeriod, BusinessDate)) AS Duration
    FROM
        `{env}-cdh`.derivative.DBPHAPS
    GROUP BY
        BusinessDate,
        CDI
) b ON a.CDI = b.CDI AND a.BusinessDate = b.BusinessDate
LEFT JOIN (
    SELECT
        BusinessDate,
        PortfolioName,
        cdi,
        SUM(gbv) AS GBV
    FROM
        `{env}-cdh`.derivative.dexposure
    GROUP BY
        BusinessDate,
        PortfolioName,
        cdi
) c ON a.CDI = c.CDI AND a.BusinessDate = c.BusinessDate
LEFT JOIN 
`{env}-cdh`.primaryactive.portfolio p 
    ON substring(a.CDI, 1, 3) = p.Prefix
WHERE
    a.BusinessDate = {Business_date!r}
    AND a.CDI IS NOT NULL
GROUP BY
    PortfolioId
    , a.SourcePortfolioName
    ,key
    ,Date
    ,a.businessDate
    ,substring(cast(date (a.HAPSPeriod) as string),1,7)
"""

# Execute the first query
df_investorbp_haps = spark.sql(query_investorbp_haps)

# COMMAND ----------

data_per_portfolio = df_investorbp_haps

# COMMAND ----------

irr = spark.sql ( f"""
select * from `{env}-cdh`.reference.portfolio_discount_rate
""")

# COMMAND ----------

output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# Combine all DataFrames in the output_table dictionary into a single DataFrame
combined_df = None
for portfolio_id, df in output_table.items():
    df = df.withColumn("PortfolioId", F.lit(portfolio_id))
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)

# Join the combined_df with df_cepalbp on PortfolioId
result_df_investorbp = df_investorbp_haps.join(
    combined_df,
    on=['PortfolioId', 'key'],  # Join on both 'PortfolioId' and 'key'
    how='left'
)
# add the calculated column
result_df_investorbp = result_df_investorbp.withColumn("InvestorBPNVP/GBV", F.try_divide( result_df_investorbp.NPV ,result_df_investorbp.GBV))
# Display  
 

# COMMAND ----------

result_df_investorbp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Second INVEstorBP for UNION

# COMMAND ----------

query_investorbp_initial = f"""
SELECT
    p.PortfolioId,
    a.PortfolioName,
    a.CDI as `key`,
    CAST(a.recoverydate AS DATE) AS `Date`,
    substring(cast(date (a.recoverydate) as string),1,7) as Month,
    a.businessDate as `BusinessDate`,
    SUM(a.recoveryAmount) AS `Amount`,
    MAX(b.Duration) AS InvestorBPDuration,
    MAX(c.GBV) AS GBV
FROM
    `{env}-cdh`.derivative.DBPCepalRecoveriesInitial a
LEFT JOIN (
    SELECT
        BusinessDate,
        CDI,
        MAX(datediff(recoveryDate, BusinessDate)) AS Duration
    FROM
        `{env}-cdh`.derivative.DBPCepalRecoveriesInitial
    GROUP BY
        BusinessDate,
        CDI
) b ON a.CDI = b.CDI AND a.BusinessDate = b.BusinessDate
LEFT JOIN (
    SELECT
        BusinessDate,
        PortfolioName,
        cdi,
        SUM(gbv) AS GBV
    FROM
        `{env}-cdh`.derivative.dexposure
    GROUP BY
        BusinessDate,
        PortfolioName,
        cdi
) c ON a.CDI = c.CDI AND a.BusinessDate = c.BusinessDate
left join `{env}-cdh`.primaryactive.portfolio as p 
on substring(a.CDI, 1, 3) = p.Prefix
WHERE
    a.BusinessDate = {Business_date!r}
    AND a.CDI IS NOT NULL
GROUP BY
    p.PortfolioId,
    a.PortfolioName,
    key,
    Date,
    a.businessDate,
    substring(cast(date (a.recoverydate) as string),1,7)
"""

# Execute the second query
df_investorbp_initial = spark.sql(query_investorbp_initial)

# COMMAND ----------

data_per_portfolio = df_investorbp_initial

# COMMAND ----------

irr = spark.sql( f"""
select * from `{env}-cdh`.reference.portfolio_discount_rate
""")

# COMMAND ----------

output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# Combine all DataFrames in the output_table dictionary into a single DataFrame
combined_df = None
for portfolio_id, df in output_table.items():
    df = df.withColumn("PortfolioId", F.lit(portfolio_id))
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)

# Join the combined_df with df_cepalbp on PortfolioId
result_df_investorbp_2nd = df_investorbp_initial.join(
    combined_df,
    on=['PortfolioId', 'key'],  # Join on both 'PortfolioId' and 'key'
    how='left'
)
# add the calculated column
result_df_investorbp_2nd = result_df_investorbp_2nd.withColumn("InvestorBPNVP/GBV", F.try_divide( result_df_investorbp_2nd.NPV, result_df_investorbp_2nd.GBV))
# Display the result
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final union and query

# COMMAND ----------

# Union the two DataFrames by column names
df_investorbp_combined = result_df_investorbp.unionByName(result_df_investorbp_2nd)

 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final query

# COMMAND ----------


# Select and coalesce the columns
result_df_cepal_final = result_df_cepal.select(
    result_df_cepal.PortfolioName.alias('PortfolioName'),
    result_df_cepal.key.alias('CDI'),
    result_df_cepal.GBV.alias('GBV'),
    result_df_cepal.Month.alias('YearMonth'),
    result_df_cepal.BusinessDate.alias('BusinessDate'),
    result_df_cepal.Amount.alias('GrossAmount'),
    result_df_cepal.CepalBPDuration.alias('Duration'),
    result_df_cepal.WAL.alias('AverageLife'),
    result_df_cepal.NPV.alias('NPV'),
    result_df_cepal["CepalBPNVP/GBV"].alias('NVP/GBV'),
)

# Create a temporary view from the DataFrame
result_df_cepal_final.createOrReplaceTempView("result_df_cepal_final")

# Define the final left join query
final_query = """
SELECT a.*, d.CollectionsPreOnboardingSecuritized, d.CollectionsPreOnboardingThrirdParties
,CASE WHEN a.PortfolioName like '%Cosmos%' THEN 1
      WHEN a.PortfolioName like '%Galaxy%' THEN 1
      ELSE 0
END AS IsPortfolioSecuritizedFlag
FROM result_df_cepal_final a
LEFT JOIN (
    SELECT DebtorCode, 
           SUM(CASE WHEN portfolio IN ('GalaxyIV', 'CosmosWHL') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
           SUM(CASE WHEN portfolio IN ('Hermes B') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThrirdParties
    FROM `qa-cdh`.reference.collections_preonboarding
    WHERE DebtorCode <> 'NULL'
    GROUP BY DebtorCode
) d ON a.CDI = d.DebtorCode
"""

# Execute the final left join query
final_df_cepalbp = spark.sql(final_query)

# Get current columns
current_columns = final_df_cepalbp.columns

# Move 'BusinessDate' to the last position
new_columns = [col for col in current_columns if col != "BusinessDate"] + ["BusinessDate"]

# Select the new column order
final_df_cepalbp = final_df_cepalbp.select(new_columns)

# Display the final DataFrame
display(final_df_cepalbp)

# COMMAND ----------

# Select and coalesce the columns
df_investorbp_combined_final = df_investorbp_combined.select(
    df_investorbp_combined.PortfolioName.alias('PortfolioName'),
    df_investorbp_combined.key.alias('CDI'),
    df_investorbp_combined.GBV.alias('GBV'),
    df_investorbp_combined.Month.alias('YearMonth'),
    df_investorbp_combined.BusinessDate.alias('BusinessDate'),
    df_investorbp_combined.Amount.alias('GrossAmount'),
    df_investorbp_combined.InvestorBPDuration.alias('Duration'),
    df_investorbp_combined.WAL.alias('AverageLife'),
    df_investorbp_combined.NPV.alias('NPV'),
    df_investorbp_combined["InvestorBPNVP/GBV"].alias('NVP/GBV')
)

# Create a temporary view from the DataFrame
df_investorbp_combined_final.createOrReplaceTempView("df_investorbp_combined_final")

# Define the final left join query
final_query = """
SELECT a.*, d.CollectionsPreOnboardingSecuritized, d.CollectionsPreOnboardingThrirdParties
,CASE WHEN a.PortfolioName like '%Cosmos%' THEN 1
      WHEN a.PortfolioName like '%Galaxy%' THEN 1
      ELSE 0
END AS IsPortfolioSecuritizedFlag
FROM df_investorbp_combined_final a
LEFT JOIN (
    SELECT DebtorCode, 
           SUM(CASE WHEN portfolio IN ('GalaxyIV', 'CosmosWHL') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
           SUM(CASE WHEN portfolio IN ('Hermes B') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThrirdParties
    FROM `qa-cdh`.reference.collections_preonboarding
    WHERE DebtorCode <> 'NULL'
    GROUP BY DebtorCode
) d ON a.CDI = d.DebtorCode
"""

# Execute the final left join query
final_df_investorbp = spark.sql(final_query)

# Get current columns
current_columns = final_df_investorbp.columns

# Move 'BusinessDate' to the last position
new_columns = [col for col in current_columns if col != "BusinessDate"] + ["BusinessDate"]

# Select the new column order
final_df_investorbp = final_df_investorbp.select(new_columns)

# Display the final DataFrame
display(final_df_investorbp)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

def process_and_save_result(result_df: DataFrame, curated_table_path: str, run_date: str) -> None:
    """
    Processes and saves the result dataframe to a Delta table.

    Parameters:
    - result_df: DataFrame to be saved
    - curated_table_path: Path to the Delta table
    - run_date: The run date for deletion criteria

    Returns:
    - None
    """
    # Initialize DeltaTable object for the given curated_table_path
    if DeltaTable.isDeltaTable(spark, curated_table_path):
        delta_table = DeltaTable.forPath(spark, curated_table_path)
        
        # Delete data for the specific run_date before inserting new data
        delta_table.delete(f"BusinessDate = '{run_date}'")


    result_df.write \
        .format("delta") \
        .partitionBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(curated_table_path) # Save using Delta Lake


# COMMAND ----------

# Path to the Delta table 
table_path = f"{target_area}.{table_name_cepalbp}"

# Call the function
process_and_save_result(final_df_cepalbp, table_path, Business_date)

# COMMAND ----------

# Path to the Delta table 
table_path = f"{target_area}.{table_name_investorbp}"

# Call the function
process_and_save_result(final_df_investorbp, table_path, Business_date)
