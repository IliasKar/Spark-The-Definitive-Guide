# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# DBTITLE 1,Import Functions
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
    try_divide,
)

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime

from pytz import timezone

from creditmemo_helpers import *

# COMMAND ----------

# DBTITLE 1,Define the specific BuisnessDate
# Define the specific BuisnessDate to run the notebook since no automated flow is available yet 
dbutils.widgets.text("BusinessDate", "")

# COMMAND ----------

# DBTITLE 1,Define securitized and third-party portfolios
# Define lists of securitized and third-party portfolios
securitized_portfolios_list = ["Galaxy IV Funding DAC", "Cosmos Wholesale", "Galaxy II Funding DAC", "Orion X Securitisation DAC", "Cosmos Retail", "Cosmos Common Customers", "DOMUS PORTFOLIO - RETAIL", "DOMUS PORTFOLIO - WHOLESALE", "GAIA II PORTFOLIO - D", "GAIA II PORTFOLIO RETAIL", "RHODIUM PORTFOLIO - WHOLESALE", "GAIA II PORTFOLIO WHOLESALE", "RHODIUM PORTFOLIO - D", "RHODIUM PORTFOLIO - RETAIL", "DOMUS PORTFOLIO - D", "GAIA I PORTFOLIO"]
thirdparties_portfolios_list = ["Hermes Portfolio B"]

# Convert lists to SQL-friendly string format
securitized_portfolios_sql = ", ".join(f"'{p}'" for p in securitized_portfolios_list)
thirdparties_portfolios_sql = ", ".join(f"'{p}'" for p in thirdparties_portfolios_list)

# COMMAND ----------

# DBTITLE 1,Def env and BusinessDate
 # Initialize timezone
athens_tz = timezone('Europe/Athens')

# Get Environment Prefix
env = get_env_prefix()

try:    
    BusinessDate = dbutils.widgets.get("BusinessDate")
    if BusinessDate is None or BusinessDate == "":
         # Get current date in Athens timezone
        run_date = datetime.now(athens_tz).strftime("%Y-%m-%d")  

        # Query for previous business day
        BusinessDate_df = spark.sql(f"""
            SELECT PreviousBusinessDay
            FROM `{env}-cdh`.reference.ref_calendar
            WHERE CalendarDate = '{run_date}'
        """)  

        rows = BusinessDate_df.collect()
        # Get the previous business day
        Business_date = rows[0].PreviousBusinessDay if rows else None
    else:
        # Parse the provided date to a date object
        Business_date = datetime.strptime(BusinessDate, "%Y-%m-%d").date()

    print("Business_date:", Business_date)

except Exception as e:
    print(f"exception {e}")
    raise ValueError("ERROR: BusinessDate is not a valid date.Add a value in the following format 'YYYY-MM-DD'")

# COMMAND ----------

# DBTITLE 1,Define Variable Values
table_name_investorbp="CreditMemoEnhancementInvestorBP"
target_area = f"`{env}-cdh-delivery`.`business-plan`"

checkpoint_path = f"`{env}-cdh-delivery`.`business-plan`.temp_investorbp_checkpoint"

# COMMAND ----------

# DBTITLE 1,Load Tables
# Calendar
calendar = spark.sql(f"""
    SELECT DISTINCT
        calendar.CurrentMonthLastWorkingDate,
        calendar.FirstDateOfMonth
    FROM `{env}-cdh`.reference.ref_calendar calendar
""")

calendar.createOrReplaceTempView("calendar")

# Portfolio
portfolio = spark.sql(f"""
    SELECT
        Prefix, 
        PortfolioId, 
        unit_code_profits,
        PortfolioName, 
        substring(AssignmentDate, 1, 7) AS PortfolioOnBoardingMonth,
        BusinessDate
    FROM `{env}-cdh`.primary.portfolio
    WHERE BusinessDate = '{Business_date}' 
""")

portfolio.createOrReplaceTempView("portfolio")

# DCollection
dcollection = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.derivative.dcollection
    WHERE BusinessDate = '{Business_date}'  
""")

dcollection.createOrReplaceTempView("dcollection")

# DExposure
# PK[09/07/2025] - High Balance perimeter added (Ref. AAAS-1125)
DExposure = spark.sql(f""" 
    SELECT d1.*, 1 AS Is_HighBalance
    FROM  `{env}-cdh`.derivative.dexposure d1
    WHERE d1.portfolioname IN ('Galaxy II Funding DAC', 'Orion X Securitisation DAC', 'Cosmos Retail')
    AND d1.IsHighBalanceFlag = true
    AND d1.BusinessDate = '{Business_date}' 
    UNION ALL
    SELECT d2.*, 0 AS Is_HighBalance
    FROM `{env}-cdh`.derivative.dexposure d2
    WHERE d2.portfolioname NOT IN ('Galaxy II Funding DAC', 'Orion X Securitisation DAC', 'Cosmos Retail')
    AND d2.BusinessDate = '{Business_date}'  
""")

DExposure.createOrReplaceTempView("DExposure")

# DExposureAll (including open and closed accounts)
# 1. Load the full exposures table
DExposure_full = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.derivative.dexposure
""")

# 2. Run function to get the exposures as of the business date
DExposureall = get_cm_exposures_by_business_date(DExposure_full, Business_date)
# 3. Register as a temp view for SQL queries
DExposureall.createOrReplaceTempView("DExposureall")

# portfolio discount rate
irr = spark.sql(f"""
select * from `{env}-cdh`.reference.portfolio_discount_rate
""")

# COMMAND ----------

# DBTITLE 1,Check Data Availability
check_data_availability([DExposure, dcollection], Business_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations for InvestorBP Securitized

# COMMAND ----------

# DBTITLE 1,Define InvestorBP Haps Securitized DF
# Define the InvestorBP Haps Securitized DF
df_investorbp_haps = spark.sql(f"""
WITH exposures_HB AS (
        SELECT DISTINCT de.CDI 
        FROM DExposure de 
        WHERE de.Is_HighBalance = 1
    ), 
    DBPHAPS AS (
            SELECT dbh.SourcePortfolioName
                , dbh.CDI
                , dbh.HAPSPeriod
                , dbh.businessDate
                , SUM(COALESCE(dbh.HAPSTarget, 0)) AS HAPSTargetSum
            FROM `{env}-cdh`.derivative.DBPHAPS dbh
            WHERE dbh.BusinessDate = '{Business_date}' 
                AND dbh.CDI IS NOT NULL
            GROUP BY ALL
            UNION
            SELECT h.spv AS SourcePortfolioName
                , h.primaryCDI AS CDI
                , h.refDate AS HAPSPeriod
                , '{Business_date}' AS businessDate
                , SUM(COALESCE(h.value, 0)) AS HAPSTargetSum
            FROM `{env}-ds`.reference.haps h
            INNER JOIN exposures_HB hb ON h.primaryCDI = hb.CDI 
            WHERE h.SPV IN ('Galaxy II Funding DAC', 'Orion X Securitisation DAC', 'Cosmos Securitisation DAC')
                AND h.primaryCDI IS NOT NULL
            GROUP BY ALL
            UNION
            SELECT 
            SourcePortfolioName,
            CDI,
            HAPSPeriod,
            '{Business_date}' AS businessDate,
            SUM(COALESCE(HAPSTarget, 0)) AS HAPSTargetSum
            FROM `{env}-cdh`.reference.haps_businessplan
            WHERE CDI <> '-'
            GROUP BY ALL    
    )
SELECT
    CASE 
        WHEN DBPHAPS.SourcePortfolioName = 'CosmosWHL' THEN 35
        WHEN DBPHAPS.SourcePortfolioName = 'GalaxyIV' THEN 21
        WHEN DBPHAPS.SourcePortfolioName = 'Orion X Securitisation DAC' THEN 19
        WHEN DBPHAPS.SourcePortfolioName = 'Galaxy II Funding DAC' THEN 20
        WHEN DBPHAPS.SourcePortfolioName = 'Cosmos Retail' THEN 34
        WHEN DBPHAPS.SourcePortfolioName = 'DOMUS PORTFOLIO - RETAIL' THEN 69
        WHEN DBPHAPS.SourcePortfolioName = 'DOMUS PORTFOLIO - WHOLESALE' THEN 70
        WHEN DBPHAPS.SourcePortfolioName = 'GAIA II PORTFOLIO - D' THEN 78
        WHEN DBPHAPS.SourcePortfolioName = 'GAIA II PORTFOLIO RETAIL' THEN 76
        WHEN DBPHAPS.SourcePortfolioName = 'RHODIUM PORTFOLIO - WHOLESALE' THEN 73
        WHEN DBPHAPS.SourcePortfolioName = 'GAIA II PORTFOLIO WHOLESALE' THEN 77
        WHEN DBPHAPS.SourcePortfolioName = 'RHODIUM PORTFOLIO - D' THEN 74
        WHEN DBPHAPS.SourcePortfolioName = 'RHODIUM PORTFOLIO - RETAIL' THEN 72
        WHEN DBPHAPS.SourcePortfolioName = 'DOMUS PORTFOLIO - D' THEN 71
        WHEN DBPHAPS.SourcePortfolioName = 'GAIA I PORTFOLIO' THEN 75
        ELSE portfolio.PortfolioId 
    END AS PortfolioId,
    CASE
        WHEN DBPHAPS.SourcePortfolioName = 'CosmosWHL' THEN 'Cosmos Wholesale'
        WHEN DBPHAPS.SourcePortfolioName = 'GalaxyIV' THEN 'Galaxy IV Funding DAC'
        WHEN DBPHAPS.SourcePortfolioName = 'Cosmos Securitisation DAC' THEN 'Cosmos Retail'
        ELSE DBPHAPS.SourcePortfolioName
    END AS PortfolioName,
    DBPHAPS.CDI as `key`,
    CAST(DBPHAPS.HAPSPeriod AS DATE) as `Date`,
    substring(cast(date (DBPHAPS.HAPSPeriod) as string),1,7) as Month,
    year(DBPHAPS.HAPSPeriod) as Year,
    DBPHAPS.businessDate as `BusinessDate`,
    DBPHAPS.HAPSTargetSum AS `Amount`,
    SUM(DBPHAPS.HAPSTargetSum) OVER (PARTITION BY DBPHAPS.CDI, year(DBPHAPS.HAPSPeriod)) AS YearlyHAPSTargetSum,
    a.MonthsLeft,
    MAX(gbv.GBV) AS GBV,
    collectionpostonboarding.portfolioname AS collectionpostonboarding_portfolioname,
    collectionpostonboardingcurrentyear.portfolioname AS collectionpostonboardingcurrentyear_portfolioname,
    collectionpostonboarding.AmountOfYear,
    collectionpostonboarding.YearActualDate,
    collectionpostonboardingcurrentyear.AmountOfCurrentYear,
    collectionpostonboardingcurrentyear.YearActualDate AS CurrentYearActualDate,
    collectionpreonboarding.CollectionsPreOnboardingSecuritized,
    collectionpreonboarding.CollectionsPreOnboardingThirdParties,
    CASE WHEN collectionpostonboardingcurrentyear.portfolioname IN ({securitized_portfolios_sql}) 
            THEN collectionpostonboardingcurrentyear.AmountOfCurrentYear
        END AS AmountOfCurrentYearSecuritized , 
    CASE WHEN collectionpostonboardingcurrentyear.portfolioname IN ({thirdparties_portfolios_sql}) 
            THEN collectionpostonboardingcurrentyear.AmountOfCurrentYear
        END AS AmountOfCurrentYearThirdParties    
FROM DBPHAPS
    LEFT JOIN (
            SELECT 
                BusinessDate
                , CDI
                , substring(HAPSPeriod, 1, 7) AS Month
                , CEIL(months_between(substring(HAPSPeriod, 1, 7), BusinessDate)) AS MonthsLeft
            FROM DBPHAPS
            GROUP BY BusinessDate, CDI, Month
            ) a ON DBPHAPS.CDI = a.CDI
        AND substring(DBPHAPS.HAPSPeriod, 1, 7) = a.Month
    LEFT JOIN (
            SELECT
                BusinessDate, PortfolioName, cdi, SUM(gbv) AS GBV
            FROM dexposure
            GROUP BY ALL
            ) gbv ON DBPHAPS.CDI = gbv.CDI
    LEFT JOIN portfolio
    ON substring(DBPHAPS.CDI, 1, 3) = portfolio.Prefix
    LEFT JOIN 
        (
            SELECT  
                dexposure.CDI, 
                year(dcollection.ActualDate) AS YearActualDate,
                dcollection.portfolioname,
                portfolio.PortfolioId,
                sum(dcollection.Amount) as AmountOfYear,
                dcollection.BusinessDate
            FROM dcollection
                INNER JOIN  portfolio
                    ON portfolio.unit_code_profits = dcollection.portfolioKey
                INNER JOIN dexposureall dexposure
                    ON dexposure.ExposureKey = dcollection.ExposureKey
            WHERE substring(dcollection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                AND year(dcollection.ActualDate) < YEAR(dcollection.BusinessDate)
                AND dcollection.FinalReason IN ('Matched', 'matched')
            GROUP BY ALL
        ) collectionpostonboarding ON a.cdi = collectionpostonboarding.CDI
        AND year(a.Month) <  year(collectionpostonboarding.BusinessDate)
    LEFT JOIN
        (
            SELECT 
                dexposure.CDI, 
                year(dcollection.ActualDate) AS YearActualDate,
                dcollection.portfolioname,
                portfolio.PortfolioId,
                sum(dcollection.Amount) as AmountOfCurrentYear,
                dcollection.BusinessDate        
            FROM dcollection
                INNER JOIN  portfolio
                    ON portfolio.unit_code_profits = dcollection.portfolioKey
                INNER JOIN dexposureall dexposure
                    ON dexposure.ExposureKey = dcollection.ExposureKey
            WHERE substring(dcollection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                AND year(dcollection.ActualDate) = YEAR(dcollection.BusinessDate)
                AND dcollection.FinalReason IN ('Matched', 'matched')
            GROUP BY ALL
        ) collectionpostonboardingcurrentyear ON a.cdi = collectionpostonboardingcurrentyear.cdi
        AND year(a.Month) <=  year(collectionpostonboardingcurrentyear.BusinessDate)
    LEFT JOIN (
            SELECT 
                cp.DebtorCode AS CDI, 
                SUM(CASE WHEN cp.portfolio IN ({securitized_portfolios_sql}) THEN cp.Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
                SUM(CASE WHEN cp.portfolio IN ({thirdparties_portfolios_sql}) THEN cp.Amount ELSE 0 END) AS CollectionsPreOnboardingThirdParties
            FROM `{env}-cdh`.reference.collections_preonboarding cp
                LEFT JOIN portfolio p ON substring(cp.DebtorCode, 1, 3) = p.Prefix
            WHERE   cp.DebtorCode is not null
            GROUP BY DebtorCode
            UNION
            SELECT 
                 e.CDI, 
                SUM(COALESCE(h.TRANSACTION_AMOUNT, 0)) AS CollectionsPreOnboardingSecuritized,
                0 AS CollectionsPreOnboardingThirdParties
            FROM `{env}-ds`.reference.dpo_post_migration_transactions_glx h
                INNER JOIN ( SELECT DISTINCT e1.CDI, e1.ExposureKey, e1.portfolioname
                    FROM  `{env}-cdh`.derivative.dexposure e1                      
                    WHERE e1.portfolioname IN ('Galaxy II Funding DAC', 'Orion X Securitisation DAC', 'Cosmos Retail')
                    AND e1.CDI IN (SELECT e2.CDI FROM exposures_HB e2 ) ) e ON h.ActiveAccount = e.ExposureKey -- PK[LegalActionPriority flag is not updated for all Accounts therefore we must fetch all Accounts for CDIs in High Balance perimeter]
                INNER JOIN portfolio p ON substring(h.ActiveAccount, 1, 3) = p.Prefix
                    AND e.portfolioname = p.PortfolioName
            WHERE   h.Month >= '2020-09-01'
                AND substring(h.Month, 1, 7) <= p.PortfolioOnBoardingMonth
            GROUP BY e.CDI            
        ) collectionpreonboarding ON DBPHAPS.CDI = collectionpreonboarding.CDI
GROUP BY ALL
""")

# COMMAND ----------

# DBTITLE 1,Process and Mark Business Plans for Current Year
# Extract the current year
current_year = df_investorbp_haps.select(F.year("businessdate").alias("current_year")).distinct().collect()[0]["current_year"]

# Filter keys that have business plans for the current year
# Use string formatting only once and avoid casting inside filter for every row
current_year_prefix = str(current_year)
df_curr_bp_exists = (df_investorbp_haps.filter(F.col("Month").substr(1, 4) == current_year_prefix).select("key").distinct())

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BP_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BP_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BP_CURR_YEAR column
df_investorbp_haps = (
    df_investorbp_haps
    .join(df_curr_bp_exists, df_investorbp_haps.key == df_curr_bp_exists.key_curr_bp_exists, "left")
    .withColumn("HAS_BP_CURR_YEAR", F.coalesce(F.col("HAS_BP_CURR_YEAR"), F.lit(0)))
    .drop("key_curr_bp_exists")
)

# Add MaxPreviousYear column for each key
df_max_previous_year = df_investorbp_haps.filter(F.col("Year") < current_year).groupBy("key").agg(F.max("Year").alias("MaxPreviousYear"))

# Join the main DataFrame with the MaxPreviousYear DataFrame
df_investorbp_haps = df_investorbp_haps.join(df_max_previous_year, "key", "left")

# COMMAND ----------

# DBTITLE 1,Calc SumOfAmountPreviousYears
# Group the df_investorbp_haps DataFrame by 'key' and 'Month', 
# and calculate the sum of 'AmountOfYear' for each group.
df_investorbp_haps_with_sum = df_investorbp_haps.groupBy("key", "Month").agg(
    F.sum("AmountOfYear").alias("SumOfAmountPreviousYears")  
).withColumnRenamed("Month", "sum_Month").withColumnRenamed("key", "sum_key")

# Perform an inner join between df_investorbp_haps and df_investorbp_haps_with_sum on 'key' and 'Month'
df_investorbp_haps = df_investorbp_haps.join(
    df_investorbp_haps_with_sum,
    (F.col("key") == F.col("sum_key")) & (F.col("Month") == F.col("sum_Month")),
    "inner"
).drop("sum_key", "sum_Month", "AmountOfYear", "YearActualDate"
       ).distinct()

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_investorbp_haps = df_investorbp_haps.withColumn(
    "CollectionAmountCalc",
    F.when(
        # If there is a business plan for the current year and the year is the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") == current_year),
        F.col("AmountOfCurrentYear")
    ).when(
        # If there is a business plan for the current year and the year is before the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is a business plan for the current year and the year is after the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") > current_year),
        F.lit(None)
    ).when(
        # If there is no business plan for the current year and the year is the MaxPreviousYear
        (F.col("HAS_BP_CURR_YEAR") == 0) & (F.col("Year") == F.col("MaxPreviousYear")),
        F.col("AmountOfCurrentYear") + F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is no business plan for the current year and the year is before the current year
        (F.col("HAS_BP_CURR_YEAR") == 0) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).otherwise(F.lit(None))  # For all other cases, set CollectionAmountCalc to None
)

# COMMAND ----------

# DBTITLE 1,Calc Total CollectionAmountforPostOnBoarding for PrevYears
# Prune to only needed columns for calculation
calc_cols = ["key", "Year", "AmountOfCurrentYear", "SumOfAmountPreviousYears"]
df_calc = df_investorbp_haps.select(*calc_cols).distinct()

# Window spec for lag
window_spec = Window.partitionBy("key").orderBy("Year")

# Calculate lag and CollectionAmountCalcTotal in one step
df_calc = (
    df_calc
    .withColumn("lag_SumOfAmountPreviousYears", F.lag("SumOfAmountPreviousYears", 1).over(window_spec))
    .withColumn(
        "CollectionAmountCalcTotal",
        F.when(
            F.col("Year") == current_year,
            F.coalesce(F.col("AmountOfCurrentYear"), F.lit(0)) +
            F.coalesce(F.col("lag_SumOfAmountPreviousYears"), F.lit(0))
        ).otherwise(F.coalesce(F.col("SumOfAmountPreviousYears"),F.lit(0)))
    )
    .select("key", "Year", "CollectionAmountCalcTotal")
)

# Join back to original DataFrame, keeping all columns and adding the new one
df_investorbp_haps = (
    df_investorbp_haps
    .join(df_calc, on=["key", "Year"], how="inner")
)

# COMMAND ----------

# DBTITLE 1,Calc CollectionsPostOnboarding
df_investorbp_haps = df_investorbp_haps.withColumn(
    "CollectionsPostOnboardingSecuritizedCalc",
    F.when(
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list),
        F.col("CollectionAmountCalc")
    ).otherwise(F.lit(None))
).withColumn(
    "CollectionsPostOnboardingThirdPartiesCalc",
    F.when(
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(thirdparties_portfolios_list),
        F.col("CollectionAmountCalc")
    ).otherwise(F.lit(None))
).withColumn(
    "CollectionsPostOnboardingSecuritizedCalcTotal",
    F.when( (F.col("Year") <= current_year) | 
            (F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list)),
        F.col("CollectionAmountCalcTotal")
    ).otherwise(F.lit(None))
).drop("CollectionAmountCalc", "CollectionAmountCalcTotal", "collectionpostonboarding_portfolioname", "collectionpostonboardingcurrentyear_portfolioname")

# COMMAND ----------

# DBTITLE 1,Calc YearlyAmount
df_investorbp_haps = df_investorbp_haps.withColumn(
    "YearlyAmount",
    F.coalesce(F.col("YearlyHAPSTargetSum"),F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingSecuritizedCalc"),F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingThirdPartiesCalc"),F.lit(0))
).drop("YearlyHAPSTargetSum")

# COMMAND ----------

# DBTITLE 1,Checkpoint 1
df_investorbp_haps = checkpoint_df(df_investorbp_haps, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Function "calculate_months_diff"
# Create the df for the function "calculate_months_diff"
df_investorbp_haps_yearly = df_investorbp_haps.select("PortfolioId", "key", "Date", "Year", "YearlyAmount", "BusinessDate").distinct()
df_investorbp_haps_yearly = df_investorbp_haps_yearly.filter(df_investorbp_haps_yearly.Date >= df_investorbp_haps_yearly.BusinessDate)

# Apply the function and select the necessary columns
df_investorbp_haps_yearly_data = calculate_months_diff(df_investorbp_haps_yearly)
df_investorbp_haps_yearly_data = df_investorbp_haps_yearly_data.select("PortfolioId", "key", "Year", "YearlyAmount", "BusinessDate", "MonthsDiff").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_NPV_per_year"
NPVyearly_data_investorbp_haps = calculate_NPV_per_year(df_investorbp_haps_yearly_data, irr)

# COMMAND ----------

# DBTITLE 1,Function "calculate_duration"
# Apply the function
npv_duration_data_investorbp_haps = calculate_duration(NPVyearly_data_investorbp_haps)

# Select the fileds needed
npv_duration_data_investorbp_haps = npv_duration_data_investorbp_haps.select("key", "PortfolioId", "Duration").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_WAL_NPV"
# Create the df for the function "calculate_WAL_NPV"
data_per_portfolio = df_investorbp_haps.filter(df_investorbp_haps.Date >= df_investorbp_haps.BusinessDate)

# Apply the function
df_WAL_NVP_final = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine ALL DFs
# Perform the joins, keeping all columns from df_investorbp_haps
result_df_investorbp = (
    df_investorbp_haps
    .join(df_WAL_NVP_final, on=['PortfolioId', 'key'], how='left')
    .join(npv_duration_data_investorbp_haps, on=['PortfolioId', 'key'], how='left')
)

# Register as temp view
result_df_investorbp.createOrReplaceTempView("result_df_investorbp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final query for InvestorBP (Securitized)

# COMMAND ----------

# DBTITLE 1,Checkpoint 2
result_df_investorbp = checkpoint_df(result_df_investorbp, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Final Query Part I
# Persist the input DataFrame if reused
df = result_df_investorbp 

# Window definitions
w_key_year = Window.partitionBy("key", "Year")
w_cdi_ordered_year = Window.partitionBy("CDI", "PortfolioName").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Feature engineering
business_year = F.expr("left(BusinessDate, 4)")

df_feat = (
    df
    .withColumn("TotalGrossAmount", F.sum("Amount").over(w_key_year))
    .withColumn("DurationCalc", F.when(F.col("Year") >= business_year, F.col("Duration")))
    .withColumn("TotalNPV", F.when(F.col("Year") >= business_year, F.col("NPV")))
    .withColumn("CollectionsPostOnboardingSecuritizedCalcFinal", F.col("CollectionsPostOnboardingSecuritizedCalc").cast("float"))
    .withColumn("CollectionsPostOnboardingSecuritizedCalcSum", F.col("CollectionsPostOnboardingSecuritizedCalcTotal").cast("float"))
    .withColumn("CollectionsPostOnboardingThirdPartiesCalcFinal", F.col("CollectionsPostOnboardingThirdPartiesCalc").cast("float"))
    .withColumn(
        "IsPortfolioSecuritizedFlag",
         F.when(F.col("PortfolioName").isin(securitized_portfolios_list), F.lit(1)) 
         .otherwise(0)
    )
)

# Column renaming for clarity
df_feat = (
    df_feat
    .withColumnRenamed("key", "CDI")
    .withColumnRenamed("WAL", "AverageLife")
)

# Select only the columns we want from the original DF plus the new columns
selected_columns = [
    "PortfolioName", "CDI", "GBV", "Year", 
    "TotalGrossAmount", "DurationCalc",  "AverageLife", "TotalNPV",   
     "CollectionsPreOnboardingSecuritized", "CollectionsPreOnboardingThirdParties",
    "CollectionsPostOnboardingSecuritizedCalcFinal", "CollectionsPostOnboardingSecuritizedCalcSum",
    "CollectionsPostOnboardingThirdPartiesCalcFinal", "IsPortfolioSecuritizedFlag", "BusinessDate",
]
df_final = df_feat.select(*selected_columns)

# Removes duplicates
df_grouped = df_final.dropDuplicates()

# Calculate GrossAmountCalc in a separate step
df_grouped = df_grouped.withColumn(
    "GrossAmountCalc",
    F.sum("TotalGrossAmount").over(w_cdi_ordered_year)
    - F.coalesce(F.col("CollectionsPreOnboardingSecuritized"), F.lit(0))
    - F.coalesce(F.col("CollectionsPostOnboardingSecuritizedCalcSum"), F.lit(0))
)

# Select RS columns and ensure unique per CDI/Year
rs_cols = [
    "PortfolioName", "CDI", "GBV", "Year", "GrossAmountCalc", "TotalGrossAmount", 
    "DurationCalc", "AverageLife", "TotalNPV",
    "CollectionsPreOnboardingSecuritized", "CollectionsPreOnboardingThirdParties",
    "CollectionsPostOnboardingSecuritizedCalcFinal", "CollectionsPostOnboardingSecuritizedCalcSum",
    "CollectionsPostOnboardingThirdPartiesCalcFinal", "IsPortfolioSecuritizedFlag", "BusinessDate"
]
rs = df_grouped.select(*rs_cols).dropDuplicates(["CDI", "Year", "PortfolioName"])

# COMMAND ----------

# DBTITLE 1,Final Query Part II Optimized
# Part 2:  Final Calculations and Cleanup

# Add BusinessYear column
rs_with_year = rs.withColumn("BusinessYear", F.year("BusinessDate"))

# Define window partitioned by CDI + PortfolioName and ordered by Year
w_cdi_year = Window.partitionBy("CDI", "PortfolioName").orderBy("Year")
w_cdi_ordered_year = Window.partitionBy("CDI", "PortfolioName").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate PrevGrossAmount for each row: last GrossAmountCalc where Year == BusinessYear, up to current row
rs_with_prev = rs_with_year.withColumn(
    "PrevGrossAmount",
    F.last(
        F.when(F.col("Year") == F.col("BusinessYear"), F.col("GrossAmountCalc")),
        ignorenulls=True
    ).over(w_cdi_year)
)

# Calculate TotalGrossAmountCalc: sum of TotalGrossAmount for rows where Year > BusinessYear, up to current row
rs_with_total = rs_with_prev.withColumn(
    "TotalGrossAmountCalc",
    F.sum(
        F.when(F.col("Year") > F.col("BusinessYear"), F.col("TotalGrossAmount"))
    ).over(w_cdi_ordered_year)
).withColumn(
    "TotalGrossAmountPrev",
    F.sum(
        F.when(F.col("Year") < F.col("BusinessYear"), F.col("TotalGrossAmount"))
    ).over(w_cdi_ordered_year)
).withColumn(
    "DistinctCollectionsPostOnboardingYTD",
    F.last(
        F.when(F.col("Year") == F.col("BusinessYear"), F.col("CollectionsPostOnboardingSecuritizedCalcSum")),
        ignorenulls=True
    ).over(w_cdi_year)
)

# Calculate GrossAmount and NPV using the distinct sum column
final = rs_with_total.withColumn(
    "GrossAmount",
    F.when(
        F.col("Year") >= F.col("BusinessYear"),
        F.coalesce(F.col("PrevGrossAmount"), F.lit(0)) +
        F.coalesce(F.col("TotalGrossAmountCalc"), F.lit(0))
    ).otherwise(F.col("GrossAmountCalc"))
).withColumn("IngestionTimestamp",lit(from_utc_timestamp(current_timestamp(), "Europe/Athens"))
).withColumn(
    "NPV",
     F.when(
        F.col("Year") >= F.col("BusinessYear"),
    (F.coalesce(F.col("TotalNPV"), F.lit(0)) + 
     F.coalesce(F.col("TotalGrossAmountPrev"), F.lit(0))) -
    (
        F.coalesce(F.col("CollectionsPreOnboardingSecuritized"), F.lit(0)) +
        F.coalesce(F.col("CollectionsPreOnboardingThirdParties"), F.lit(0))
    ) - 
    F.coalesce(F.col("DistinctCollectionsPostOnboardingYTD"),F.lit(0)) 
).otherwise(lit(None)))
final = final.withColumn("NPV/GBV", F.expr("try_divide(NPV, GBV)"))

# Select and order final columns
final_cols = [
    F.col("PortfolioName"),
    F.col("CDI"),
    F.col("GBV"),
    F.col("Year").cast("string").alias("Year"),
    F.col("GrossAmount"),
    F.col("TotalGrossAmount"),
    F.col("DurationCalc").alias("Duration"),
    F.col("AverageLife"),
    F.greatest(F.col("NPV"), lit(0)).alias("NPV"),
    F.col("TotalNPV"),
    F.col("NPV/GBV"),
    F.col("CollectionsPreOnboardingSecuritized"),
    F.col("CollectionsPreOnboardingThirdParties"),
    F.col("CollectionsPostOnboardingSecuritizedCalcFinal"),
    F.col("CollectionsPostOnboardingSecuritizedCalcSum"),
    F.col("CollectionsPostOnboardingThirdPartiesCalcFinal"),
    F.col("IsPortfolioSecuritizedFlag"),
    F.col("IngestionTimestamp"),
    F.col("BusinessDate"),
    F.col("BusinessYear")
]
final_df_investorbp = final.select(*final_cols)

# --------- Additional logic when NPV = 0  ----------
final_df_investorbp = final_df_investorbp.withColumn(
    "Duration",
    F.when(F.col("NPV") == 0, F.lit(0)).otherwise(F.coalesce(F.col("Duration"), F.lit(0)))
).withColumn(
    "AverageLife",
    F.when(F.col("NPV") == 0, F.lit(0)).otherwise(F.coalesce(F.col("AverageLife"), F.lit(0)))
).withColumn(
    "NPV/GBV",
    F.when(F.col("NPV") == 0, F.lit(0)).otherwise(F.coalesce(F.col("NPV/GBV"), F.lit(0)))
).withColumn(
    "NPV",
    F.when( (F.col("Year") >= F.col("BusinessYear")) & (F.col("TotalNPV") == 0) & (F.col("GrossAmount") > 0), 
           F.col("GrossAmount")).otherwise(F.col("NPV"))
).drop("BusinessYear")

# Move BusinessDate to the last position (if not already)
cols = [c for c in final_df_investorbp.columns if c != "BusinessDate"] + ["BusinessDate"]
final_df_investorbp = final_df_investorbp.select(cols)

# Fetch only distinct rows
final_df_investorbp = final_df_investorbp.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process and Save results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save results for InvestorBP table

# COMMAND ----------

# DBTITLE 1,Save_To_FinalTable
# Function to check if table exists in the catalog
def table_exists(table_name: str) -> bool:
  return spark.catalog.tableExists(table_name)

# Ensure table_path to Delta table is correctly formatted for investorbp
table_path = f"{target_area}.{table_name_investorbp}"

# Set table properties
table_properties = {
    'logRetentionDuration': 'interval 5 weeks',
    'deletedFileRetentionDuration': 'interval 5 weeks',
    'minReaderVersion': '3',
    'minWriterVersion': '7',
    'columnMapping.mode': 'name',
    'dataSkippingStatsColumns': 'BusinessDate'
}

# Check if table_path and final_df_investorbp exist and not empty
if table_path and final_df_investorbp and not final_df_investorbp.isEmpty():
    # Delete only if table exists
    if table_exists(table_path):
        spark.sql(f"""
            DELETE FROM {table_path}
            WHERE BusinessDate = '{Business_date}'
              AND IsPortfolioSecuritizedFlag = 1
        """)

    # Cast BusinessDate to Date to match Delta table schema
    final_df_investorbp = final_df_investorbp.withColumn(
        "BusinessDate", col("BusinessDate").cast("date")
    )
    
    # Append data to Delta table (creates table if it does not exist)
    final_df_investorbp.write \
        .format("delta") \
        .clusterBy("BusinessDate") \
        .mode("append") \
        .options(**{f"delta.{k}": v for k, v in table_properties.items()}) \
        .saveAsTable(table_path)
         
    print(f"Data appended successfully to {table_path}")

else:
    raise ValueError("Invalid table path or DataFrame")

#Drop temp table
spark.sql(f"DROP TABLE {checkpoint_path}")
