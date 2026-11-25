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
securitized_portfolios_list = ["Galaxy IV Funding DAC", "Cosmos Wholesale", "Galaxy II Funding DAC", "Orion X Securitisation DAC", "Cosmos Retail"]
thirdparties_portfolios_list = ["Hermes Portfolio B", "Symbol", "Neptune", "Cetus"] 

# Convert lists to SQL-friendly string format
securitized_portfolios_sql = ", ".join(f"'{p}'" for p in securitized_portfolios_list)
thirdparties_portfolios_sql = ", ".join(f"'{p}'" for p in thirdparties_portfolios_list)

# COMMAND ----------

# DBTITLE 1,Def env and Business
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
        # print("SQL query result:", rows) KEEP ONLY FOR TEST
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
table_name_cepalbp="CreditMemoEnhancementCepalBP"
target_area = f"`{env}-cdh-delivery`.`business-plan`"

checkpoint_path = f"`{env}-cdh-delivery`.`business-plan`.temp_cepalbp_checkpoint"

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
    WHERE BusinessDate =  '{Business_date}'  
""")

dcollection.createOrReplaceTempView("dcollection")

# DExposure
# PK[01/07/2025] - High Balance perimeter added (Ref. AAAS-1125)
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
# MAGIC ### Calculations for CepalBP

# COMMAND ----------

# DBTITLE 1,Define CepalBP DF
# Define the CepalBP subquery
df_cepalbp = spark.sql(f"""
WITH latestbusinessplan AS (
        SELECT max(crp1.BusinessPlan) as BusinessPlan, crp1.CDI 
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty crp1
        WHERE crp1.BusinessDate = '{Business_date}'
        GROUP BY crp1.CDI
        UNION
        SELECT max(crp2.BusinessPlan) as BusinessPlan, crp2.AgreementKey AS CDI
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty crp2
        WHERE crp2.BusinessDate = '{Business_date}'  
        GROUP BY crp2.AgreementKey
    ),
    exposures_HB AS (
        SELECT DISTINCT de.CDI, de.AgreementKey
        FROM DExposure de 
        WHERE de.Is_HighBalance = 1
    ),
    DBPCepalRecoveriesParty AS (
        SELECT crp.PortfolioName
            , crp.CDI
            , crp.Month
            , crp.BusinessDate
            , SUM(COALESCE(crp.GrossRecoveries, 0)) AS GrossRecoveriesSum
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty crp
        INNER JOIN latestbusinessplan --keep the latest business plan
            ON crp.CDI = latestbusinessplan.CDI 
                AND crp.BusinessPlan = latestbusinessplan.BusinessPlan
        WHERE crp.BusinessDate = '{Business_date}' 
            AND crp.keytype='CDI'
        GROUP BY ALL
        UNION
        SELECT DISTINCT 
              crp_b.PortfolioName
            , d.CDI
            , crp_b.Month
            , crp_b.BusinessDate
            ,SUM(COALESCE(crp_b.GrossRecoveries, 0)) AS GrossRecoveriesSum
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty crp_b
        INNER JOIN latestbusinessplan --keep the latest business plan
            ON crp_b.AgreementKey = latestbusinessplan.CDI 
                AND crp_b.BusinessPlan = latestbusinessplan.BusinessPlan
        INNER JOIN exposures_HB d ON d.AgreementKey = crp_b.AgreementKey
        WHERE crp_b.BusinessDate = '{Business_date}' 
            AND crp_b.keytype='Contract'
        GROUP BY ALL 
    )
    SELECT
        CASE 
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Cosmos Wholesale' THEN 35
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Galaxy IV Funding DAC' THEN 21
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Symbol' THEN 15
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Neptune' THEN 16
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Cetus' THEN 56
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Orion X Securitisation DAC' THEN 19
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Galaxy II Funding DAC' THEN 20
            WHEN DBPCepalRecoveriesParty.PortfolioName = 'Cosmos Retail' THEN 34
            ELSE collectionpostonboarding.PortfolioId 
        END AS PortfolioId,
        DBPCepalRecoveriesParty.PortfolioName,
        DBPCepalRecoveriesParty.CDI AS `key`, 
        calendar.CurrentMonthLastWorkingDate AS `Date`,
        DBPCepalRecoveriesParty.Month,
        substring(DBPCepalRecoveriesParty.Month, 1, 4) as Year,
        DBPCepalRecoveriesParty.BusinessDate, 
        DBPCepalRecoveriesParty.GrossRecoveriesSum AS `Amount`,
        SUM(DBPCepalRecoveriesParty.GrossRecoveriesSum) OVER (PARTITION BY DBPCepalRecoveriesParty.CDI, YEAR(DBPCepalRecoveriesParty.Month)) AS YearlyGrossRecoveries,
        SUM(DBPCepalRecoveriesParty.GrossRecoveriesSum * MonthsLeft) AS AmountMonthsLeft, 
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
            END AS AmountOfCurrentYearThirdParties ,
        crp.MonthsLeft 
    FROM DBPCepalRecoveriesParty       
        LEFT JOIN (
                SELECT 
                    d1.Month, 
                    d1.CDI, 
                    d1.BusinessDate,
                    CEIL(months_between(Month, d1.BusinessDate)) AS MonthsLeft,
                    RecoveriesInitial.InitialMonthPlan
                FROM DBPCepalRecoveriesParty d1
                INNER JOIN (SELECT d2.CDI, MIN(Month)  InitialMonthPlan, d2.BusinessDate 
                            FROM DBPCepalRecoveriesParty d2
                            WHERE d2.BusinessDate = '{Business_date}'
                            GROUP BY d2.CDI, d2.BusinessDate) AS RecoveriesInitial ON d1.CDI = RecoveriesInitial.CDI
                                                                AND d1.BusinessDate = RecoveriesInitial.BusinessDate
                GROUP BY ALL
            ) crp
        ON crp.CDI = DBPCepalRecoveriesParty.CDI
            AND crp.Month = DBPCepalRecoveriesParty.Month
        LEFT JOIN (
                SELECT BusinessDate, PortfolioName, cdi, SUM(gbv) AS GBV
                FROM dexposure
                GROUP BY ALL
                ) gbv 
        ON DBPCepalRecoveriesParty.CDI = gbv.CDI
        LEFT JOIN
            (
            SELECT 
                    dexposure.CDI, 
                    year(dcollection.ActualDate) AS YearActualDate,
                    substring(dcollection.ActualDate, 1, 7) AS ActualMonthCollections,
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
            ) collectionpostonboarding
        ON crp.cdi = collectionpostonboarding.cdi
            AND year(crp.Month) < year(collectionpostonboarding.BusinessDate)
            AND crp.InitialMonthPlan <=  collectionpostonboarding.ActualMonthCollections                     -- PK[13/06/2026] Due to AAAS-1112. Exclude collections existing before the beginning of Plan.
            AND collectionpostonboarding.ActualMonthCollections = crp.Month
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
            ) collectionpostonboardingcurrentyear
        ON crp.cdi = collectionpostonboardingcurrentyear.cdi
            AND year(crp.Month) <= year(collectionpostonboardingcurrentyear.BusinessDate)
        LEFT JOIN calendar
        ON cast(CONCAT(substring(DBPCepalRecoveriesParty.Month,1,7),'-01') as date) = calendar.FirstDateOfMonth
        LEFT JOIN (
            SELECT 
                DebtorCode,
                SUM(CASE WHEN portfolio IN ('GalaxyIV', 'CosmosWHL') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
                SUM(CASE WHEN portfolio IN ('Hermes B') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThirdParties
            FROM `{env}-cdh`.reference.collections_preonboarding
            LEFT JOIN portfolio 
            ON substring(collections_preonboarding.DebtorCode, 1, 3) = portfolio.Prefix
            WHERE DebtorCode <> 'NULL' 
                AND portfolio in ('GalaxyIV','CosmosWHL','Hermes B')
                AND substring(collections_preonboarding.CollectionPeriod, 1, 7) <= portfolio.PortfolioOnBoardingMonth
            GROUP BY DebtorCode
            ) collectionpreonboarding 
        ON DBPCepalRecoveriesParty.CDI = collectionpreonboarding.DebtorCode

    GROUP BY ALL
""")

# COMMAND ----------

# DBTITLE 1,Process and Mark Business Plans for Current Year
# Extract the current year
current_year = df_cepalbp.select(F.year("businessdate").alias("current_year")).distinct().collect()[0]["current_year"]

# Filter keys that have business plans for the current year
# Use string formatting only once and avoid casting inside filter for every row
current_year_prefix = str(current_year)
df_curr_bp_exists = (df_cepalbp.filter(F.col("Month").substr(1, 4) == current_year_prefix).select("key").distinct())

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BP_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BP_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BP_CURR_YEAR column
df_cepalbp = (
    df_cepalbp
    .join(df_curr_bp_exists, df_cepalbp.key == df_curr_bp_exists.key_curr_bp_exists, "left")
    .withColumn("HAS_BP_CURR_YEAR", F.coalesce(F.col("HAS_BP_CURR_YEAR"), F.lit(0)))
    .drop("key_curr_bp_exists")
)

# Add MaxPreviousYear column for each key
df_max_previous_year = df_cepalbp.filter(F.col("Year") < current_year).groupBy("key").agg(F.max("Year").alias("MaxPreviousYear"))

# Join the main DataFrame with the MaxPreviousYear DataFrame
df_cepalbp = df_cepalbp.join(df_max_previous_year, "key", "left")

# COMMAND ----------

# DBTITLE 1,Calc SumOfAmountPreviousYears

# Create a new column that takes YearActualDate if not null, else year from Month
df_cepalbp = df_cepalbp.withColumn(
    "YearActualDate",
    F.when(F.col("YearActualDate").isNull(), F.year(F.col("Month"))).otherwise(F.col("YearActualDate"))
)

# Calculate SumOfAmountPreviousYears grouped by key, Month, and YearActualDate
df_sum = df_cepalbp.groupBy("key", "Month", "YearActualDate").agg(
    F.sum("AmountOfYear").alias("SumOfAmountPreviousYears")
)

# Calculate total sum of SumOfAmountPreviousYears grouped by key and YearActualDate
df_total_sum_filtered = df_sum.groupBy("key", "YearActualDate").agg(
    F.sum("SumOfAmountPreviousYears").alias("TotalSumOfAmountPreviousYears")
).filter(F.col("TotalSumOfAmountPreviousYears").isNotNull())

# Join SumOfAmountPreviousYears back to the original DataFrame on key, Month, YearActualDate
df_with_sum = df_cepalbp.join(
    df_sum,
    on=["key", "Month", "YearActualDate"],
    how="left"
)

# Join TotalSumOfAmountPreviousYears back to df_with_sum on key and YearActualDate
df_cepalbp = df_with_sum.join(
    df_total_sum_filtered,
    on=["key", "YearActualDate"],
    how="left"
).drop("AmountOfYear", "YearActualDate").distinct()

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_cepalbp = df_cepalbp.withColumn(
    "CollectionAmountCalc",
    F.when(
        # If there is a business plan for the current year and the year is the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") == current_year),
        F.col("AmountOfCurrentYear")
    ).when(
        # If there is a business plan for the current year and the year is before the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") < current_year),
        F.col("TotalSumOfAmountPreviousYears")
    ).when(
        # If there is a business plan for the current year and the year is after the current year
        (F.col("HAS_BP_CURR_YEAR") == 1) & (F.col("Year") > current_year),
        F.lit(None)
        ).when(
        # If there is no business plan for the current year and the year is the MaxPreviousYear
        (F.col("HAS_BP_CURR_YEAR") == 0) & (F.col("Year") == F.col("MaxPreviousYear")),
        F.col("AmountOfCurrentYear") + F.col("TotalSumOfAmountPreviousYears")
    ).when(
        # If there is no business plan for the current year and the year is before the current year
        (F.col("HAS_BP_CURR_YEAR") == 0) & (F.col("Year") < current_year),
        F.col("TotalSumOfAmountPreviousYears")
    ).otherwise(F.lit(None))  # For all other cases, set CollectionAmountCalc to None
)

# COMMAND ----------

# DBTITLE 1,Calc Total CollectionAmountforPostOnBoarding for PrevYears
# Select needed columns and remove duplicates if any
calc_cols = ["key", "Year", "AmountOfCurrentYear", "TotalSumOfAmountPreviousYears"]
df_calc = df_cepalbp.select(*calc_cols).distinct()

# Define window for cumulative sum partitioned by key ordered by Year up to current row
window_spec = Window.partitionBy("key").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Compute running cumulative sum of TotalSumOfAmountPreviousYears per key ordered by Year
df_calc = df_calc.withColumn(
    "CumulativeAmountPrevYears",
    F.sum(F.coalesce(F.col("TotalSumOfAmountPreviousYears"), F.lit(0))).over(window_spec)
)

# Final column with conditional logic based on Year comparison to current_year
df_calc_collections = df_calc.withColumn(
    "CollectionAmountCalcTotalAll",
    F.when(
        F.col("Year") == current_year,
        F.coalesce(F.col("AmountOfCurrentYear"), F.lit(0)) +
        F.coalesce(F.col("CumulativeAmountPrevYears"), F.lit(0)) # cumulative sum for current_year
    ).otherwise(F.coalesce(F.col("TotalSumOfAmountPreviousYears"), F.lit(0))  # otherwise use TotalSumOfAmountPreviousYears
    )
).select("key", "Year", "CollectionAmountCalcTotalAll", "TotalSumOfAmountPreviousYears")

# Remove duplicates by aggregating with max CollectionAmountCalcTotal per key and Year
df_calc_unique = df_calc_collections.groupBy("key", "Year").agg(
    F.max("CollectionAmountCalcTotalAll").alias("CollectionAmountCalcTotal")
).drop("CollectionAmountCalcTotalAll", "CumulativeAmountPrevYears")

# Join back to original DataFrame, keeping all columns and adding the new one
df_cepalbp = (
    df_cepalbp
    .join(df_calc_unique, on=["key", "Year"], how="inner")
    .dropDuplicates()  # Remove duplicates
)

# COMMAND ----------

# DBTITLE 1,Calc CollectionsPostOnboarding
df_cepalbp = df_cepalbp.withColumn(
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
    F.when(
    (F.col("Year") <= current_year) | 
    (F.coalesce(
        F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list)),
        F.col("CollectionAmountCalcTotal")
   ).otherwise(F.lit(None))
).drop("CollectionAmountCalc", "CollectionAmountCalcTotal", "collectionpostonboarding_portfolioname", "collectionpostonboardingcurrentyear_portfolioname")

# COMMAND ----------

# DBTITLE 1,Calc YearlyAmount
df_cepalbp = df_cepalbp.withColumn(
    "YearlyAmount",
    F.coalesce(F.col("YearlyGrossRecoveries"),F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingSecuritizedCalc"),F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingThirdPartiesCalc"),F.lit(0))
)

# COMMAND ----------

# DBTITLE 1,Checkpoint 1
df_cepalbp = checkpoint_df(df_cepalbp, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Function "calculate_months_diff"
# Create the df for the function "calculate_months_diff"
df_cepalbp_yearly = df_cepalbp.select("PortfolioId", "key", "Date", "Year", "YearlyAmount", "BusinessDate").distinct()
df_cepalbp_yearly = df_cepalbp_yearly.filter(df_cepalbp_yearly.Date >= df_cepalbp_yearly.BusinessDate)

# Apply the function and select the necessary columns
df_cepalbp_yearly_data = calculate_months_diff(df_cepalbp_yearly)
df_cepalbp_yearly_data = df_cepalbp_yearly_data.select("PortfolioId", "key", "Year", "YearlyAmount", "BusinessDate", "MonthsDiff").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_NPV_per_year"
# Apply the function
NPVyearly_data_cepalbp = calculate_NPV_per_year(df_cepalbp_yearly_data, irr)

# COMMAND ----------

# DBTITLE 1,Function "calculate_duration"
# Apply the function
npv_duration_data = calculate_duration(NPVyearly_data_cepalbp)

# Select the fileds needed
npv_duration_data = npv_duration_data.select("key", "PortfolioId", "Duration").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_WAL_NPV"
# Create the df for the function "calculate_WAL_NPV"
data_per_portfolio = df_cepalbp.filter(df_cepalbp.Date >= df_cepalbp.BusinessDate)

# Apply the function
df_WAL_NVP_final = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine ALL DFs Optimized
# Perform the joins, keeping all columns from df_investorbp_haps
result_df_cepal = (
    df_cepalbp
    .join(df_WAL_NVP_final, on=['PortfolioId', 'key'], how='left')
    .join(npv_duration_data, on=['PortfolioId', 'key'], how='left')
)

result_df_cepal.createOrReplaceTempView("result_df_cepal")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final query for CepalBP

# COMMAND ----------

# DBTITLE 1,Checkpoint 2
result_df_cepal = checkpoint_df(result_df_cepal, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Final Query Part I Optimized
# Persist the input DataFrame if reused
df = result_df_cepal 

# Window definitions
w_key_year = Window.partitionBy("key", "Year")
w_cdi_ordered_year = Window.partitionBy("CDI", "PortfolioName").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Aggregate Amount per key (CDI), PortfolioName and Year to get unique TotalGrossAmount per year
df_agg = (
    df.groupBy("key", "PortfolioName", "Year")
      .agg(F.sum("Amount").alias("TotalGrossAmount"))
      .withColumnRenamed("key", "CDI")
)

# Calculate cumulative TotalGrossAmount per CDI + PortfolioName ordered by Year
df_agg = df_agg.withColumn(
    "TotalGrossAmountCumulative", F.sum("TotalGrossAmount").over(w_cdi_ordered_year)
)

# Feature engineering
business_year = F.expr("left(BusinessDate, 4)")
business_month = F.expr("left(BusinessDate, 7)")

# Column renaming for clarity
df_feat = (
    df
    .withColumnRenamed("key", "CDI")
    .withColumnRenamed("WAL", "AverageLife")
    .withColumn("DurationCalc", F.when(F.col("Year") >= business_year, F.col("Duration")))
    .withColumn("TotalNPV", F.when(F.col("Year") >= business_year, F.col("NPV")))
    .withColumn("CollectionsPostOnboardingSecuritizedCalcFinal", F.col("CollectionsPostOnboardingSecuritizedCalc").cast("float"))
    .withColumn("CollectionsPostOnboardingSecuritizedCalcSum", F.col("CollectionsPostOnboardingSecuritizedCalcTotal").cast("float"))
    .withColumn("CollectionsPostOnboardingThirdPartiesCalcFinal", F.col("CollectionsPostOnboardingThirdPartiesCalc").cast("float"))
    .withColumn("IsPortfolioSecuritizedFlag", F.lit(0))
)

# Join the cumulative results back to the features DataFrame
df_joined = df_feat.join(df_agg, on=["CDI", "PortfolioName", "Year"], how="left")

# Select only the columns we want from the original DF plus the new columns
selected_columns = [
    "PortfolioName", "CDI", "GBV", "Year", 
    "TotalGrossAmount", "TotalGrossAmountCumulative", "DurationCalc",  "AverageLife", "TotalNPV", 
    "CollectionsPreOnboardingSecuritized", "CollectionsPreOnboardingThirdParties",
    "CollectionsPostOnboardingSecuritizedCalcFinal", "CollectionsPostOnboardingSecuritizedCalcSum",
    "CollectionsPostOnboardingThirdPartiesCalcFinal", "IsPortfolioSecuritizedFlag", "BusinessDate",
]
df_final = df_joined.select(*[c for c in selected_columns if c in df_joined.columns])

# Remove duplicates per CDI/Year/Portfolio
df_final = df_final.dropDuplicates(["CDI", "Year", "PortfolioName"])

# Calculate GrossAmountCalc as cumulative minus collections
df_final = df_final.withColumn(
    "GrossAmountCalc",
     F.when(
        F.col("Year") <= business_year,
        F.col("TotalGrossAmountCumulative") - 
        F.coalesce(F.col("CollectionsPostOnboardingSecuritizedCalcSum"), F.lit(0))
     ).otherwise(F.lit(0))
)

# Select RS columns and ensure unique per CDI/Year
rs_cols = [
    "PortfolioName", "CDI", "GBV", "Year", "GrossAmountCalc", "TotalGrossAmount", "TotalGrossAmountCumulative", 
    "DurationCalc", "AverageLife", "TotalNPV",
    "CollectionsPreOnboardingSecuritized", "CollectionsPreOnboardingThirdParties",
    "CollectionsPostOnboardingSecuritizedCalcFinal", "CollectionsPostOnboardingSecuritizedCalcSum",
    "CollectionsPostOnboardingThirdPartiesCalcFinal", "IsPortfolioSecuritizedFlag", "BusinessDate"
]
rs = df_final.select(*[c for c in rs_cols if c in df_final.columns]).dropDuplicates(["CDI", "Year", "PortfolioName"])

# COMMAND ----------

# DBTITLE 1,Final Query Part II Optimized
# Part 2:  Final Calculations and Cleanup

# Add BusinessYear column
rs_with_year = rs.withColumn("BusinessYear", F.year("BusinessDate"))

# Define window partitioned by CDI + PortfolioName and ordered by Year
w_cdi_ordered_year = Window.partitionBy("CDI", "PortfolioName").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate previous year's GrossAmountCalc (PrevGrossAmount) , TotalGrossAmount
rs_with_prev = rs_with_year.withColumn(
    "CurrentYearGross",
    F.max(F.when(F.col("Year") == F.col("BusinessYear"), F.col("GrossAmountCalc"))).over(w_cdi_ordered_year)
).withColumn(
    "PrevGrossAmount",    
        F.when(F.col("Year") >= F.col("BusinessYear"), F.col("CurrentYearGross")).otherwise(F.lit(0))
).withColumn(
    "TotalGrossAmountCalc",
    F.sum(
        F.when(F.col("Year") > F.col("BusinessYear"), F.col("TotalGrossAmount"))
    ).over(w_cdi_ordered_year)
).withColumn(
    "TotalGrossAmountPrevSum",
    F.sum(
        F.when(F.col("Year") < F.col("BusinessYear"), F.col("TotalGrossAmount"))
    ).over(w_cdi_ordered_year)
).withColumn(
    "DistinctCollectionsPostOnboardingYTD",
     F.when(F.col("Year") >= F.col("BusinessYear"),
        F.last(
            F.col("CollectionsPostOnboardingSecuritizedCalcSum"),
            ignorenulls=True
        ).over(w_cdi_ordered_year))  
).drop("CurrentYearGross")

# Calculate GrossAmount , NPV and NPV/GBV
final = rs_with_prev.withColumn(
    "GrossAmount",
    F.when(
        F.col("Year") > F.col("BusinessYear"),
        (F.coalesce(F.col("PrevGrossAmount"), F.lit(0)) +
        F.coalesce(F.col("TotalGrossAmountCalc"), F.lit(0))) 
    ).otherwise(F.col("GrossAmountCalc"))
).withColumn(
        "NPV",
        F.when(
            F.col("Year") >= F.col("BusinessYear"),
        (F.coalesce(F.col("TotalNPV"), F.lit(0)) + 
        F.coalesce(F.col("TotalGrossAmountPrevSum"), F.lit(0))) - F.coalesce(F.col("DistinctCollectionsPostOnboardingYTD"), F.lit(0)) 
    ).otherwise(lit(None))
).withColumn("IngestionTimestamp",lit(from_utc_timestamp(current_timestamp(), "Europe/Athens"))) 
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
final_df_cepalbp = final.select(*final_cols)

# --------- Additional logic when NPV = 0  ----------
final_df_cepalbp = final_df_cepalbp.withColumn(
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
cols = [c for c in final_df_cepalbp.columns if c != "BusinessDate"] + ["BusinessDate"]
final_df_cepalbp = final_df_cepalbp.select(cols)

# Fetch only distinct rows
final_df_cepalbp = final_df_cepalbp.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process and Save results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save results for CepalBP table

# COMMAND ----------

# DBTITLE 1,Save_To_FinalTable
# Ensure table_path to Delta table is correctly formatted for cephalbp
table_path = f"{target_area}.{table_name_cepalbp}"

# Check if the table_path and final_df_cepalbp are valid
if table_path and final_df_cepalbp:
    # Delete records from the Delta table where BusinessDate matches the specified date
    spark.sql(f"""
            DELETE FROM {table_path}
            WHERE BusinessDate = '{Business_date}'
            """)    

    # Write to final table CreditMemoEnhancementCepalBP
    final_df_cepalbp.write \
            .format("delta") \
            .partitionBy("BusinessDate") \
            .mode("append") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path) # Save using Delta Lake
else:
    raise ValueError("Invalid table path or DataFrame")

#Drop temp table
spark.sql(f"DROP TABLE {checkpoint_path}")
