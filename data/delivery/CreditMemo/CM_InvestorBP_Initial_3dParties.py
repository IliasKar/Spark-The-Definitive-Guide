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
securitized_portfolios_list = ["Galaxy IV Funding DAC", "Cosmos Wholesale", "Galaxy II Funding DAC", "Orion X Securitisation DAC", "Cosmos Retail", "Cosmos Common Customers"]
thirdparties_portfolios_list = ["Hermes Portfolio B"]

# Convert lists to SQL-friendly string format
securitized_portfolios_sql = ", ".join(f"'{p}'" for p in securitized_portfolios_list)
thirdparties_portfolios_sql = ", ".join(f"'{p}'" for p in thirdparties_portfolios_list)

# COMMAND ----------

# DBTITLE 1,Def env & target tables
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

# DBTITLE 1,Define variable values
table_name_investorbp="CreditMemoEnhancementInvestorBP"
target_area = f"`{env}-cdh-delivery`.`business-plan`"

checkpoint_path = f"`{env}-cdh-delivery`.`business-plan`.temp_investorbp_checkpoint"

# COMMAND ----------

# DBTITLE 1,load tables
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
DExposure = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.derivative.dexposure
    WHERE BusinessDate = '{Business_date}'
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
# MAGIC ### Calculations for InvestorBP 3rdParties

# COMMAND ----------

# DBTITLE 1,Define the InvestorBP Initial 3rdParties DF
# Define the InvestorBP Initial 3rdParties DF
df_investorbp_initial = spark.sql(f"""
WITH latestbusinessplan AS (
        SELECT max(BusinessPlanYear) as BusinessPlanYear, CDI 
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesInitial
        WHERE BusinessDate = '{Business_date}'
        GROUP BY CDI
    ),
    DBPCepalRecoveriesInitial AS(
        SELECT PortfolioName
      , latestbusinessplan.CDI
      , RecoveryDate
      , BusinessDate
      , SUM(COALESCE(RecoveryAmount, 0)) AS RecoveryAmountSum
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesInitial
        INNER JOIN latestbusinessplan
        ON DBPCepalRecoveriesInitial.CDI = latestbusinessplan.CDI
        AND DBPCepalRecoveriesInitial.BusinessPlanYear = latestbusinessplan.BusinessPlanYear
        WHERE BusinessDate = '{Business_date}'
        GROUP BY ALL
    )
SELECT
    portfolio.PortfolioId,
    DBPCepalRecoveriesInitial.PortfolioName,
    DBPCepalRecoveriesInitial.CDI as `key`,
    CAST(DBPCepalRecoveriesInitial.RecoveryDate AS DATE) AS `Date`,
    substring(cast(date (DBPCepalRecoveriesInitial.RecoveryDate) as string),1,7) as Month,
    DBPCepalRecoveriesInitial.BusinessDate as `BusinessDate`,
    year(DBPCepalRecoveriesInitial.RecoveryDate) as Year,
    a.MonthsLeft,
    DBPCepalRecoveriesInitial.RecoveryAmountSum AS `Amount`,
    SUM(DBPCepalRecoveriesInitial.RecoveryAmountSum) OVER (PARTITION BY DBPCepalRecoveriesInitial.CDI, year(DBPCepalRecoveriesInitial.RecoveryDate)) AS YearlyRecoveryAmountSum,
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
FROM DBPCepalRecoveriesInitial
    LEFT JOIN (SELECT 
                  d1.BusinessDate
                , d1.CDI
                , substring(d1.RecoveryDate, 1, 7) AS Month
                , CEIL(months_between(substring(d1.RecoveryDate, 1, 7), d1.BusinessDate)) AS MonthsLeft
                , RecoveriesInitial.InitialMonthPlan
            FROM DBPCepalRecoveriesInitial d1
            INNER JOIN (SELECT d2.CDI, MIN(substring(d2.RecoveryDate, 1, 7))  InitialMonthPlan, d2.BusinessDate -- Fetch Initial Plan Month for each CDI
                        FROM DBPCepalRecoveriesInitial d2
                        WHERE d2.BusinessDate ='{Business_date}'
                        GROUP BY d2.CDI, d2.BusinessDate) AS RecoveriesInitial ON d1.CDI = RecoveriesInitial.CDI
                                                              AND d1.BusinessDate = RecoveriesInitial.BusinessDate
            GROUP BY ALL 
            ) a ON DBPCepalRecoveriesInitial.CDI = a.CDI
        AND substring(DBPCepalRecoveriesInitial.RecoveryDate, 1, 7) = a.Month
    LEFT JOIN (
            SELECT
                BusinessDate, PortfolioName, cdi, SUM(gbv) AS GBV
            FROM dexposure
            GROUP BY ALL
            ) gbv 
    ON DBPCepalRecoveriesInitial.CDI = gbv.CDI
    LEFT JOIN portfolio
    ON substring(DBPCepalRecoveriesInitial.CDI, 1, 3) = portfolio.Prefix
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
                AND year(dcollection.ActualDate) <= YEAR(dcollection.BusinessDate)
                AND dcollection.FinalReason IN ('Matched', 'matched')
            GROUP BY ALL
        ) collectionpostonboarding
    ON a.cdi = collectionpostonboarding.CDI
        AND year(a.Month) <  year(collectionpostonboarding.BusinessDate)
        AND a.InitialMonthPlan <=  collectionpostonboarding.ActualMonthCollections                     -- PK[13/06/2026] Due to AAAS-1112. Exclude collections existing before the beginning of Plan.
        AND collectionpostonboarding.ActualMonthCollections = a.Month
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
    ON a.cdi = collectionpostonboardingcurrentyear.cdi
        AND year(a.Month) <= year(collectionpostonboardingcurrentyear.BusinessDate)
    LEFT JOIN (
        SELECT 
            DebtorCode, 
            SUM(CASE WHEN portfolio IN ({securitized_portfolios_sql}) THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
            SUM(CASE WHEN portfolio IN ({thirdparties_portfolios_sql}) THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThirdParties
        FROM `{env}-cdh`.reference.collections_preonboarding
        LEFT JOIN portfolio 
        ON substring(collections_preonboarding.DebtorCode, 1, 3) = portfolio.Prefix
        WHERE 
            collections_preonboarding.DebtorCode is not null 
        GROUP BY DebtorCode
        ) collectionpreonboarding
    ON DBPCepalRecoveriesInitial.CDI = collectionpreonboarding.DebtorCode 
GROUP BY ALL
""")

# COMMAND ----------

# DBTITLE 1,Process and Mark Business Plans for Current Year
# Extract the current year
current_year = df_investorbp_initial.select(F.year("businessdate").alias("current_year")).distinct().collect()[0]["current_year"]

# Filter keys that have business plans for the current year
# Use string formatting only once and avoid casting inside filter for every row
current_year_prefix = str(current_year)
df_curr_bp_exists = (df_investorbp_initial.filter(F.col("Month").substr(1, 4) == current_year_prefix).select("key").distinct())

# Filter keys that have non-null AmountOfCurrentYear
df_curr_am_exists = df_investorbp_initial.filter(df_investorbp_initial.AmountOfCurrentYear.isNotNull()).select("key").distinct()

# Find keys that have AmountOfCurrentYear but no business plans for the current year
df_keys_curr_amm_nobp = df_curr_am_exists.join(df_curr_bp_exists, "key", "left_anti")

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BP_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BP_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BP_CURR_YEAR column
df_investorbp_initial = (
    df_investorbp_initial
    .join(df_curr_bp_exists, df_investorbp_initial.key == df_curr_bp_exists.key_curr_bp_exists, "left")
    .withColumn("HAS_BP_CURR_YEAR", F.coalesce(F.col("HAS_BP_CURR_YEAR"), F.lit(0)))
    .drop("key_curr_bp_exists")
)

# Add MaxPreviousYear column for each key
df_max_previous_year = df_investorbp_initial.filter(F.col("Year") < current_year).groupBy("key").agg(F.max("Year").alias("MaxPreviousYear"))

# Join the main DataFrame with the MaxPreviousYear DataFrame
df_investorbp_initial = df_investorbp_initial.join(df_max_previous_year, "key", "left")

# COMMAND ----------

# DBTITLE 1,Calc SumOfAmountPreviousYears
# Calculate SumOfAmountPreviousYears grouped by key, Month, and YearActualDate
df_sum = df_investorbp_initial.groupBy("key", "Month", "YearActualDate").agg(
    F.sum("AmountOfYear").alias("SumOfAmountPreviousYears")
)

# Calculate total sum of SumOfAmountPreviousYears grouped by key and YearActualDate
df_total_sum_filtered = df_sum.groupBy("key", "YearActualDate").agg(
    F.sum("SumOfAmountPreviousYears").alias("TotalSumOfAmountPreviousYears")
).filter(F.col("TotalSumOfAmountPreviousYears").isNotNull())

# Join SumOfAmountPreviousYears back to the original DataFrame on key, Month, YearActualDate
df_with_sum = df_investorbp_initial.join(
    df_sum,
    on=["key", "Month", "YearActualDate"],
    how="left"
)

# Join TotalSumOfAmountPreviousYears back to df_with_sum on key and YearActualDate
df_investorbp_initial = df_with_sum.join(
    df_total_sum_filtered,
    on=["key", "YearActualDate"],
    how="left"
).drop("AmountOfYear", "YearActualDate").distinct()

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_investorbp_initial = df_investorbp_initial.withColumn(
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

# DBTITLE 1,Calc Total CollectionAmountfPostOnBoarding for PrevYears
# Select needed columns and remove duplicates if any
calc_cols = ["key", "Year", "AmountOfCurrentYear", "TotalSumOfAmountPreviousYears"]
df_calc = df_investorbp_initial.select(*calc_cols).distinct()

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
df_investorbp_initial = (
    df_investorbp_initial
    .join(df_calc_unique, on=["key", "Year"], how="inner")
    .dropDuplicates()  # Remove duplicates
)

# COMMAND ----------

# DBTITLE 1,Calc CollectionsPostOnboarding
df_investorbp_initial = df_investorbp_initial.withColumn(
    "CollectionsPostOnboardingSecuritizedCalc",
    F.when(
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list),
        F.col("CollectionAmountCalc")
    ).otherwise(F.lit(None))
).withColumn(
    "CollectionsPostOnboardingThirdPartiesCalc",
    F.when(
    (F.col("Year") <= current_year) | 
    (F.coalesce(
       F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(thirdparties_portfolios_list)),
        F.col("CollectionAmountCalcTotal")
    ).otherwise(F.lit(None))
).withColumn(
    "CollectionsPostOnboardingSecuritizedCalcTotal",
    F.when(
    (F.col("Year") <= current_year) | 
    (F.coalesce(
        F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(thirdparties_portfolios_list)),
        F.col("CollectionAmountCalcTotal")
   ).otherwise(F.lit(None))
).drop("CollectionAmountCalc", "CollectionAmountCalcTotal", "collectionpostonboarding_portfolioname", "collectionpostonboardingcurrentyear_portfolioname")

# COMMAND ----------

# DBTITLE 1,Calc YearlyAmount
df_investorbp_initial = df_investorbp_initial.withColumn(
    "YearlyAmount",
    F.coalesce(F.col("YearlyRecoveryAmountSum"), F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingSecuritizedCalc"), F.lit(0)) - F.coalesce(F.col("CollectionsPostOnboardingThirdPartiesCalc"), F.lit(0))
).drop("YearlyRecoveryAmountSum")

# COMMAND ----------

# DBTITLE 1,Checkpoint 1
df_investorbp_initial = checkpoint_df(df_investorbp_initial, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Function "calculate_months_diff"
# Create the df for the function "calculate_months_diff"
df_investorbp_initial_yearly = df_investorbp_initial.select("PortfolioId", "key", "Date", "Year", "YearlyAmount", "BusinessDate").distinct()
df_investorbp_initial_yearly = df_investorbp_initial_yearly.filter(df_investorbp_initial_yearly.Date >= df_investorbp_initial_yearly.BusinessDate)

# Apply the function and select the necessary columns
df_investorbp_initial_yearly_data = calculate_months_diff(df_investorbp_initial_yearly)
df_investorbp_initial_yearly_data = df_investorbp_initial_yearly_data.select("PortfolioId", "key", "Year", "YearlyAmount", "BusinessDate", "MonthsDiff").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_NPV_per_year"
NPVyearly_data_investorbp_initial = calculate_NPV_per_year(df_investorbp_initial_yearly_data, irr)

# COMMAND ----------

# DBTITLE 1,Function "calculate_duration"
# Apply the function
npv_duration_data_investorbp_initial = calculate_duration(NPVyearly_data_investorbp_initial)

# Select the fileds needed
npv_duration_data_investorbp_initial = npv_duration_data_investorbp_initial.select("key", "PortfolioId", "Duration").distinct()

# COMMAND ----------

# DBTITLE 1,Function "calculate_WAL_NPV"
# Create the df for the function "calculate_WAL_NPV"
data_per_portfolio = df_investorbp_initial.filter(df_investorbp_initial.Date >= df_investorbp_initial.BusinessDate)

# Apply the function
df_WAL_NVP_final = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine ALL DFs
# Perform the joins, keeping all columns from df_investorbp_initial
rs_df_investorbp_initial = (
    df_investorbp_initial
    .join(df_WAL_NVP_final, on=['PortfolioId', 'key'], how='left')
    .join(npv_duration_data_investorbp_initial, on=['PortfolioId', 'key'], how='left')
)

rs_df_investorbp_initial.createOrReplaceTempView("rs_df_investorbp_initial")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final query for InvestorBP (ThirdParties)

# COMMAND ----------

# DBTITLE 1,Checkpoint 2
rs_df_investorbp_initial = checkpoint_df(rs_df_investorbp_initial, checkpoint_path)

# COMMAND ----------

# DBTITLE 1,Final Query Part I
# Persist the input DataFrame if reused
df = rs_df_investorbp_initial 

# Window definitions
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

# DBTITLE 1,Final Query Part II
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
    F.col("TotalGrossAmount").cast("Double").alias("TotalGrossAmount"),
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
final_df_investbp = final.select(*final_cols)

# --------- Additional logic when NPV = 0  ----------
final_df_investbp = final_df_investbp.withColumn(
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

# # Move BusinessDate to the last position (if not already)
cols = [c for c in final_df_investbp.columns if c != "BusinessDate"] + ["BusinessDate"]
final_df_investbp = final_df_investbp.select(cols)

# Fetch only distinct rows
final_df_investbp = final_df_investbp.distinct()

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

# Check if table_path and final_df_investbp exist and not empty
if table_path and final_df_investbp and not final_df_investbp.isEmpty():
    # Delete only if table exists
    if table_exists(table_path):
        spark.sql(f"""
            DELETE FROM {table_path}
            WHERE BusinessDate = '{Business_date}'
              AND IsPortfolioSecuritizedFlag = 0
        """)

       # Append data to Delta table (creates table if it does not exist)
    final_df_investbp.write \
        .format("delta") \
        .clusterBy("BusinessDate") \
        .mode("append") \
        .saveAsTable(table_path)

    print(f"Data appended and table {table_path} refreshed successfully.") 

else:
    raise ValueError("Invalid table path or DataFrame")

#Drop temp table
spark.sql(f"DROP TABLE {checkpoint_path}")
