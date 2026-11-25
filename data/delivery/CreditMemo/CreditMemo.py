# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# DBTITLE 1,Authentication
import boto3
from botocore.exceptions import NoCredentialsError

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
   
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        # if aws_secret_key is not None:
        #     print("AWS secret key retrieved successfully.")
        # else:
        #     print("AWS secret key is None. Please check Databricks secrets configuration.")
 
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None

# Function to get AWS credentials from Databricks secrets
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration
 
        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
   
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key, ssm_client, aws_secret_key):
 
    if aws_secret_key is None:
        print("AWS secret key not available.")
        return
 
    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )
 
    return response['Parameter']['Value']

# Function to get the environment prefix
def get_env_prefix():
    secrets_scope = "aws-databricks"
    aws_keys = get_scope_key_names(secrets_scope)
    aws_key = aws_keys[0] if aws_keys else None
    # Get AWS secret key from Databricks secrets
    aws_secret_key = get_aws_credentials(secrets_scope,aws_key)
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)
 
    S3_client = boto3.client('s3', region_name='eu-central-1',
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret_key)  

    env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix', ssm_client, aws_secret_key)
    return env_prefix


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
    try_divide,
)

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import timedelta

# COMMAND ----------

# DBTITLE 1,Define function "calculate_WAL_NPV" Optimized
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_WAL_NPV(initial_data, portfolio_IRR):
    """
    Calculates WAL and NPV for each PortfolioId and key using Spark DataFrames.
    Returns a dictionary: {PortfolioId: DataFrame with columns ['key', 'WAL', 'NPV']}
    """
    try:
        # Join the IRR to the initial data
        data = initial_data.join(portfolio_IRR, on='PortfolioId', how='left')

        # Filter for positive Amounts for WAL and NPV calculations
        data_pos = data.filter(F.col('Amount') > 0)

        # Calculate min(Date) per PortfolioId for WAL calculation
        min_date_window = Window.partitionBy('PortfolioId')
        data_pos = data_pos.withColumn('min_date', F.min('Date').over(min_date_window))

        # Calculate 'years' as (Date + 1 day - min(Date)) / 365
        data_pos = data_pos.withColumn(
            'years',
            (F.datediff('Date', 'min_date') + 1) / F.lit(365)
        )

        # Calculate 'product' = Amount * years
        data_pos = data_pos.withColumn('product', F.col('Amount') * F.col('years'))

        # Group by PortfolioId and key for WAL
        wal_df = data_pos.groupBy('PortfolioId', 'key').agg(
            F.sum('Amount').alias('Amount'),
            F.sum('product').alias('product')
        ).withColumn(
            'WAL', F.round(F.col('product') / F.col('Amount'), 3)
        ).select('PortfolioId', 'key', 'WAL')

        # For NPV: Calculate months since min_date and discount
        data_pos = data_pos.withColumn(
            'months_since_min',
            (F.datediff('Date', 'min_date') / 30.44)
        )

        # Calculate NPV per row
        data_pos = data_pos.withColumn(
            'NPV_row',
            F.col('Amount') / F.pow(1 + F.col('DiscountRate') / 12, F.col('months_since_min'))
        )

        # Group by PortfolioId and key for NPV
        npv_df = data_pos.groupBy('PortfolioId', 'key').agg(
            F.sum('NPV_row').alias('NPV')
        )

        # Merge WAL and NPV results
        result_df = wal_df.join(npv_df, on=['PortfolioId', 'key'], how='outer')

        # Get all distinct keys per PortfolioId (to fill missing WAL/NPV with 0)
        all_keys = initial_data.select('PortfolioId', 'key').distinct()
        result_df = all_keys.join(result_df, on=['PortfolioId', 'key'], how='left').fillna(0)

        # Split into dictionary by PortfolioId
        output_table = {}
        for row in result_df.select('PortfolioId').distinct().collect():
            pid = row['PortfolioId']
            output_table[pid] = result_df.filter(F.col('PortfolioId') == pid).select('key', 'WAL', 'NPV')

        return output_table

    except Exception as e:
        print(f"Calculation failed! The following error occurred:\n{e}")
        return False
 

# COMMAND ----------

# DBTITLE 1,Define function "calculate_WAL_NPV"
# def calculate_WAL_NPV(
#     initial_data,  # This should be a Spark DataFrame
#     portfolio_IRR  # This should also be a Spark DataFrame
# ) -> dict:
#     """
#     Calculates WAL and NPV using PySpark DataFrames.
    
#     Inputs:
#     1. initial_data: Spark DataFrame with columns: 'PortfolioId', 'key', 'Date', and 'Amount'.
#     2. portfolio_IRR: Spark DataFrame with columns: 'PortfolioId', 'DiscountRate'.
    
#     Returns:
#     output_table: Dictionary where keys are PortfolioId, and values are Spark DataFrames with calculated KPIs: 'key', 'WAL', 'NPV'.
#     """
#     try:
#         output_table = {}

#         # Collect portfolio IRRs into a dictionary for lookup
#         irr_dict = {row['PortfolioId']: row['DiscountRate'] for row in portfolio_IRR.collect()}

#         unique_portfolio_ids = initial_data.select("PortfolioId").distinct().collect()

#         # Loop over each PortfolioId in portfolio_IRR
#         for row in unique_portfolio_ids:
#             PortfolioId = row["PortfolioId"]
#             print(f'Running calculation for portfolioID: {PortfolioId}')

#             # Filter data for the current PortfolioId
#             data = initial_data.filter(F.col('PortfolioId') == PortfolioId)
            
#             IRR = irr_dict[PortfolioId]

#             # Filter rows where Amount > 0
#             WAL_calc = data.filter(F.col('Amount') > 0)

#             # Calculate 'years' as (Date + 1 day - min(Date)) / 365
#             min_date = WAL_calc.agg(F.min('Date')).collect()[0][0]
#             WAL_calc = WAL_calc.withColumn('years', 
#                         (F.datediff(WAL_calc['Date'], F.lit(min_date)) + 1) / 365)

#             # Calculate 'product' (Amount * years)
#             WAL_calc = WAL_calc.withColumn('product', F.col('Amount') * F.col('years'))

#             # Group by 'key' and sum 'Amount' and 'product'
#             WAL_calc_grouped = WAL_calc.groupBy('key') \
#                                        .agg(F.sum('Amount').alias('Amount'),
#                                             F.sum('product').alias('product'))

#             # Calculate 'WAL' (product / Amount)
#             WAL_calc_grouped = WAL_calc_grouped.withColumn('WAL', F.round(F.col('product') / F.col('Amount'), 3)) \
#                                                .select('key', 'WAL')

#             # NPV Calculation
#             NPV_table = data.filter(F.col('Amount') > 0)

#             # Calculate 'Date' difference in months
#             date_diff = F.datediff(NPV_table['Date'], F.lit(min_date))
#             years_diff = date_diff / 365
#             months_diff = (date_diff % 365) / 30
#             NPV_table = NPV_table.withColumn('Date', years_diff * 12 + months_diff)

#             # Calculate NPV for each row
#             NPV_table = NPV_table.withColumn('NPV', 
#                             F.col('Amount') / (F.pow((1 + IRR / 12), F.col('Date'))))

#             # Sum NPV per 'key'
#             npv_grouped = NPV_table.groupBy('key').agg(F.sum('NPV').alias('NPV'))

#             # Merge WAL and NPV results
#             wal_npv = WAL_calc_grouped.join(npv_grouped, on='key', how='outer')

#             # Ensure all debtors (keys) are represented
#             debtors = data.select('key').distinct()
#             final_table = debtors.join(wal_npv, on='key', how='left').fillna(0)

#             # Store final table in the output dictionary
#             output_table[PortfolioId] = final_table
#             print('WAL,NPV calculation completed!')

#         return output_table

#     except Exception as e:
#         print(f"Calculation failed! The following error occurred:\n{e}")
#         return False

# COMMAND ----------

# DBTITLE 1,Define function "calculate_months_diff"
def calculate_months_diff(df):
    """
    Calculates the difference in months between the 'Date' and 'BusinessDate' columns for each 'key' in the DataFrame.

    The calculation is performed as follows:
    - For each unique 'key', determine the maximum year present in the 'Date' column (referred to as MaxYear).
    - For records where the year of 'Date' is less than MaxYear:
        - Calculate the difference in months between the last day of that year and the 'BusinessDate'.
    - For records where the year of 'Date' equals MaxYear:
        - Calculate the difference in months between the max 'Date' and the 'BusinessDate'.

    Parameters:
    df (DataFrame): A PySpark DataFrame containing the columns 'key', 'Date', and 'BusinessDate'.

    Returns:
    DataF
    """
    # Ensure 'Date' and 'BusinessDate' are in date format
    df = df.withColumn("Date", F.to_date("Date")) \
           .withColumn("BusinessDate", F.to_date("BusinessDate"))

    # Extract year and month from 'Date' and 'BusinessDate'
    df = df.withColumn("DateYear", F.year("Date")) \
           .withColumn("DateMonth", substring("Date", 1, 7)) \
           .withColumn("BusinessMonth", substring("BusinessDate", 1, 7))

    # Calculate the last month of each year
    df = df.withColumn("LastMonthOfYear", F.concat(F.col("DateYear"), F.lit("-12")))

    # Create a window partitioned by 'key' to calculate MaxYear
    window_spec = Window.partitionBy("key")

    # Calculate MaxYear for each 'key'
    df = df.withColumn("MaxYear", F.max("DateYear").over(window_spec))

    # Calculate MonthsDiff based on the given conditions
    df = df.withColumn(
        "MonthsDiff",
        F.when(
            F.col("DateYear") < F.col("MaxYear"),
            F.months_between("LastMonthOfYear", "BusinessMonth")
        ).when(
            F.col("DateYear") == F.col("MaxYear"),
            F.months_between("DateMonth", "BusinessMonth")
        ).otherwise(None)
    )


    # Create a window to select only the max MonthsDiff within MaxYear
    max_months_diff_window = Window.partitionBy("key", "MaxYear").orderBy(F.col("MonthsDiff").desc())
    
    # Assign row number based on max MonthsDiff within MaxYear
    df = df.withColumn("RowNum", F.row_number().over(max_months_diff_window))
    
    # Filter only the row with the highest MonthsDiff for MaxYear
    df = df.filter((F.col("DateYear") < F.col("MaxYear")) | (F.col("RowNum") == 1))
    
    # Drop unnecessary columns
    df = df.drop("LastMonthOfYear", "RowNum")

    return df

# COMMAND ----------

# DBTITLE 1,Define function "calculate_NPV_per_year"
def calculate_NPV_per_year(initial_data, portfolio_IRR):
    """
    Calculates NPV per key and year using PySpark DataFrames.

    Inputs:
    1. initial_data: Spark DataFrame with columns: 'PortfolioId', 'key', 'Year', 'YearlyAmount', 'MonthsDiff'.
    2. portfolio_IRR: Spark DataFrame with columns: 'PortfolioId', 'DiscountRate'.

    Returns:
    final_table: Spark DataFrame containing all original columns plus 'NPVYearly'.
    """
    try:
        # Join initial_data with portfolio_IRR to get the DiscountRate for each PortfolioId
        data_with_irr = initial_data.join(portfolio_IRR, on='PortfolioId', how='left')

        # Filter out rows with non-positive YearlyAmount
        data_with_irr = data_with_irr.filter(F.col('YearlyAmount') > 0)

        # Initialize an empty list to hold DataFrames
        npv_tables = []

        # Calculate NPV for each row
        data_with_npv = data_with_irr.withColumn(
            'NPV',
            F.col('YearlyAmount') / F.pow((1 + F.col('DiscountRate') / 12), F.col('MonthsDiff'))
        )
        print(f'Running calculation for NPVYearly...')

        # Define a window partitioned by 'PortfolioId', 'key', and 'Year'
        window_spec = Window.partitionBy('PortfolioId', 'key', 'Year')

        # Calculate NPVYearly using the window function
        data_with_npv_yearly = data_with_npv.withColumn(
            'NPVYearly',
            F.sum('NPV').over(window_spec)
        )

        # Drop the 'NPV' column if it's not needed
        final_table = data_with_npv_yearly.drop('NPV')

        print(f'NPVYearly calculation completed!')

        return final_table

    except Exception as e:
        print(f"Calculation failed! The following error occurred:\n{e}")
        return None


# COMMAND ----------

# DBTITLE 1,Def function "calculate_duration"
def calculate_duration(df):
    grouped = df.groupBy(['key', 'portfolioid'])
    duration = grouped.agg(
        (F.sum(F.col('npvyearly') * F.col('monthsdiff')) / (12 * F.sum(F.col('npvyearly')))).alias('Duration')
    )
    df = df.join(duration, on=['key', 'portfolioid'], how='left')
    return df

# COMMAND ----------

# DBTITLE 1,Define the specific BuisnessDate
# Define the specific BuisnessDate to run the notebook since no automated flow is available yet 
dbutils.widgets.text("BusinessDate", '2025-01-13')

# COMMAND ----------

# DBTITLE 1,Define securitized and third-party portfolios
# Define lists of securitized and third-party portfolios
securitized_portfolios_list = ["Galaxy IV Funding DAC", "Cosmos Wholesale"]
thirdparties_portfolios_list = ["Hermes Portfolio B"]

# Convert lists to SQL-friendly string format
securitized_portfolios_sql = ", ".join(f"'{p}'" for p in securitized_portfolios_list)
thirdparties_portfolios_sql = ", ".join(f"'{p}'" for p in thirdparties_portfolios_list)

# COMMAND ----------

# DBTITLE 1,def env & target tables
Business_date = dbutils.widgets.get("BusinessDate")

# import aws_utils

# Get Environment Prefix
env = get_env_prefix()
env = 'p'

table_name_cepalbp="CreditMemoEnhancementCepalBP"
table_name_investorbp="CreditMemoEnhancementInvestorBP"
target_area = f"`d-cdh-delivery`.`business-plan`"
# target_area = f"`{env}-cdh-delivery`.`business-plan`"

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
    WHERE BusinessDate = {Business_date!r}
""")

portfolio.createOrReplaceTempView("portfolio")


# Collection
collection = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.primary.collection
    WHERE BusinessDate = {Business_date!r}
""")

collection.createOrReplaceTempView("collection")


# DExposure
DExposure = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.derivative.dexposure
    WHERE BusinessDate = {Business_date!r}
""")

DExposure.createOrReplaceTempView("DExposure")


# DExposureAll (including open and closed accounts)
DExposureall = spark.sql(f"""
    SELECT *
    FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date({Business_date!r})
""")

DExposureall.createOrReplaceTempView("DExposureall")


# portfolio discount rate
irr = spark.sql(f"""
select * from `{env}-cdh`.reference.portfolio_discount_rate
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations for CepalBP

# COMMAND ----------

# DBTITLE 1,Define the CepalBP df
# Define the CepalBP subquery
df_cepalbp = spark.sql(f"""
WITH latestbusinessplan AS (
        SELECT max(BusinessPlan) as BusinessPlan, CDI 
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesParty
        WHERE BusinessDate = {Business_date!r}
        GROUP BY CDI
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
        WHERE crp.BusinessDate = {Business_date!r}
            AND crp.keytype='CDI'
        GROUP BY ALL
)


        SELECT
            CASE 
                WHEN DBPCepalRecoveriesParty.PortfolioName = 'Cosmos Wholesale' THEN 35
                WHEN DBPCepalRecoveriesParty.PortfolioName = 'Galaxy IV Funding DAC' THEN 21
                WHEN DBPCepalRecoveriesParty.PortfolioName = 'Symbol' THEN 15
                WHEN DBPCepalRecoveriesParty.PortfolioName = 'Neptune' THEN 16
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
                            Month, CDI, BusinessDate,
                            CEIL(months_between(Month, BusinessDate)) AS MonthsLeft
                    FROM DBPCepalRecoveriesParty
                    GROUP BY BusinessDate, CDI, Month
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
                        year(collection.ActualDate) AS YearActualDate,
                        collection.portfolioname,
                        portfolio.PortfolioId,
                        sum(collection.Amount) as AmountOfYear,
                        collection.BusinessDate         
                    FROM collection
                    INNER JOIN  portfolio
                        ON portfolio.unit_code_profits = collection.portfolioKey
                    INNER JOIN dexposureall dexposure
                        ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                                    THEN collection.loanbankcode
                                                        ELSE concat(portfolio.Prefix,collection.loanbankcode) END
                    WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                        AND year(collection.ActualDate) < YEAR(collection.BusinessDate)
                        AND collection.FinalReason IN ('Matched', 'matched')
                    GROUP BY ALL
                ) collectionpostonboarding
            ON crp.cdi = collectionpostonboarding.cdi
                AND year(crp.Month) < year(collectionpostonboarding.BusinessDate)
            LEFT JOIN
                (
                SELECT 
                        dexposure.CDI, 
                        year(collection.ActualDate) AS YearActualDate,
                        collection.portfolioname,
                        portfolio.PortfolioId,
                        sum(collection.Amount) as AmountOfCurrentYear,
                        collection.BusinessDate      
                    FROM collection
                    INNER JOIN  portfolio
                        ON portfolio.unit_code_profits = collection.portfolioKey
                    INNER JOIN dexposureall dexposure
                        ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                                    THEN collection.loanbankcode
                                                        ELSE concat(portfolio.Prefix,collection.loanbankcode) END
                    WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                        AND year(collection.ActualDate) = YEAR(collection.BusinessDate)
                        AND collection.FinalReason IN ('Matched', 'matched')
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
df_curr_bp_exists = df_cepalbp.filter(df_cepalbp.Month.startswith(current_year)).select("key").distinct()

# Filter keys that have non-null AmountOfCurrentYear
df_curr_am_exists = df_cepalbp.filter(df_cepalbp.AmountOfCurrentYear.isNotNull()).select("key").distinct()

# Find keys that have AmountOfCurrentYear but no business plans for the current year
df_keys_curr_amm_nobp = df_curr_am_exists.join(df_curr_bp_exists, df_curr_am_exists.key == df_curr_bp_exists.key, "left_anti")

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BR_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BR_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BR_CURR_YEAR column
df_cepalbp = df_cepalbp.join(df_curr_bp_exists, df_cepalbp.key == df_curr_bp_exists.key_curr_bp_exists, "left").drop("key_curr_bp_exists")
df_cepalbp = df_cepalbp.withColumn("HAS_BR_CURR_YEAR", F.coalesce(df_cepalbp.HAS_BR_CURR_YEAR, lit(0)))

# Add MaxPreviousYear column for each key
df_max_previous_year = df_cepalbp.filter(F.col("Year") < current_year).groupBy("key").agg(F.max("Year").alias("MaxPreviousYear"))

# Join the main DataFrame with the MaxPreviousYear DataFrame
df_cepalbp = df_cepalbp.join(df_max_previous_year, "key", "left")

# COMMAND ----------

# DBTITLE 1,Calc SumOfAmountPreviousYears
# Group the df_cepalbp DataFrame by 'key' and 'Month', 
# and calculate the sum of 'AmountOfYear' for each group.
df_cepalbp_with_sum = df_cepalbp.groupBy("key", "Month").agg(
    F.sum("AmountOfYear").alias("SumOfAmountPreviousYears")  
).withColumnRenamed("Month", "sum_Month").withColumnRenamed("key", "sum_key")

# Perform an inner join between df_cepalbp and df_cepalbp_with_sum on 'key' and 'Month'
df_cepalbp = df_cepalbp.join(
    df_cepalbp_with_sum,
    (F.col("key") == F.col("sum_key")) & (F.col("Month") == F.col("sum_Month")),
    "inner"
).drop("sum_key", "sum_Month", "AmountOfYear", "YearActualDate"
       ).distinct()

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_cepalbp = df_cepalbp.withColumn(
    "CollectionAmountCalc",
    F.when(
        # If there is a business plan for the current year and the year is the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") == current_year),
        F.col("AmountOfCurrentYear")
    ).when(
        # If there is a business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is a business plan for the current year and the year is after the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") > current_year),
        F.lit(None)
    ).when(
        # If there is no business plan for the current year and the year is the MaxPreviousYear
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") == F.col("MaxPreviousYear")),
        F.col("AmountOfCurrentYear") + F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is no business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).otherwise(F.lit(None))  # For all other cases, set CollectionAmountCalc to None
)

# COMMAND ----------

# DBTITLE 1,Calc Total CollectionAmountforPostOnBoarding for PreviousYears
from pyspark.sql import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("sum_key").orderBy("sum_Year")

# Create New df for calculating Collections Amount Total per Year
df_cepalbp_total = (
    df_cepalbp
    .select("key", "PortfolioId", "Year", "BusinessDate", "AmountOfCurrentYear", "SumOfAmountPreviousYears")
    .distinct()
    .withColumnRenamed("key", "sum_key")
    .withColumnRenamed("Year", "sum_Year")
    .withColumnRenamed("PortfolioId", "sum_PortfolioId")
    .withColumnRenamed("BusinessDate", "sum_BusinessDate")
    .withColumnRenamed("AmountOfCurrentYear", "sum_AmountOfCurrentYear")
    .withColumnRenamed("SumOfAmountPreviousYears", "sum_SumOfAmountPreviousYears")
)

df_cepalbp_total = df_cepalbp_total.withColumn(
    "CollectionAmountCalcTotal",
    F.when(
        # If the year is the current year calculate the total amount 
        (F.col("sum_Year") == current_year),         
        F.coalesce(F.col("sum_AmountOfCurrentYear"), F.lit(0)) + F.coalesce(F.lag("sum_SumOfAmountPreviousYears",1).over(window_spec), F.lit(0))  # Access previous row 
    ).otherwise(F.col("sum_SumOfAmountPreviousYears")) 
)

# Perform an inner join between df_cepalbp and df_cepalbp_total on 'key' and 'Year'
df_cepalbp = df_cepalbp.join(
    df_cepalbp_total,
    (F.col("key") == F.col("sum_key")) & (F.col("Year") == F.col("sum_Year")),
    "inner"
).drop("sum_key", "sum_Year", "sum_PortfolioId", "sum_BusinessDate", "sum_AmountOfCurrentYear", "sum_SumOfAmountPreviousYears"
       ).distinct()

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
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list),
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
output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine all dfs
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

result_df_cepal = result_df_cepal.join(
    npv_duration_data,
    on=['key', 'PortfolioId'],
    how='left'
)

result_df_cepal.createOrReplaceTempView("result_df_cepal")

# COMMAND ----------

# from pyspark.sql.functions import sum as _sum

# display(result_df_cepal.select(_sum("NPV").alias("Total_NPV")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations for InvestorBP

# COMMAND ----------

# MAGIC %md
# MAGIC #### First InvestorBP df

# COMMAND ----------

# DBTITLE 1,Define the InvestorBP 1st df
# Define the InvestorBP 1st df
df_investorbp_haps = spark.sql(f"""
WITH DBPHAPS AS (
        SELECT SourcePortfolioName
            , CDI
            , HAPSPeriod
            , businessDate
            , SUM(COALESCE(DBPHAPS.HAPSTarget, 0)) AS HAPSTargetSum
        FROM `{env}-cdh`.derivative.DBPHAPS
        WHERE BusinessDate = {Business_date!r}
            AND CDI IS NOT NULL
        GROUP BY ALL
)
SELECT
    CASE 
        WHEN DBPHAPS.SourcePortfolioName = 'CosmosWHL' THEN 35
        WHEN DBPHAPS.SourcePortfolioName = 'GalaxyIV' THEN 21
        ELSE portfolio.PortfolioId 
    END AS PortfolioId,
    CASE
        WHEN DBPHAPS.SourcePortfolioName = 'CosmosWHL' THEN 'Cosmos Wholesale'
        WHEN DBPHAPS.SourcePortfolioName = 'GalaxyIV' THEN 'Galaxy IV Funding DAC'
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
            ) a 
    ON DBPHAPS.CDI = a.CDI
        AND substring(DBPHAPS.HAPSPeriod, 1, 7) = a.Month
    LEFT JOIN (
            SELECT
                BusinessDate, PortfolioName, cdi, SUM(gbv) AS GBV
            FROM dexposure
            GROUP BY ALL
            ) gbv 
    ON DBPHAPS.CDI = gbv.CDI
    LEFT JOIN portfolio
    ON substring(DBPHAPS.CDI, 1, 3) = portfolio.Prefix
    LEFT JOIN 
        (
            SELECT  
                dexposure.CDI, 
                year(collection.ActualDate) AS YearActualDate,
                collection.portfolioname,
                portfolio.PortfolioId,
                sum(collection.Amount) as AmountOfYear,
                collection.BusinessDate
            FROM collection
            INNER JOIN  portfolio
                ON portfolio.unit_code_profits = collection.portfolioKey
            INNER JOIN dexposureall dexposure
                ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                            THEN collection.loanbankcode
                                                ELSE concat(portfolio.Prefix,collection.loanbankcode) END
            WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                AND year(collection.ActualDate) < YEAR(collection.BusinessDate)
                AND collection.FinalReason IN ('Matched', 'matched')
            GROUP BY ALL
        ) collectionpostonboarding
    ON a.cdi = collectionpostonboarding.CDI
        AND year(a.Month) <  year(collectionpostonboarding.BusinessDate)
    LEFT JOIN
        (
            SELECT 
                dexposure.CDI, 
                year(collection.ActualDate) AS YearActualDate,
                collection.portfolioname,
                portfolio.PortfolioId,
                sum(collection.Amount) as AmountOfCurrentYear,
                collection.BusinessDate        
            FROM collection
            INNER JOIN  portfolio
                ON portfolio.unit_code_profits = collection.portfolioKey
            INNER JOIN dexposureall dexposure
                ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                            THEN collection.loanbankcode
                                                ELSE concat(portfolio.Prefix,collection.loanbankcode) END
            WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                AND year(collection.ActualDate) = YEAR(collection.BusinessDate)
                AND collection.FinalReason IN ('Matched', 'matched')
            GROUP BY ALL
        ) collectionpostonboardingcurrentyear
    ON a.cdi = collectionpostonboardingcurrentyear.cdi
        AND year(a.Month) <=  year(collectionpostonboardingcurrentyear.BusinessDate)
    LEFT JOIN (
            SELECT 
                DebtorCode, 
                SUM(CASE WHEN portfolio IN ('GalaxyIV', 'CosmosWHL') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
                SUM(CASE WHEN portfolio IN ('Hermes B') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThirdParties
            FROM `{env}-cdh`.reference.collections_preonboarding
            LEFT JOIN portfolio 
            ON substring(collections_preonboarding.DebtorCode, 1, 3) = portfolio.Prefix
            WHERE 
                DebtorCode <> 'NULL' 
                AND portfolio in ('GalaxyIV','CosmosWHL','Hermes B')
                AND substring(collections_preonboarding.CollectionPeriod, 1, 7) <= portfolio.PortfolioOnBoardingMonth
            GROUP BY DebtorCode
        ) collectionpreonboarding 
    ON DBPHAPS.CDI = collectionpreonboarding.DebtorCode
GROUP BY ALL
""")

# COMMAND ----------

# DBTITLE 1,Process and Mark Business Plans for Current Year
# Extract the current year
current_year = df_investorbp_haps.select(F.year("businessdate").alias("current_year")).distinct().collect()[0]["current_year"]

# Filter keys that have business plans for the current year
# df_curr_bp_exists = df_investorbp_haps.filter(df_investorbp_haps.Month.startswith(current_year)).select("key").distinct()
df_curr_bp_exists = df_investorbp_haps.filter(df_investorbp_haps.Month.cast("string").startswith(str(current_year))).select("key").distinct()

# Filter keys that have non-null AmountOfCurrentYear
df_curr_am_exists = df_investorbp_haps.filter(df_investorbp_haps.AmountOfCurrentYear.isNotNull()).select("key").distinct()

# Find keys that have AmountOfCurrentYear but no business plans for the current year
df_keys_curr_amm_nobp = df_curr_am_exists.join(df_curr_bp_exists, df_curr_am_exists.key == df_curr_bp_exists.key, "left_anti")

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BR_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BR_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BR_CURR_YEAR column
df_investorbp_haps = df_investorbp_haps.join(df_curr_bp_exists, df_investorbp_haps.key == df_curr_bp_exists.key_curr_bp_exists, "left").drop("key_curr_bp_exists")
df_investorbp_haps = df_investorbp_haps.withColumn("HAS_BR_CURR_YEAR", F.coalesce(df_investorbp_haps.HAS_BR_CURR_YEAR, lit(0)))

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

# display(df_investorbp_haps.filter(df_investorbp_haps.key=='GLC0002985677'))

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_investorbp_haps = df_investorbp_haps.withColumn(
    "CollectionAmountCalc",
    F.when(
        # If there is a business plan for the current year and the year is the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") == current_year),
        F.col("AmountOfCurrentYear")
    ).when(
        # If there is a business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is a business plan for the current year and the year is after the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") > current_year),
        F.lit(None)
    ).when(
        # If there is no business plan for the current year and the year is the MaxPreviousYear
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") == F.col("MaxPreviousYear")),
        F.col("AmountOfCurrentYear") + F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is no business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).otherwise(F.lit(None))  # For all other cases, set CollectionAmountCalc to None
)

# COMMAND ----------

# DBTITLE 1,Calc Total CollectionAmountforPostOnBoarding for PreviousYears
from pyspark.sql import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("sum_key").orderBy("sum_Year")

# Create New df for calculating Collections Amount Total per Year
df_investorbp_haps_total = (
    df_investorbp_haps
    .select("key", "PortfolioId", "Year", "BusinessDate", "AmountOfCurrentYear", "SumOfAmountPreviousYears")
    .distinct()
    .withColumnRenamed("key", "sum_key")
    .withColumnRenamed("Year", "sum_Year")
    .withColumnRenamed("PortfolioId", "sum_PortfolioId")
    .withColumnRenamed("BusinessDate", "sum_BusinessDate")
    .withColumnRenamed("AmountOfCurrentYear", "sum_AmountOfCurrentYear")
    .withColumnRenamed("SumOfAmountPreviousYears", "sum_SumOfAmountPreviousYears")
)

df_investorbp_haps_total = df_investorbp_haps_total.withColumn(
    "CollectionAmountCalcTotal",
    F.when(
        # If the year is the current year calculate the total amount 
        (F.col("sum_Year") == current_year),         
        F.coalesce(F.col("sum_AmountOfCurrentYear"), F.lit(0)) + F.coalesce(F.lag("sum_SumOfAmountPreviousYears",1).over(window_spec), F.lit(0))  # Access previous row 
    ).otherwise(F.col("sum_SumOfAmountPreviousYears")) 
)

# Perform an inner join between df_investorbp_haps and df_investorbp_haps_total on 'key' and 'Year'
df_investorbp_haps = df_investorbp_haps.join(
    df_investorbp_haps_total,
    (F.col("key") == F.col("sum_key")) & (F.col("Year") == F.col("sum_Year")),
    "inner"
).drop("sum_key", "sum_Year", "sum_PortfolioId", "sum_BusinessDate", "sum_AmountOfCurrentYear", "sum_SumOfAmountPreviousYears"
       ).distinct()
       

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
    F.when(
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list),
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
output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine all dfs
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

result_df_investorbp = result_df_investorbp.join(
    npv_duration_data_investorbp_haps,
    on=['key', 'PortfolioId'],
    how='left'
)

# COMMAND ----------

# DBTITLE 1,Calc NPV_Calc
# Get Max Amount values for each key across all years
df_max = result_df_investorbp.groupBy("key").agg(
    F.max("AmountOfCurrentYear").alias("MaxAmountOfCurrentYear"),
    F.max("SumOfAmountPreviousYears").alias("MaxSumOfAmountPreviousYears")
)

# Join the main DataFrame with the MaxPreviousYear DataFrame
result_df_investorbp = result_df_investorbp.join(df_max, "key", "left")

result_df_investorbp = result_df_investorbp.withColumn(
    "NPV_Calc",
    F.coalesce(F.col("NPV"), F.lit(0)) - (
        F.coalesce(F.col("CollectionsPreOnboardingSecuritized"), F.lit(0)) +
        F.coalesce(F.col("CollectionsPreOnboardingThirdParties"), F.lit(0))
    ) - ( 
          F.coalesce(F.col("MaxAmountOfCurrentYear"), F.lit(0)) +
          F.coalesce(F.col("MaxSumOfAmountPreviousYears"), F.lit(0))
        )
).drop("MaxAmountOfCurrentYear","MaxSumOfAmountPreviousYears")


# COMMAND ----------

# display(result_df_investorbp.filter(result_df_investorbp.key=='GLC0002985677'))
# from pyspark.sql.functions import sum as _sum
# display(result_df_investorbp.select(_sum("NPV").alias("Total_NPV")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Second InvestorBP df

# COMMAND ----------

# DBTITLE 1,Define the InvestorBP 2nd df
# Define the InvestorBP 2nd subquery
df_investorbp_initial = spark.sql(f"""
WITH DBPCepalRecoveriesInitial AS (
        SELECT PortfolioName
            , CDI
            , RecoveryDate
            , BusinessDate
            , SUM(COALESCE(RecoveryAmount, 0)) AS RecoveryAmountSum
        FROM `{env}-cdh`.derivative.DBPCepalRecoveriesInitial
        WHERE BusinessDate = {Business_date!r}
            AND CDI IS NOT NULL
            AND InitialBusinessPlan = False
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
            LEFT JOIN (
                    SELECT 
                        BusinessDate
                        , CDI
                        , substring(RecoveryDate, 1, 7) AS Month
                        , CEIL(months_between(substring(RecoveryDate, 1, 7), BusinessDate)) AS MonthsLeft
                    FROM DBPCepalRecoveriesInitial
                    GROUP BY BusinessDate, CDI, Month
                    ) a 
            ON DBPCepalRecoveriesInitial.CDI = a.CDI
                AND substring(RecoveryDate, 1, 7) = a.Month
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
                        year(collection.ActualDate) AS YearActualDate,
                        collection.portfolioname,
                        portfolio.PortfolioId,
                        sum(collection.Amount) as AmountOfYear,
                        collection.BusinessDate
                    FROM collection
                    INNER JOIN  portfolio
                        ON portfolio.unit_code_profits = collection.portfolioKey
                    INNER JOIN dexposureall dexposure
                        ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                                    THEN collection.loanbankcode
                                                        ELSE concat(portfolio.Prefix,collection.loanbankcode) END 
                    WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                        AND year(collection.ActualDate) <= YEAR(collection.BusinessDate)
                        AND collection.FinalReason IN ('Matched', 'matched')
                    GROUP BY ALL
                ) collectionpostonboarding
            ON a.cdi = collectionpostonboarding.CDI
                AND year(a.Month) <  year(collectionpostonboarding.BusinessDate)
            LEFT JOIN
                (
                    SELECT 
                        dexposure.CDI, 
                        year(collection.ActualDate) AS YearActualDate,
                        collection.portfolioname,
                        portfolio.PortfolioId,
                        sum(collection.Amount) as AmountOfCurrentYear,
                        collection.BusinessDate        
                    FROM collection
                    INNER JOIN  portfolio
                        ON portfolio.unit_code_profits = collection.portfolioKey
                    INNER JOIN dexposureall dexposure
                        ON dexposure.ExposureKey = CASE WHEN LEFT(collection.loanbankcode,3)= portfolio.Prefix 
                                                                    THEN collection.loanbankcode
                                                        ELSE concat(portfolio.Prefix,collection.loanbankcode) END
                    WHERE substring(collection.ActualDate, 1, 7) >= portfolio.PortfolioOnBoardingMonth
                        AND year(collection.ActualDate) = YEAR(collection.BusinessDate)
                        AND collection.FinalReason IN ('Matched', 'matched')
                    GROUP BY ALL
                ) collectionpostonboardingcurrentyear
            ON a.cdi = collectionpostonboardingcurrentyear.cdi
                AND year(a.Month) <= year(collectionpostonboardingcurrentyear.BusinessDate)
            LEFT JOIN (
                SELECT 
                    DebtorCode, 
                    SUM(CASE WHEN portfolio IN ('GalaxyIV', 'CosmosWHL') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingSecuritized,
                    SUM(CASE WHEN portfolio IN ('Hermes B') THEN Amount ELSE 0 END) AS CollectionsPreOnboardingThirdParties
                FROM `{env}-cdh`.reference.collections_preonboarding
                LEFT JOIN portfolio 
                ON substring(collections_preonboarding.DebtorCode, 1, 3) = portfolio.Prefix
                WHERE 
                    collections_preonboarding.DebtorCode <> 'NULL' 
                    AND collections_preonboarding.portfolio in ('GalaxyIV','CosmosWHL','Hermes B')
                    AND substring(collections_preonboarding.CollectionPeriod, 1, 7) <= portfolio.PortfolioOnBoardingMonth
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
# df_curr_bp_exists = df_investorbp_initial.filter(df_investorbp_initial.Month.startswith(current_year)).select("key").distinct()
df_curr_bp_exists = df_investorbp_initial.filter(df_investorbp_initial.Month.cast("string").startswith(str(current_year))).select("key").distinct()

# Filter keys that have non-null AmountOfCurrentYear
df_curr_am_exists = df_investorbp_initial.filter(df_investorbp_initial.AmountOfCurrentYear.isNotNull()).select("key").distinct()

# Find keys that have AmountOfCurrentYear but no business plans for the current year
df_keys_curr_amm_nobp = df_curr_am_exists.join(df_curr_bp_exists, df_curr_am_exists.key == df_curr_bp_exists.key, "left_anti")

# Mark keys that have business plans for the current year
df_curr_bp_exists = df_curr_bp_exists.withColumn("HAS_BR_CURR_YEAR", lit(1)).select(F.col("key").alias("key_curr_bp_exists"), "HAS_BR_CURR_YEAR")

# Join the main DataFrame with the current year business plan keys and set HAS_BR_CURR_YEAR column
df_investorbp_initial = df_investorbp_initial.join(df_curr_bp_exists, df_investorbp_initial.key == df_curr_bp_exists.key_curr_bp_exists, "left").drop("key_curr_bp_exists")
df_investorbp_initial = df_investorbp_initial.withColumn("HAS_BR_CURR_YEAR", F.coalesce(df_investorbp_initial.HAS_BR_CURR_YEAR, lit(0)))

# Add MaxPreviousYear column for each key
df_max_previous_year = df_investorbp_initial.filter(F.col("Year") < current_year).groupBy("key").agg(F.max("Year").alias("MaxPreviousYear"))

# Join the main DataFrame with the MaxPreviousYear DataFrame
df_investorbp_initial = df_investorbp_initial.join(df_max_previous_year, "key", "left")

# COMMAND ----------

# DBTITLE 1,Calc SumOfAmountPreviousYears
# Group the df_investorbp_initial DataFrame by 'key' and 'Month', 
# and calculate the sum of 'AmountOfYear' for each group.
df_investorbp_initial_with_sum = df_investorbp_initial.groupBy("key", "Month").agg(
    F.sum("AmountOfYear").alias("SumOfAmountPreviousYears")  
).withColumnRenamed("Month", "sum_Month").withColumnRenamed("key", "sum_key")

# Perform an inner join between df_investorbp_initial and df_investorbp_initial_with_sum on 'key' and 'Month'
df_investorbp_initial = df_investorbp_initial.join(
    df_investorbp_initial_with_sum,
    (F.col("key") == F.col("sum_key")) & (F.col("Month") == F.col("sum_Month")),
    "inner"
).drop("sum_key", "sum_Month", "AmountOfYear", "YearActualDate"
       ).distinct()

# COMMAND ----------

# DBTITLE 1,Calc CollectionAmountCalc
df_investorbp_initial = df_investorbp_initial.withColumn(
    "CollectionAmountCalc",
    F.when(
        # If there is a business plan for the current year and the year is the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") == current_year),
        F.col("AmountOfCurrentYear")
    ).when(
        # If there is a business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is a business plan for the current year and the year is after the current year
        (F.col("HAS_BR_CURR_YEAR") == 1) & (F.col("Year") > current_year),
        F.lit(None)
    ).when(
        # If there is no business plan for the current year and the year is the MaxPreviousYear
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") == F.col("MaxPreviousYear")),
        F.col("AmountOfCurrentYear") + F.col("SumOfAmountPreviousYears")
    ).when(
        # If there is no business plan for the current year and the year is before the current year
        (F.col("HAS_BR_CURR_YEAR") == 0) & (F.col("Year") < current_year),
        F.col("SumOfAmountPreviousYears")
    ).otherwise(F.lit(None))  # For all other cases, set CollectionAmountCalc to None
)

# COMMAND ----------

# DBTITLE 1,Calc Total CollectionAmountforPostOnBoarding for PreviousYears
from pyspark.sql import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("sum_key").orderBy("sum_Year")

# Create New df for calculating Collections Amount Total per Year
df_investorbp_initial_total = (
    df_investorbp_initial
    .select("key", "PortfolioId", "Year", "BusinessDate", "AmountOfCurrentYear", "SumOfAmountPreviousYears")
    .distinct()
    .withColumnRenamed("key", "sum_key")
    .withColumnRenamed("Year", "sum_Year")
    .withColumnRenamed("PortfolioId", "sum_PortfolioId")
    .withColumnRenamed("BusinessDate", "sum_BusinessDate")
    .withColumnRenamed("AmountOfCurrentYear", "sum_AmountOfCurrentYear")
    .withColumnRenamed("SumOfAmountPreviousYears", "sum_SumOfAmountPreviousYears")
)

df_investorbp_initial_total = df_investorbp_initial_total.withColumn(
    "CollectionAmountCalcTotal",
    F.when(
        # If the year is the current year calculate the total amount 
        (F.col("sum_Year") == current_year),         
        F.coalesce(F.col("sum_AmountOfCurrentYear"), F.lit(0)) + F.coalesce(F.lag("sum_SumOfAmountPreviousYears",1).over(window_spec), F.lit(0))  # Access previous row 
    ).otherwise(F.col("sum_SumOfAmountPreviousYears")) 
)

# Perform an inner join between df_investorbp_haps and df_investorbp_haps_total on 'key' and 'Year'
df_investorbp_initial = df_investorbp_initial.join(
    df_investorbp_initial_total,
    (F.col("key") == F.col("sum_key")) & (F.col("Year") == F.col("sum_Year")),
    "inner"
).drop("sum_key", "sum_Year", "sum_PortfolioId", "sum_BusinessDate", "sum_AmountOfCurrentYear", "sum_SumOfAmountPreviousYears"
       ).distinct()

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
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(thirdparties_portfolios_list),
        F.col("CollectionAmountCalc")
    ).otherwise(F.lit(None))
).withColumn(
    "CollectionsPostOnboardingSecuritizedCalcTotal",
    F.when(
        F.coalesce(F.col("collectionpostonboarding_portfolioname"), F.col("collectionpostonboardingcurrentyear_portfolioname")).isin(securitized_portfolios_list),
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
output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

# DBTITLE 1,Combine all dfs
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

result_df_investorbp_2nd = result_df_investorbp_2nd.join(
    npv_duration_data_investorbp_initial,
    on=['key', 'PortfolioId'],
    how='left'
)

# COMMAND ----------

# DBTITLE 1,Calc NPV_Calc
result_df_investorbp_2nd = result_df_investorbp_2nd.withColumn(
    "NPV_Calc",
    F.coalesce(F.col("NPV"), F.lit(0)) - (
        F.coalesce(F.col("CollectionsPreOnboardingSecuritized"), F.lit(0)) +
        F.coalesce(F.col("CollectionsPreOnboardingThirdParties"), F.lit(0))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Union of InvestorBP dfs

# COMMAND ----------

# DBTITLE 1,Union InvestorBP dfs
# Union the two DataFrames by column names
df_investorbp_combined = result_df_investorbp.unionByName(result_df_investorbp_2nd)

df_investorbp_combined.createOrReplaceTempView("df_investorbp_combined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final query

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final query for CepalBP

# COMMAND ----------

# Define the final query with NULL-safe GrossAmount calculation
# final_df_cepalbp = spark.sql(f"""
# WITH Final AS (
#         SELECT a.*,
#             SUM(a.Amount) OVER (PARTITION BY a.key, a.Year ) AS TotalGrossAmount, 
#             CASE WHEN Month >= left(BusinessDate,7) THEN a.Duration ELSE Null END AS DurationCalc,
#             CASE WHEN a.Month >= left(a.BusinessDate,7) THEN 
#                         a.NPV - (
#                                 coalesce(amountOfCurrentYearSecuritized,0)
#                                 + coalesce(amountOfCurrentYearThirdParties,0)
#                             -- coalesce(CollectionsPostOnboardingSecuritizedCalc,0)
#                             -- + coalesce(CollectionsPostOnboardingThirdPartiesCalc,0)
#                                 )
#                         ELSE Null END AS NPVCalc,
#             CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV ELSE Null END AS TotalNPV,
#             CAST(a.CollectionsPostOnboardingSecuritizedCalc AS FLOAT) AS CollectionsPostOnboardingSecuritized,
#             CAST(a.CollectionsPostOnboardingSecuritizedCalcTotal AS FLOAT) AS CollectionsPostOnboardingSecuritizedCalcSum,
#             CAST(a.CollectionsPostOnboardingThirdPartiesCalc AS FLOAT) AS CollectionsPostOnboardingThirdParties,
#             CASE 
#                 WHEN a.PortfolioName LIKE '%Cosmos%' THEN 1
#                 WHEN a.PortfolioName LIKE '%Galaxy%' THEN 1
#                 ELSE 0
#             END AS IsPortfolioSecuritizedFlag
#         FROM 
#             result_df_cepal a
#     ),
#     RS AS (
#         SELECT 
#             F.PortfolioName,
#             F.key AS CDI,
#             F.GBV,
#             F.Year,
#             -- SUM(F.TotalGrossAmount) OVER (PARTITION BY F.key, F.Year ORDER BY F.Year) AS GrossAmount, -- keep for testing GrossAmount incremental total 
#             SUM(F.TotalGrossAmount) OVER (PARTITION BY F.key, F.Year ORDER BY F.Year) 
#             - COALESCE(F.CollectionsPostOnboardingSecuritizedCalcSum, 0)
#             - COALESCE(F.CollectionsPostOnboardingThirdParties, 0) AS GrossAmountCalc,
#             F.TotalGrossAmount,
#             F.DurationCalc AS Duration,
#             F.WAL AS AverageLife,
#             F.NPVCalc AS NPV,
#             F.TotalNPV,
#             try_divide(F.NPVCalc, F.GBV) AS `NPV/GBV`,
#             F.CollectionsPreOnboardingSecuritized,
#             F.CollectionsPreOnboardingThirdParties AS CollectionsPreOnboardingThrirdParties,
#             F.CollectionsPostOnboardingSecuritized,
#             F.CollectionsPostOnboardingSecuritizedCalcSum AS CollectionsPostOnboardingSecuritizedCalcTotal,
#             F.CollectionsPostOnboardingThirdParties,   
#             F.IsPortfolioSecuritizedFlag,    
#             F.BusinessDate
#         FROM Final F
#         GROUP BY ALL),
#     PrevGrossAmount AS (
#         SELECT 
#             R1.CDI,
#             R1.PortfolioName,
#             LAST_VALUE(R1.GrossAmountCalc) OVER(ORDER BY R1.CDI, R1.Year ASC) AS PrevGrossAmount
#         FROM RS R1
#         WHERE R1.Year = YEAR(R1.BusinessDate)),
#     TotalGrossAmountCalc AS (
#         SELECT 
#             R2.CDI,
#             R2.Year,
#             SUM(R2.TotalGrossAmount) OVER(PARTITION BY R2.CDI ORDER BY R2.Year) AS TotalGrossAmountCalc
#         FROM RS R2
#         WHERE R2.Year > YEAR(R2.BusinessDate)
#     ) 
#     SELECT 
#         R.PortfolioName,
#         R.CDI,
#         R.GBV,
#         CAST(R.Year AS VARCHAR(4)) AS Year,
#         CASE WHEN R.Year > YEAR(R.BusinessDate) THEN B.PrevGrossAmount + C.TotalGrossAmountCalc
#             ELSE R.GrossAmountCalc 
#         END AS GrossAmount,
#         R.TotalGrossAmount,
#         R.Duration,
#         R.AverageLife,
#         R.NPV,
#         R.TotalNPV,
#         `NPV/GBV`,
#         R.CollectionsPreOnboardingSecuritized,
#         R.CollectionsPreOnboardingThrirdParties,
#         R.CollectionsPostOnboardingSecuritized,
#         R.CollectionsPostOnboardingSecuritizedCalcTotal,
#         R.CollectionsPostOnboardingThirdParties,   
#         R.IsPortfolioSecuritizedFlag,    
#         R.BusinessDate
#     FROM RS R 
#         LEFT JOIN PrevGrossAmount B ON R.CDI = B.CDI 
#         LEFT JOIN TotalGrossAmountCalc C ON R.CDI = C.CDI AND R.Year = C.Year
# """)

# # Get current columns
# current_columns = final_df_cepalbp.columns

# # Move 'BusinessDate' to the last position
# new_columns = [col for col in current_columns if col != "BusinessDate"] + ["BusinessDate"]

# # Select the new column order
# final_df_cepalbp = final_df_cepalbp.select(new_columns)

# COMMAND ----------

# display(final_df_cepalbp.filter(final_df_cepalbp.CDI == 'CSW0000394567'))
# display(final_df_cepalbp.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final query for InvestorBP

# COMMAND ----------

# Define the final query with NULL-safe GrossAmount calculation
# final_df_investorbp = spark.sql(f"""
# WITH Final AS (
#     SELECT a.*
#         , SUM(a.Amount) OVER (PARTITION BY a.key, a.Year ) AS TotalGrossAmount 
#         , CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.Duration ELSE Null END AS DurationCalc
#         -- , CASE WHEN a.Month >= left(a.BusinessDate,7) THEN 
#         --                                                 a.NPV - (
#         --                                                         coalesce(CollectionsPostOnboardingSecuritizedCalc,0)
#         --                                                         + coalesce(CollectionsPostOnboardingThirdPartiesCalc,0)
#         --                                                         + coalesce(CollectionsPreOnboardingThirdParties,0)
#         --                                                         + coalesce(CollectionsPreOnboardingSecuritized,0)
#         --                                                         ) 
#         --   ELSE Null END AS NPVCalc
#         , CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV_Calc ELSE Null END AS NPVCalc
#         , CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV ELSE Null END AS TotalNPV    
#         , CAST(a.CollectionsPostOnboardingSecuritizedCalc AS FLOAT) AS CollectionsPostOnboardingSecuritizedCalcFinal
#         , CAST(a.CollectionsPostOnboardingSecuritizedCalcTotal AS FLOAT) CollectionsPostOnboardingSecuritizedCalcSum
#         , CAST(a.CollectionsPostOnboardingThirdPartiesCalc AS FLOAT) AS CollectionsPostOnboardingThirdPartiesCalcFinal
#         , CASE 
#             WHEN a.PortfolioName LIKE '%Cosmos%' THEN 1
#             WHEN a.PortfolioName LIKE '%Galaxy%' THEN 1
#             ELSE 0
#           END AS IsPortfolioSecuritizedFlag
#     FROM 
#         df_investorbp_combined a
#     WHERE a.key IN ('GLC0002985677','GLC0000399920', 'GLC0000389330')
#   ), 
#   RS AS (
#     SELECT  
#           F.PortfolioName,
#           F.key AS CDI,
#           F.GBV,
#           F.Year,
#           -- SUM(R.TotalGrossAmount) OVER (PARTITION BY R.key ORDER BY R.Year) AS GrossAmount,  -- keep for testing GrossAmount incremental total 
#           SUM(F.TotalGrossAmount) OVER (PARTITION BY F.key ORDER BY F.Year)  
#             - COALESCE(F.CollectionsPreOnboardingSecuritized, 0)   
#             - COALESCE(F.CollectionsPostOnboardingSecuritizedCalcSum, 0) AS GrossAmountCalc,
#           F.TotalGrossAmount,
#           F.DurationCalc AS Duration,
#           F.WAL AS AverageLife,
#           F.NPVCalc AS NPV,
#           F.TotalNPV,
#           try_divide(F.NPVCalc, F.GBV) AS `NPV/GBV`,
#           F.CollectionsPreOnboardingSecuritized,
#           F.CollectionsPreOnboardingThirdParties AS CollectionsPreOnboardingThirdParties,
#           F.CollectionsPostOnboardingSecuritizedCalcFinal AS CollectionsPostOnboardingSecuritized,
#           F.CollectionsPostOnboardingSecuritizedCalcSum AS CollectionsPostOnboardingSecuritizedCalcTotal,
#           F.CollectionsPostOnboardingThirdPartiesCalcFinal AS CollectionsPostOnboardingThirdParties,   
#           F.IsPortfolioSecuritizedFlag,    
#           F.BusinessDate
#     FROM Final F
#     GROUP BY ALL),
#   PrevGrossAmount AS (
#     SELECT 
#           R1.CDI,
#           R1.Year,
#           LAST_VALUE(R1.GrossAmountCalc) OVER(ORDER BY R1.CDI, R1.Year ASC) AS PrevGrossAmount
#     FROM RS R1
#     WHERE R1.Year = YEAR(R1.BusinessDate)),
#   TotalGrossAmountCalc AS (
#     SELECT 
#         R2.CDI,
#         R2.Year,
#         SUM(R2.TotalGrossAmount) OVER(PARTITION BY R2.CDI ORDER BY R2.Year) AS TotalGrossAmountCalc
#     FROM RS R2
#     WHERE R2.Year > YEAR(R2.BusinessDate)
#   ) 
#   SELECT R.PortfolioName,
#         R.CDI,
#         R.GBV,
#         CAST(R.Year AS VARCHAR(4)) AS Year,
#         CASE WHEN R.Year > YEAR(R.BusinessDate) THEN B.PrevGrossAmount + C.TotalGrossAmountCalc
#             ELSE R.GrossAmountCalc 
#         END AS GrossAmount,
#         R.TotalGrossAmount,
#         R.Duration,
#         R.AverageLife,
#         R.NPV,
#         R.TotalNPV,
#         `NPV/GBV`,
#         R.CollectionsPreOnboardingSecuritized,
#         R.CollectionsPreOnboardingThirdParties,
#         R.CollectionsPostOnboardingSecuritized,
#         R.CollectionsPostOnboardingSecuritizedCalcTotal,
#         R.CollectionsPostOnboardingThirdParties,   
#         R.IsPortfolioSecuritizedFlag,    
#         R.BusinessDate
#   FROM RS R 
#     LEFT JOIN PrevGrossAmount B ON R.CDI = B.CDI 
#     LEFT JOIN TotalGrossAmountCalc C ON R.CDI = C.CDI AND R.Year = C.Year 
# """)

# # Get current columns
# current_columns = final_df_investorbp.columns

# # Move 'BusinessDate' to the last position
# new_columns = [col for col in current_columns if col != "BusinessDate"] + ["BusinessDate"]

# # Select the new column order
# final_df_investorbp = final_df_investorbp.select(new_columns)

# COMMAND ----------

# DBTITLE 1,Final Query for Investor Optimized
# Define the final query with NULL-safe GrossAmount calculation
final_df_investorbp = spark.sql(f"""
WITH Final AS (
    SELECT 
        a.PortfolioName,
        a.key,
        a.GBV,
        a.Year,
        -- a.Month,
        a.BusinessDate,
        a.Duration,
        a.WAL,
        -- a.NPV_Calc,
        -- a.NPV,
        SUM(a.Amount) OVER (PARTITION BY a.key, a.Year ) AS TotalGrossAmount,
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.Duration ELSE Null END AS DurationCalc,  
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV_Calc ELSE Null END AS NPVCalc,
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV ELSE Null END AS TotalNPV,    
        CAST(a.CollectionsPostOnboardingSecuritizedCalc AS FLOAT) AS CollectionsPostOnboardingSecuritizedCalcFinal,
        CAST(a.CollectionsPostOnboardingSecuritizedCalcTotal AS FLOAT) CollectionsPostOnboardingSecuritizedCalcSum,
        CAST(a.CollectionsPostOnboardingThirdPartiesCalc AS FLOAT) AS CollectionsPostOnboardingThirdPartiesCalcFinal,
        CASE 
          WHEN a.PortfolioName LIKE '%Cosmos%' THEN 1
          WHEN a.PortfolioName LIKE '%Galaxy%' THEN 1
          ELSE 0
        END AS IsPortfolioSecuritizedFlag
    FROM 
        df_investorbp_combined a
    WHERE a.key IN ('GLC0002985677','GLC0000399920', 'GLC0000389330')
  ), 
  RS AS (
    SELECT  
          F.PortfolioName,
          F.key AS CDI,
          F.GBV,
          F.Year,
          -- SUM(R.TotalGrossAmount) OVER (PARTITION BY R.key ORDER BY R.Year) AS GrossAmount,  -- keep for testing GrossAmount incremental total 
          SUM(F.TotalGrossAmount) OVER (PARTITION BY F.key ORDER BY F.Year)  
            - COALESCE(F.CollectionsPreOnboardingSecuritized, 0)   
            - COALESCE(F.CollectionsPostOnboardingSecuritizedCalcSum, 0) AS GrossAmountCalc,
          F.TotalGrossAmount,
          F.DurationCalc AS Duration,
          F.WAL AS AverageLife,
          F.NPVCalc AS NPV,
          F.TotalNPV,
          try_divide(F.NPVCalc, F.GBV) AS `NPV/GBV`,
          F.CollectionsPreOnboardingSecuritized,
          F.CollectionsPreOnboardingThirdParties AS CollectionsPreOnboardingThirdParties,
          F.CollectionsPostOnboardingSecuritizedCalcFinal AS CollectionsPostOnboardingSecuritized,
          F.CollectionsPostOnboardingSecuritizedCalcSum AS CollectionsPostOnboardingSecuritizedCalcTotal,
          F.CollectionsPostOnboardingThirdPartiesCalcFinal AS CollectionsPostOnboardingThirdParties,   
          F.IsPortfolioSecuritizedFlag,    
          F.BusinessDate
    FROM Final F
    GROUP BY ALL),
  PrevGrossAmount AS (
    SELECT 
          R1.CDI,
          R1.Year,
          LAST_VALUE(R1.GrossAmountCalc) OVER(ORDER BY R1.CDI, R1.Year ASC) AS PrevGrossAmount
    FROM RS R1
    WHERE R1.Year = YEAR(R1.BusinessDate)),
  TotalGrossAmountCalc AS (
    SELECT 
        R2.CDI,
        R2.Year,
        SUM(R2.TotalGrossAmount) OVER(PARTITION BY R2.CDI ORDER BY R2.Year) AS TotalGrossAmountCalc
    FROM RS R2
    WHERE R2.Year > YEAR(R2.BusinessDate)
  ) 
  SELECT R.PortfolioName,
        R.CDI,
        R.GBV,
        CAST(R.Year AS VARCHAR(4)) AS Year,
        CASE WHEN R.Year > YEAR(R.BusinessDate) THEN B.PrevGrossAmount + C.TotalGrossAmountCalc
            ELSE R.GrossAmountCalc 
        END AS GrossAmount,
        R.TotalGrossAmount,
        R.Duration,
        R.AverageLife,
        R.NPV,
        R.TotalNPV,
        `NPV/GBV`,
        R.CollectionsPreOnboardingSecuritized,
        R.CollectionsPreOnboardingThirdParties,
        R.CollectionsPostOnboardingSecuritized,
        R.CollectionsPostOnboardingSecuritizedCalcTotal,
        R.CollectionsPostOnboardingThirdParties,   
        R.IsPortfolioSecuritizedFlag,    
        R.BusinessDate
  FROM RS R 
    LEFT JOIN PrevGrossAmount B ON R.CDI = B.CDI 
    LEFT JOIN TotalGrossAmountCalc C ON R.CDI = C.CDI AND R.Year = C.Year 
""")

# Get current columns
current_columns = final_df_investorbp.columns

# Move 'BusinessDate' to the last position
new_columns = [col for col in current_columns if col != "BusinessDate"] + ["BusinessDate"]

# Select the new column order
final_df_investorbp = final_df_investorbp.select(new_columns)

# COMMAND ----------

# Optimize source data partitioning first (if using Delta Lake)
spark.sql(f"OPTIMIZE df_investorbp_combined ZORDER BY (key, Year)")

# Cache frequently used columns if reused multiple times
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB for broadcast joins

final_df_investorbp = spark.sql(f"""
WITH Final AS (
    /* Partition pruning & predicate pushdown */
    SELECT 
        a.PortfolioName,
        a.key,
        a.GBV,
        a.Year,
        a.Month,
        a.BusinessDate,
        a.Duration,
        a.WAL,
        a.NPV_Calc,
        a.NPV,
        a.CollectionsPostOnboardingSecuritizedCalc,
        a.CollectionsPostOnboardingSecuritizedCalcTotal,
        a.CollectionsPostOnboardingThirdPartiesCalc,
        a.CollectionsPreOnboardingSecuritized,
        a.CollectionsPreOnboardingThirdParties,
        SUM(a.Amount) OVER key_year AS TotalGrossAmount,
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.Duration END AS DurationCalc,
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV_Calc END AS NPVCalc,
        CASE WHEN a.Month >= left(a.BusinessDate,7) THEN a.NPV END AS TotalNPV,
        CASE 
            WHEN a.PortfolioName LIKE '%Cosmos%' THEN 1
            WHEN a.PortfolioName LIKE '%Galaxy%' THEN 1
            ELSE 0
        END AS IsPortfolioSecuritizedFlag
    FROM df_investorbp_combined a
    WHERE a.key IN ('GLC0002985677','GLC0000399920', 'GLC0000389330')
    WINDOW key_year AS (PARTITION BY key, Year)
), 
RS AS (
    SELECT  
        F.PortfolioName,
        F.key AS CDI,
        F.GBV,
        F.Year,
        SUM(F.TotalGrossAmount) OVER key_order 
            - COALESCE(F.CollectionsPreOnboardingSecuritized, 0)   
            - COALESCE(F.CollectionsPostOnboardingSecuritizedCalcSum, 0) AS GrossAmountCalc,
        F.TotalGrossAmount,
        F.DurationCalc AS Duration,
        F.WAL AS AverageLife,
        F.NPVCalc AS NPV,
        F.TotalNPV,
        F.NPVCalc / F.GBV AS `NPV/GBV`,
        F.CollectionsPreOnboardingSecuritized,
        F.CollectionsPreOnboardingThirdParties,
        F.CollectionsPostOnboardingSecuritizedCalcFinal,
        F.CollectionsPostOnboardingSecuritizedCalcSum,
        F.CollectionsPostOnboardingThirdPartiesCalcFinal,
        F.IsPortfolioSecuritizedFlag,    
        F.BusinessDate
    FROM Final F
    WINDOW key_order AS (PARTITION BY key ORDER BY Year)
), 
PrevGrossAmount AS (
    SELECT 
        CDI,
        Year,
        LAST_VALUE(GrossAmountCalc) OVER key_year AS PrevGrossAmount
    FROM RS
    WHERE Year = YEAR(BusinessDate)
    WINDOW key_year AS (PARTITION BY CDI ORDER BY Year)
), 
TotalGrossAmountCalc AS (
    SELECT 
        CDI,
        Year,
        SUM(TotalGrossAmount) OVER key_order AS TotalGrossAmountCalc
    FROM RS
    WHERE Year > YEAR(BusinessDate)
    WINDOW key_order AS (PARTITION BY CDI ORDER BY Year)
) 
SELECT /*+ BROADCASTJOIN(B) */
    R.PortfolioName,
    R.CDI,
    R.GBV,
    CAST(R.Year AS VARCHAR(4)) AS Year,
    COALESCE(B.PrevGrossAmount, 0) + COALESCE(C.TotalGrossAmountCalc, 0) AS GrossAmount,
    R.TotalGrossAmount,
    R.Duration,
    R.AverageLife,
    R.NPV,
    R.TotalNPV,
    `NPV/GBV`,
    R.CollectionsPreOnboardingSecuritized,
    R.CollectionsPreOnboardingThirdParties,
    R.CollectionsPostOnboardingSecuritizedCalcFinal AS CollectionsPostOnboardingSecuritized,
    R.CollectionsPostOnboardingSecuritizedCalcSum,
    R.CollectionsPostOnboardingThirdPartiesCalcFinal AS CollectionsPostOnboardingThirdParties,   
    R.IsPortfolioSecuritizedFlag,    
    R.BusinessDate
FROM RS R 
LEFT JOIN PrevGrossAmount B USING (CDI)
LEFT JOIN TotalGrossAmountCalc C USING (CDI, Year)
""")

# Optimize column order without shuffling data
final_df_investorbp = final_df_investorbp.selectExpr(*[col for col in final_df_investorbp.columns if col != "BusinessDate"], "BusinessDate")


# COMMAND ----------

# %sql
# SELECT * FROM df_investorbp_combined WHERE key = 'GLC0000389330'

# COMMAND ----------

#  display(final_df_investorbp.filter(final_df_investorbp.CDI=='GLC0002985677'))
#  display(final_df_investorbp.filter(final_df_investorbp.CDI=='GLC0000389330').select("CDI", "Year", "GrossAmount", "TotalGrossAmount"", Duration").distinct().limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process and Save results

# COMMAND ----------

# DBTITLE 1,Definition of function "process_and_save_result"
# from delta.tables import DeltaTable
# from pyspark.sql import DataFrame

# def process_and_save_result(result_df: DataFrame, curated_table_path: str, run_date: str) -> None:
#     """
#     Processes and saves the result dataframe to a Delta table.

#     Parameters:
#     - result_df: DataFrame to be saved
#     - curated_table_path: Path to the Delta table
#     - run_date: The run date for deletion criteria

#     Returns:
#     - None
#     """
#     # Initialize DeltaTable object for the given curated_table_path
#     if DeltaTable.isDeltaTable(spark, curated_table_path):
#         delta_table = DeltaTable.forPath(spark, curated_table_path)
        
#     # Delete data for the specific run_date before inserting new data
#         delta_table.delete(f"BusinessDate = '{run_date}'")

#     result_df.write \
#         .format("delta") \
#         .partitionBy("BusinessDate") \
#         .mode("overwrite") \
#         .option("overwriteSchema", "true") \
#         .saveAsTable(curated_table_path) # Save using Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save results for CepalBP table

# COMMAND ----------

# Ensure table_path to Delta table is correctly formatted for cephalbp
# table_path = f"{target_area}.{table_name_cepalbp}"

# # Check if the table_path and final_df_cepalbp are valid
# if table_path and final_df_cepalbp:
#     # Delete records from the Delta table where BusinessDate matches the specified date
#     spark.sql(f"""
#             DELETE FROM {table_path}
#             WHERE BusinessDate = '{Business_date}'
#             """)    

#     # Write to final table CreditMemoEnhancementCepalBP
#     final_df_cepalbp.write \
#             .format("delta") \
#             .partitionBy("BusinessDate") \
#             .mode("append") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(table_path) # Save using Delta Lake
# else:
#     raise ValueError("Invalid table path or DataFrame")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save results for InvestorBP table

# COMMAND ----------

# DBTITLE 1,Write in small Batches Partitioning
# table_path = f"{target_area}.{table_name_investorbp}"

# if table_path and final_df_investorbp:
#     # Repartition DataFrame for parallelism (e.g., 100 partitions)
#     num_partitions = 100  # Tune this based on your cluster and data size
#     df_repartitioned = final_df_investorbp.repartition(num_partitions)

#     df_repartitioned.write \
#         .format("delta") \
#         .partitionBy("BusinessDate") \
#         .mode("append") \
#         .option("overwriteSchema", "true") \
#         .saveAsTable(table_path)
# else:
#     raise ValueError("Invalid table path or DataFrame")


# COMMAND ----------

# DBTITLE 1,Write in small Batches Pyspark SQL
# from pyspark.sql import DataFrame

# table_path = f"{target_area}.{table_name_investorbp}"

# def write_in_batches(df: DataFrame, table_path: str, batch_size: int = 10000):
#     total_rows = df.count()
#     num_batches = (total_rows + batch_size - 1) // batch_size

#     # Add a monotonically increasing id to help with slicing
#     df_with_id = df.withColumn("__row_id", F.monotonically_increasing_id())

#     for batch_num in range(num_batches):
#         start_id = batch_num * batch_size
#         end_id = start_id + batch_size

#         # Filter for the current batch
#         batch_df = df_with_id.filter(
#             (F.col("__row_id") >= start_id) & (F.col("__row_id") < end_id)
#         ).drop("__row_id")

#         batch_df.write \
#             .format("delta") \
#             .partitionBy("BusinessDate") \
#             .mode("append") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(table_path)

#         print(f"Batch {batch_num+1}/{num_batches} written to {table_path}")

# if table_path and final_df_investorbp:
#     import pyspark.sql.functions as F
#     # Uncomment if you want to delete existing records for the business date
#     # spark.sql(f"DELETE FROM {table_path} WHERE BusinessDate = '{Business_date}'")

#     write_in_batches(final_df_investorbp, table_path, batch_size=10000)
# else:
#     raise ValueError("Invalid table path or DataFrame")


# COMMAND ----------

# DBTITLE 1,Write in Small Batches
# # Ensure table_path to Delta table is correctly formatted for investorbp
# table_path = f"{target_area}.{table_name_investorbp}"

# # Check if the table_path and final_df_cepalbp are valid
# if table_path and final_df_investorbp:
#     batch_size = 1000  # Adjust based on your cluster's capacity
#     total_rows = final_df_investorbp.count()
#     num_batches = (total_rows + batch_size - 1) // batch_size

#     for batch_num in range(num_batches):
#         start = batch_num * batch_size
#         batch_df = final_df_investorbp.limit(batch_size).offset(start)

#         # Write to final table CreditMemoEnhancementInvestorBP
#         batch_df.write \
#             .format("delta") \
#             .partitionBy("BusinessDate") \
#             .mode("append") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(table_path)
        
#     print(f"Successfully wrote {num_batches} batches to {table_path}")
# else:
#     raise ValueError("Invalid table path or DataFrame")

# COMMAND ----------

# Ensure table_path to Delta table is correctly formatted for investorbp
# table_path = f"{target_area}.{table_name_investorbp}"

# # Check if the table_path and final_df_cepalbp are valid
# if table_path and final_df_investorbp:
#     # Delete records from the Delta table where BusinessDate matches the specified date
# #     spark.sql(f"""
# #             DELETE FROM {table_path}
# #             WHERE BusinessDate = '{Business_date}'
# #             """)        

#     # Write to final table CreditMemoEnhancementInvestorBP
#     final_df_investorbp.write \
#             .format("delta") \
#             .partitionBy("BusinessDate") \
#             .mode("append") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(table_path) # Save using Delta Lake
# else:
#     raise ValueError("Invalid table path or DataFrame")
