from pyspark.sql import functions as F
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

from pyspark.sql.window import Window

import boto3
from botocore.exceptions import NoCredentialsError

# from pyspark import dbutils
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def calculate_WAL_NPV(
    initial_data,   # Spark DataFrame: 'PortfolioId', 'key', 'Date', 'Amount'
    portfolio_IRR   # Spark DataFrame: 'PortfolioId', 'DiscountRate'
):
    """
    Calculates WAL and NPV for each PortfolioId and key using distributed Spark operations.
    Returns a DataFrame: columns ['PortfolioId', 'key', 'WAL', 'NPV']
    """
    try:
        # 1. Join IRR to initial_data (distributed, avoids collect)
        data_with_irr = initial_data.join(portfolio_IRR, on="PortfolioId", how="inner")

        # 2. Compute min_date per PortfolioId
        min_dates = data_with_irr.groupBy("PortfolioId").agg(F.min("Date").alias("min_date"))
        data = data_with_irr.join(min_dates, on="PortfolioId", how="inner")

        # 3. Compute 'years' and 'product'
        data = data.withColumn(
            "years", (F.datediff("Date", "min_date") + F.lit(1)) / F.lit(365)
        ).withColumn(
            "product", F.col("Amount") * F.col("years")
        )

        # 4. Filter for Amount > 0 for WAL and NPV
        data_pos = data.filter(F.col("Amount") > 0)

        # 5. WAL calculation: group by PortfolioId, key
        wal = (
            data_pos.groupBy("PortfolioId", "key")
            .agg(
                F.sum("Amount").alias("Amount"),
                F.sum("product").alias("product")
            )
            .withColumn("WAL", F.round(F.expr("try_divide(product, Amount)"), 3))
            .select("PortfolioId", "key", "WAL")
        )

        # 6. NPV calculation
        # Calculate months since min_date
        data_pos = data_pos.withColumn(
            "months_since_start",
            (F.datediff("Date", "min_date") / 30.4375) # average number of days in a month.
        )
        # NPV per row
        data_pos = data_pos.withColumn(
            "NPV",
            F.expr(
                "try_divide(Amount, pow(1 + (DiscountRate / 12), months_since_start))"
            )
        )
        npv = (
            data_pos.groupBy("PortfolioId", "key")
            .agg(F.sum("NPV").alias("NPV"))
            .select("PortfolioId", "key", "NPV")
        )

        # 7. Merge WAL and NPV
        wal_npv = wal.join(npv, on=["PortfolioId", "key"], how="outer")

        # 8. Ensure all debtors (keys) are represented
        all_debtors = initial_data.select("PortfolioId", "key").distinct()
        final_table = (
            all_debtors.join(wal_npv, on=["PortfolioId", "key"], how="left")
            .fillna(0, subset=["WAL", "NPV"])
        )

        return final_table.select("PortfolioId", "key", "WAL", "NPV").distinct()

    except Exception as e:
        print(f"Calculation failed! The following error occurred:\n{e}")
        return None

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
            F.expr("try_divide(YearlyAmount, pow(1 + (DiscountRate / 12), MonthsDiff))")
        )
        print(f'Running calculation for NPVYearly...')

        # Define a window partitioned by 'PortfolioId', 'key', and 'Year'
        window_spec = Window.partitionBy('PortfolioId', 'key', 'Year')

        # Calculate NPVYearly using the window function
        data_with_npv_yearly = data_with_npv.withColumn(
            'NPVYearly',
            F.coalesce(F.sum('NPV').over(window_spec), F.lit(0))
        )

        # Drop the 'NPV' column if it's not needed
        final_table = data_with_npv_yearly.drop('NPV')

        print(f'NPVYearly calculation completed!')

        return final_table

    except Exception as e:
        print(f"Calculation failed! The following error occurred:\n{e}")
        return None

def calculate_duration(df):
    grouped = df.groupBy(['key', 'portfolioid'])
    duration = grouped.agg(
            F.expr("try_divide(sum(npvyearly * monthsdiff), 12 * sum(npvyearly))"
             ).alias('Duration')
    )
    df = df.join(duration, on=['key', 'portfolioid'], how='left')
    return df

def get_cm_exposures_by_business_date(dexposure_df, business_date):
    # PK[19/06/2025]-Implements the logic of the SQL function get_cm_exposures_by_business_date in PySpark.
    # :param dexposure_df: Input DataFrame (all columns from p-cdh.derivative.dexposure)
    # :param business_date: The business date to filter on (string 'YYYY-MM-DD' or datetime.date)
    # :return: DataFrame with the required exposures
  
    # Find the latest BusinessDate <= business_date for each ExposureKey
    latest_dates_df = (
        dexposure_df
        .filter(F.col("BusinessDate") <= F.lit(business_date))
        .groupBy("ExposureKey")
        .agg(F.max("BusinessDate").alias("BusinessDate"))
    )

    # 2. Join with the main table to get all columns where IsActiveFlagInt = 0
    exposures_inactive = (
        latest_dates_df
        .join(
            dexposure_df,
            on=["ExposureKey", "BusinessDate"],
            how="inner"
        )
        .filter(F.col("IsActiveFlagInt") == 0)
    )

    # All records for BusinessDate == business_date and IsActiveFlagInt = 1
    exposures_active = (
        dexposure_df
        .filter(
            (F.col("BusinessDate") == F.lit(business_date)) &
            (F.col("IsActiveFlagInt") == 1)
        )
    )

    # Union the two DataFrames, ensuring column order and types match
    result_df = exposures_inactive.unionByName(exposures_active)

    return result_df

# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
   
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
 
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

def checkpoint_df(df, temp_table_path):
    """
    Materializes a DataFrame to a checkpoint temp table to stop logical plan from growing too large.
    Reads the table back into a dataframe.
    """

    #save as table
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(temp_table_path)

    #read checkpoint table
    df_reload = spark.table(temp_table_path)

    return df_reload

# checks whether the required tables have finished running for the given BusinessDate
def check_data_availability(dfs, businessdate):
    if any(df.limit(1).count() == 0 for df in dfs):
        raise ValueError(f"ERROR: One or more of the required tables is empty for BusinessDate = '{businessdate}'.")
    else:
        return f"Data is available for BusinessDate = '{businessdate}'."