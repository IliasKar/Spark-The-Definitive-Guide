# Databricks notebook source
# DBTITLE 1,FIND HIVE METASTORE LANDED AREA TABLES COUNTS
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException  # Import statement for AnalysisException
import os

'''
the counts.csv comes from the output of the procedure below:
SEDEV > StoredProcedure [stg].[RecordCountProcess]
Daily task in agent jobs [SEDEV-RecordProcess]

Then with the following query we produce the counts.csv =>

SELECT [businessDate]
      ,replace([entityKey], '.', '_')  [entityKey]
      ,[schemaName]
      ,[records]
      ,[ingestionDate]
  FROM [SEDEV].[stg].[LandingRecords]

'''



businessDate = dbutils.widgets.get("business_date")
env_prefix = os.environ["S3PREFIX"] 

# List of databases to query
databases = ['`{env_prefix}-cdh`.landed'.format(env_prefix=env_prefix),]

# Collect tables from each database
tables = [
    f"{row['database']}.`{row['tableName']}`"
    for db_rows in [
        spark.sql(f'show tables in {db}').collect() for db in databases
    ]
    for row in db_rows
]

# Prepare data for creating the DataFrame
allx = []
for x in tables:
    z1 = x.replace("`", "")
    z = z1.split(".")
    allx.append({"database": z[0], "tableName": z[1]})

# Define schema for tables_df
schema = StructType([
    StructField("database", StringType(), True),
    StructField("tableName", StringType(), True)
])

# Create DataFrame from the collected data
tables_df = spark.createDataFrame(data=allx, schema=schema)



def Calc_table_count_records_per_businessdate(database, table_name):
    # Define schema for result_df
    result_schema = StructType([
        StructField("tableName", StringType(), True),
        StructField("counter", IntegerType(), True)
    ])

    # Create an empty DataFrame with the specified schema
    empty_df = spark.createDataFrame(data=allx, schema=result_schema)

    try:
        # Query to get the count for the specified business date
        querystr = f"SELECT '{table_name}' as tableName, count(*) as counter FROM `{database}`.{table_name} WHERE BusinessDate = '{businessDate}'"
        result_df = spark.sql(querystr)
        return result_df
    except AnalysisException as ae:
        # Handle the case where the table does not exist
        if "Table or view not found" in str(ae):
            print(f"{database}.{table_name} does not exist.")
        else:
            # Handle other AnalysisException errors
            print(f"An error occurred: {str(ae)}")
        return empty_df

# Apply the function to each row in the DataFrame
result_dfs = [Calc_table_count_records_per_businessdate(row['database'], row['tableName']) for row in tables_df.collect()]

# Combine the DataFrames into a single DataFrame
combined_result_df = result_dfs[0]
for result_df in result_dfs[1:]:
    combined_result_df = combined_result_df.union(result_df)

# Filter out rows where counter is null
combined_result_df = combined_result_df.filter(combined_result_df["counter"].isNotNull())

# Display the combined result DataFrame
display(combined_result_df)


# COMMAND ----------

# DBTITLE 1,LOAD COUNTS FROM (replica) [SEDEV].[stg].[LandingRecords] >> A CSV UPLOADED IN GIT 
# Read counts data from a CSV file into a DataFrame
counts_dwhcore_df = spark.read.format("csv").option("delimiter", ";").option("header", True).load(f"file:{os.getcwd()}/counts.csv")

# Display the DataFrame containing counts data
display(counts_dwhcore_df)

# COMMAND ----------

# DBTITLE 1,COMPARE AND FIND THE MISMATCHES
# Filter the counts_dwhcore_df by businessDate and select required columns
counts_dwhcore_df_filtered = counts_dwhcore_df \
    .filter(counts_dwhcore_df['businessDate'] == f'{businessDate}') \
    .selectExpr('entityKey as tableName', 'records as counterdwh')

# Compare the two DataFrames using join operation
comparison_df = combined_result_df.join(counts_dwhcore_df_filtered, 'tableName', 'outer')

# Add a column to indicate whether the counts match or not
comparison_df = comparison_df.withColumn('match', comparison_df['counter'] == comparison_df['counterdwh'])

# Display comparison results: matched and mismatched table counts
differences_found_df = comparison_df.filter(comparison_df['match'] == 'false')

# COMMAND ----------

# DBTITLE 1,DISPLAY THE MISMATCHES
display(comparison_df)
