# COMMAND ----------

import json
import random
from datetime import datetime
import logging
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, to_timestamp, date_sub, date_add, date_format, col
import time
# COMMAND ----------

#for item in sc._conf.getAll():
#    print(item)

# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Now you can use dbutils
from databricks.sdk.runtime import *

# COMMAND ----------

def get_running_env():
    spark = SparkSession.builder.appName("GetWorkspaceName").getOrCreate()

    # List all Spark configuration properties to see if any contain the workspace information
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "Not Available")
    #print(workspace_url)
    if workspace_url.startswith("dbc-1c3bb"):
        return "dev"
    elif workspace_url.startswith("dbc-51fda"):
        return "qa"
    elif workspace_url.startswith("dbc-7a36a"):
        return "prd"

# COMMAND ----------

env = get_running_env()
print(f"Running in Environment: {env}")
SCHEMATA = f"{env}-LoadToLanding:staging:schemaList"

# COMMAND ----------

def get_value_from_redis(value, redis_env, redis_host, redis_port):
    env = redis_env
    print(f"Running in Environment: {env}")
    #redis_port = 6379
    spark = SparkSession.builder.appName("RedisRead").getOrCreate()
    r = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
    keys = r.keys(value)
    data = [(key, r.get(key)) for key in keys]
    df = spark.createDataFrame(data, ["key", "value"])
    df_r = df.select("value").collect()
    schema_row = df.select("value").first()
    #print(f'Schema list: {schema_row}')
    if schema_row:
        redis_value = schema_row.value
     #   print(f'Redis value: {redis_value}')
        return redis_value
    else:
        logging.error(f"No value found in redis for {value}.")


def get_table_datamode(table_name, env_redis, redis_host, redis_port):
    """
    Retrieves the groups of a schema from Redis and returns them as a list.

    Args:
        schema_name (str): The name of the schema.

    Returns:
        list: A list of groups associated with the specified schema.
    """

    env = env_redis
    redis_host = redis_host
    redis_port = redis_port
    datamode = f"{env}-Landed:DataMode:{table_name}"
    table_datamode = get_value_from_redis(datamode, env, redis_host, redis_port)
    return table_datamode


# COMMAND ----------

def get_incremental_table_keys(table_name, env_redis, redis_host, redis_port):
    """
    Retrieves the groups of a schema from Redis and returns them as a list.

    Args:
        schema_name (str): The name of the schema.

    Returns:
        list: A list of groups associated with the specified schema.
    """

    env = env_redis
    redis_host = redis_host
    redis_port = redis_port
    keys = f"{env}-Landed:Keys:{table_name}"
    incremental_table_keys = get_value_from_redis(keys, env, redis_host, redis_port).split(",")
    return incremental_table_keys

def create_staging_delta_table(
    spark: SparkSession,
    table_path: str,
) -> None:
  
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {table_path} ( 
            id INT
       )
        USING DELTA
    """
    sql_query = (
        sql_query
        + """
        TBLPROPERTIES (
            'delta.logRetentionDuration' = 'interval 5 weeks',
            'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
            'delta.minReaderVersion' = '2',
            'delta.isolationLevel' = 'Serializable',
            'delta.minWriterVersion' = '5',
            'delta.columnMapping.mode' = 'name')
    """
    )
    

    spark.sql(sql_query)
    spark.sql(
            f"""REPAIR TABLE {table_path} SYNC METADATA"""   
        )

def load_data_to_staging(
    spark: SparkSession,
    cdh_location_stg: str,
    host: str,
    port: str,
    database_name: str,
    jdbcUserName: str,
    jdbcPassword: str,
    table_str: str,
    business_date: str = None,
    schema_name: str = None,
    group_name: str = None,
    ingestion_date: str = None,
    env_prefix: str = None,
):
    # Extract task_name for logging.
    # The staging table is named "{database_name}_{schema}_{table}" (flat), e.g.
    # itstaging_cepalplatform_WhlLiquidationValueOwnerInput. For task logging we
    # want the canonical "{schema}_{table}" (cepalplatform_WhlLiquidationValueOwnerInput)
    # so it matches expected_tasks (redis strips the db prefix) and the
    # landing/landed areas (which already use the canonical name).
    _flat_name = cdh_location_stg.split(".")[-1] if "." in cdh_location_stg else cdh_location_stg
    if database_name and _flat_name.startswith(f"{database_name}_"):
        task_name = _flat_name[len(database_name) + 1:]
    else:
        task_name = _flat_name
    _log_enabled = business_date is not None

    if _log_enabled:
        try:
            from pipeline_log_helpers import log_task_start, log_task_end
            log_task_start(spark, "staging", business_date, task_name,
                          schema_name=schema_name, group_name=group_name,
                          env_prefix=env_prefix)
        except Exception as log_err:
            print(f"[WARNING] Task log start failed (non-blocking): {log_err}")

    try:
        # Drop the staging table if it exists
        spark.sql(f"DROP TABLE IF EXISTS {cdh_location_stg}")
        logging.info(f"Dropped staging table if it existed: {cdh_location_stg}")

        full_table_name = f"{database_name}.{table_str}"

        table_counts = (
            spark.read.format("sqlserver")
                .option("host", host)
                .option("port", port)
                .option("encrypt", "false")
                .option("trustServerCertificate", "true")
                .option("dbtable", full_table_name) 
                .option("database", database_name)
                .option("fetchsize","100000")
                .option("user", jdbcUserName)
                .option("password", jdbcPassword)
                .option("fetchsize", "100000")
                .load()
        ).count()
        if table_counts> 1000000:
            # SQL query to get the first column of the table
            first_column_query = f"""
                SELECT COLUMN_NAME 
                FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
                WHERE  UPPER('{database_name}' + '.' + TABLE_SCHEMA + '.' + TABLE_NAME) = '{full_table_name}'
                ORDER BY ORDINAL_POSITION
                OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
            """
            # Execute the query to get the first column name
            first_column_df = (
                spark.read.format("sqlserver")
                    .option("host", host)
                    .option("port", port)
                    .option("encrypt", "false")
                    .option("trustServerCertificate", "true")
                    .option("dbtable", f"({first_column_query}) as query")
                    .option("database", database_name)
                    .option("user", jdbcUserName)
                    .option("password", jdbcPassword)
                    .load()
            )
            # Extract the first column name
            first_column = first_column_df.first()["COLUMN_NAME"]

            
            first_column_query = f"""
                SELECT MIN(ABS(CHECKSUM({first_column}))) min_value ,MAX(ABS(CHECKSUM({first_column}))) max_value 
                FROM {full_table_name}
            """

            min_max_df = (
                        spark.read.format("sqlserver")
                            .option("host", host)
                            .option("port",  "1433")
                            .option("encrypt", "false")
                            .option("trustServerCertificate", "true")
                            .option("dbtable", f"({first_column_query}) AS query")
                            .option("database", database_name)
                            .option("user", jdbcUserName)
                            .option("password", jdbcPassword)
                            .option("fetchsize", "100000")
                            .load()
            ) 


            min_value = min_max_df.first()["min_value"]
            max_value = min_max_df.first()["max_value"]
 
            partition_column = "unique_partition_column"
            query = f"""
                select ABS(CHECKSUM({first_column})) as {partition_column}, * 
                from {full_table_name}  
                    """

            # Load the data from the SQL Server
            dwh_tbl_df = (
                spark.read.format("sqlserver")
                    .option("host", host)
                    .option("port", port)
                    .option("encrypt", "false")
                    .option("trustServerCertificate", "true")
                    .option("dbtable", f"({query}) as query") 
                    .option("database", database_name)
                    .option("user", jdbcUserName)
                    .option("password", jdbcPassword)
                    .option("fetchsize", "10000")
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", min_value)
                    .option("upperBound", max_value)
                    .option("numPartitions", 10)
                    .load()
            )
            dwh_tbl_df = dwh_tbl_df.drop(partition_column)
        else: 
            dwh_tbl_df = (
                spark.read.format("sqlserver")
                    .option("host", host)
                    .option("port", port)
                    .option("encrypt", "false")
                    .option("trustServerCertificate", "true")
                    .option("dbtable", full_table_name) 
                    .option("database", database_name)
                    .option("user", jdbcUserName)
                    .option("password", jdbcPassword)
                    .load()
            )

        logging.info(f"Loaded data from SQL Server into DataFrame with {table_counts} records.")
        
        # Process and write to Delta Lake staging table
        schema_df = dwh_tbl_df.schema.simpleString()

        if ' ' in schema_df:
            if not spark.catalog.tableExists(cdh_location_stg):
                create_staging_delta_table(spark, cdh_location_stg)

            try:
                dwh_tbl_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(cdh_location_stg)
                logging.info(f"Data written to staging Delta table with schema overwrite: {cdh_location_stg}")
            except Exception as e:
                logging.error(f"Error writing data to {cdh_location_stg}: {str(e)}")
                raise

        else:
            if 'BusinessDate' in dwh_tbl_df.columns:
                column_names = dwh_tbl_df.schema.names
                new_columns = [col(c).alias("source_BusinessDate") if c == "BusinessDate" else col(c) for c in column_names]
                dwh_tbl_df = dwh_tbl_df.select(*new_columns)

            try:
                dwh_tbl_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(cdh_location_stg)
                logging.info(f"Data written to staging Delta table: {cdh_location_stg}")
            except Exception as e:
                logging.error(f"Error writing data to {cdh_location_stg}: {str(e)}")
                raise

    except Exception as e:
        logging.error(f"Error loading data from {database_name} server: {str(e)}")
        if _log_enabled:
            try:
                from pipeline_log_helpers import log_task_end
                log_task_end(spark, "staging", business_date, task_name,
                            status="failed", error_message=str(e)[:500], env_prefix=env_prefix)
            except Exception:
                pass
        raise
 
    time.sleep(20)

    dwh_tbl_df_from_stg = spark.read.format("delta").table(cdh_location_stg) 

    if _log_enabled:
        try:
            from pipeline_log_helpers import log_task_end
            log_task_end(spark, "staging", business_date, task_name,
                        status="completed", env_prefix=env_prefix)
        except Exception as log_err:
            print(f"[WARNING] Task log end failed (non-blocking): {log_err}")

    return dwh_tbl_df_from_stg



import re

def greek_to_ascii(column_name):
    greek_to_ascii_map = {
        'α': 'a', 'β': 'b', 'γ': 'g', 'δ': 'd', 'ε': 'e', 'ζ': 'z', 'η': 'h', 
        'θ': 'th', 'ι': 'i', 'κ': 'k', 'λ': 'l', 'μ': 'm', 'ν': 'n', 'ξ': 'x',
        'ο': 'o', 'π': 'p', 'ρ': 'r', 'σ': 's', 'τ': 't', 'υ': 'y', 'φ': 'f',
        'χ': 'ch', 'ψ': 'ps', 'ω': 'o'
    }
    return re.sub('|'.join(greek_to_ascii_map.keys()), lambda x: greek_to_ascii_map[x.group()], column_name)
