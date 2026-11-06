# Databricks notebook source
import logging
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from databricks.sdk.runtime import *

import redis

# COMMAND ----------

from pyspark.conf import SparkConf

def set_spark_conf() -> SparkConf:
    """Sets the Spark configurations for the ETL pipeline.

    Sets the following Spark configuration options:

    Returns:
        A `SparkConf` object with the specified configurations.

    """
    conf_list = [
        ("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"),
        ("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED"),
        ("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED"),
        ("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"),
        ("spark.sql.sources.partitionOverwriteMode", "DYNAMIC"),
        ("spark.databricks.delta.schema.autoMerge.enabled", "true"),
    ]
    spark_conf = SparkConf().setAll(conf_list)
    return spark_conf

# COMMAND ----------


#spark_conf = set_spark_conf()
#spark = SparkSession.builder.appName("dwh_stg_loader").config(conf=spark_conf).getOrCreate()
spark = SparkSession.builder.appName("dwh_stg_loader").getOrCreate()

# COMMAND ----------

import sys, os
relative_path = "../helpers/lng_lnd"
path = os.path.abspath(relative_path)
if path not in sys.path:
    sys.path.append(path)
from landing_helpers import *
from landed_helpers import *

relative_path_auth = "../helpers/authentication_helpers"
path = os.path.abspath(relative_path_auth)
if path not in sys.path:
    sys.path.append(path)
from authentication_utils import *


# COMMAND ----------

#secrets_scope = "aws-databricks"
#aws_keys = dbutils.secrets.list(scope=secrets_scope)
#key_names = [key.key for key in aws_keys] if aws_keys else []
        

# COMMAND ----------

#aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_names[0])

# COMMAND ----------

ssm_client = boto3.client('ssm', region_name='eu-central-1')

# COMMAND ----------

catalog_name_staging = ssm_client.get_parameter(
    Name="/cfn/glue/catalog/staging/name"
)["Parameter"]["Value"]   # qa-ec1-cepal-staging-area
catalog_name_landing = ssm_client.get_parameter(
    Name="/cfn/glue/catalog/landing/name"
)["Parameter"]["Value"]     # qa-ec1-cepal-landing-area
catalog_name_landed = ssm_client.get_parameter(
    Name="/cfn/glue/catalog/landed/name"
)["Parameter"]["Value"]     # qa-ec1-cepal-landed-area

env_prefix = ssm_client.get_parameter(
    Name='/cfn/bucket_prefix'
)["Parameter"]["Value"]     # qa

redis_server = ssm_client.get_parameter(
    Name='/cfn/redis/primary/address'
)["Parameter"]["Value"]     # qa-ec1-redis-cepal-cluster.745ljs.ng.0001.euc1.cache.amazonaws.com

redis_port = ssm_client.get_parameter(
    Name='/cfn/redis/primary/port'
)["Parameter"]["Value"]     # 6379

redis_host = f"{redis_server}:{redis_port}"

# COMMAND ----------

print(redis_host)

# COMMAND ----------

def widget_exists(widget_name):
    try:
        # Try to get the widget value
        dbutils.widgets.get(widget_name)
        return True
    except Exception as e:
        if "com.databricks.dbutils_v1.InputWidgetNotDefined" in str(e):
            return False
        else:
            raise e

# COMMAND ----------

dbutils.widgets.text('database_name', 'dwh_stg', '01-Database name')
dbutils.widgets.text('schema_name', 'None', '02-Schema name')
dbutils.widgets.text('group_name', 'None', '03-Group name')
dbutils.widgets.text('tables_to_process', 'None', '04-Tables to process')
dbutils.widgets.text('business_date', 'None', '05-Business Date')
dbutils.widgets.text('external_files', 'None', '07-External Files')
dbutils.widgets.text('sender_name', 'None', '08-Sender Name')
dbutils.widgets.text('file_name', 'None', '10- File Name')
dbutils.widgets.text('folder_name', 'None', '11-Folder Name')
dbutils.widgets.text('table_schema', 'None', '12-Table Schema')



 


business_date = dbutils.widgets.get('business_date')
tables_to_process_group = dbutils.widgets.get('tables_to_process').split(',')
schema_name = dbutils.widgets.get('schema_name')
group_name = dbutils.widgets.get('group_name')
database_name = dbutils.widgets.get('database_name')
etl_zone = dbutils.widgets.get('etl_zone')
external_files = dbutils.widgets.get('external_files')
sender_name = dbutils.widgets.get('sender_name')
file_name = dbutils.widgets.get('file_name')
folder_name = dbutils.widgets.get('folder_name')
table_schema = dbutils.widgets.get('table_schema')


# COMMAND ----------

print(f"database_name: {database_name}")
print(f"schema_name: {schema_name}")
print(f"group_name: {group_name}")
print(f"tables_to_process_group: {tables_to_process_group}")
print(f"business_date: {business_date}")

# COMMAND ----------

if env_prefix == "d":
    env_redis = "dev"
elif env_prefix == "qa":
    env_redis = "qa"
elif env_prefix == "p":
    env_redis = "prd"
else:
    logging.error("No environment set")
print(env_redis)

# COMMAND ----------

import pyspark.sql.functions as F
import logging

# COMMAND ----------

secret_scope_dwh = "onprem-dwhcore"

def get_keyvalue_from_scope(secrets_scope,key_name):
    try:
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None

host = get_keyvalue_from_scope(secret_scope_dwh,"ip")
jdbcUserName = get_keyvalue_from_scope(secret_scope_dwh,"user")
jdbcPassword = get_keyvalue_from_scope(secret_scope_dwh,"password")

# COMMAND ----------

#host = "10.240.2.230"
port = "1433"

catalog = f"`{env_prefix}-cdh-uc`"

# environment ????????????????????????????????????????????????????????????
schema_catalog_stg = f"`{catalog_name_staging}`"    
schema_catalog_lng = f"`{catalog_name_landing}`"
schema_catalog_lnd = f"`{catalog_name_landed}`"

# COMMAND ----------

from datetime import datetime

current_date = datetime.now().date()

# COMMAND ----------

newsphone_args= {
    "table_schema": table_schema,
    "file_name": file_name,
    "schema_catalog": schema_catalog_lng,
    "source_bucket": f"{env_prefix}-ec1-cepal-landing-area",
    "folder_name": folder_name,
}

# COMMAND ----------

for table_name in tables_to_process_group:
    logging.info(f"Processing {table_name} to staging...")
    table_parts = table_name.split("_")[1:] 
    logging.info(table_parts)
    length = len(table_parts)
    if length == 1:
        table_name_split = f"{table_parts[0]}"
    elif length > 1:
        logging.info(length)
        for i in range(length):
            if i == 0:
                table_name_split_p = f"{table_parts[i]}"
            else:
                table_name_split_p = f"{table_name_split_p}_{table_parts[i]}"
            logging.info(f"{i}, {table_name_split_p}")
        table_name_split = table_name_split_p
    table_str = f"{schema_name}.{table_name_split}"


# START LOADING TO STAGING FROM DWH_STG FOR ALL TABLES TO PROCESS
# IN CATALOG AND SCHEMA DEFINED

    cdh_location_stg = f"{catalog}.{schema_catalog_stg}.dwh_stg_{table_name}"
    #logging.info(table_str)
    #logging.info(cdh_location)
    #logging.info(table_name_split)

    spark.sql(
        f"""DROP TABLE IF EXISTS {cdh_location_stg}"""   
    )
    
    try:
        dwh_tbl_df =(
            spark.read.format("sqlserver")
                .option("host", host)
                .option("port", port)
                .option("encrypt", "false")
                .option("trustServerCertificate", "true")
                .option("dbtable", table_str)
                .option("database", database_name)
                .option("user", jdbcUserName)
                .option("password", jdbcPassword)
                # The following options configure parallelism for the query. This is required to get better performance, otherwise only a single thread will read all the data
                # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
                # .option("partitionColumn", "partition_key")
                # lowest value to pull data for with the partitionColumn
                # .option("lowerBound", "minValue")
                # max value to pull data for with the partitionColumn
                # .option("upperBound", "maxValue")
                # number of partitions to distribute the data into. Do not set this very large (~hundreds) to not overwhelm your database
                .load()
            )
 #       dwh_tbl_df.createOrReplaceTempView(f"view_{table_name}")

    except Exception as e:
        logging.error(f"Error loading data from {database_name} server: {str(e)}")
        raise

    logging.info(dwh_tbl_df.count())

# WRITE TO STAGING UNITY CATALOG THE PROCESSED TABLE
# IN CATALOG AND SCHEMA DEFINED
    schema_df = dwh_tbl_df.schema.simpleString()
    if ' ' in schema_df:
        if spark.catalog.tableExists(cdh_location_stg) is False:
            create_staging_delta_table(
                spark,
                cdh_location_stg,
            )
            try:
                dwh_tbl_df          \
                    .write                  \
                    .format("delta")        \
                    .mode("overwrite")      \
                    .option("overwriteSchema", "true")  \
                    .saveAsTable(cdh_location_stg)
            except Exception as e:
                logging.error(f"Error writing data to {cdh_location_stg} server: {str(e)}")
                raise
    else:
        if 'BusinessDate' in dwh_tbl_df.columns:
            column_names = dwh_tbl_df.schema.names
            new_columns = [col(c).alias("source_BusinessDate") if c == "BusinessDate" else col(c) for c in column_names]
            dwh_tbl_df = dwh_tbl_df.select(*new_columns)

        try:
            dwh_tbl_df          \
                .write                  \
                .format("delta")        \
                .mode("overwrite")      \
                .saveAsTable(cdh_location_stg)
        except Exception as e:
            logging.error(f"Error writing data to {cdh_location_stg} server: {str(e)}")
            raise


# WRITE TO LANDING UNITY CATALOG THE PROCESSED TABLE
# AFTER CLUSTERING WITH LOADING DATE IN COLUMN "Partition_0"

    logging.info(f"Processing {table_name} to landing...")
    cdh_location_lng = f"{catalog}.{schema_catalog_lng}.{table_name}"
    try:
        partition_column = "Cluster_0"
        tbl_lng = with_PartitionDate(dwh_tbl_df, partition_column)
        #tbl_lng.display()
        write_to_landing(spark, tbl_lng, cdh_location_lng, partition_column, current_date)
    except Exception as e:
        logging.error(f"Error writing data to {cdh_location_lng} server: {str(e)}")
        raise

# WRITE TO LANDED UNITY CATALOG THE PROCESSED TABLE
# AFTER PARTIOTION WITH BUSINESS DATE AND ADDING 2 COLUMNS: BUSINESS DATE AND INGESTION TIMESTAMP"

    logging.info(f"Processing {table_name} to landed...")

    cdh_location_lnd = f"{catalog}.{schema_catalog_lnd}.{table_name}"

    #data_mode_key = f"{env}-Landed:DataMode:{table_name}"
    #incr_prim_key = f"{env}-Landed:Keys:{table_name}"
    print(env_redis)
    ingestion_case = get_table_datamode(table_name, env_redis, redis_host)
    print(ingestion_case)
    print(business_date)
    print(cdh_location_lnd)


    #max_cluster_date_df = spark.sql(
    #    f"SELECT MAX(Cluster_0) as max_partition FROM {cdh_location_lng}"
    #    )

    # Collect the result as a Python variable
    #max_cluster_date = max_cluster_date_df.collect()[0]['max_partition']


    #if (spark.catalog.tableExists(cdh_location_lng) is True) and (max_cluster_date == business_date) :
    #    tbl_lng = spark.read.table(cdh_location_lng)
    #else:
    #    print("table does not exist or was not updated yet")

    df_transformed = transform_df(tbl_lng, business_date)
    df_transformed.show(5)
    
    if ingestion_case == "full" or (
            ingestion_case == "incremental"
            and not spark.catalog.tableExists(cdh_location_lnd)
        ):
            print("processing full table")
            process_full_or_initial_incremental(
                spark,
                df_transformed,
                cdh_location_lnd,
                business_date
            )
    elif ingestion_case == "incremental" and spark.catalog.tableExists(
        cdh_location_lnd
    ):
        print("processing incremental")
        primary_keys = get_incremental_table_keys(table_name, env_redis, redis_host)
        print(primary_keys)
        if not primary_keys:
            logging.error("No primary keys have been provided")
            

        process_incremental(
            spark,
            df_transformed,
            cdh_location_lnd,
            business_date,
            primary_keys,
            )
    else:
        logging.error(" Datamode does not exist")


