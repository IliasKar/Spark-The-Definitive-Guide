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
        ("spark.databricks.delta.schema.autoMerge.enabled", "true"),
    ]
    spark_conf = SparkConf().setAll(conf_list)
    return spark_conf

# COMMAND ----------

spark_conf = set_spark_conf()
spark = SparkSession.builder.appName("dwh_stg_loader").config(conf=spark_conf).getOrCreate()

#spark = SparkSession.builder.appName("dwh_stg_loader").getOrCreate()

# COMMAND ----------

logging.info(spark_conf.getAll())

# COMMAND ----------

import sys, os
relative_path = "../helpers/lng_lnd"
path = os.path.abspath(relative_path)
if path not in sys.path:
    sys.path.append(path)
from landing_helpers import *
from landed_helpers import *
from newsphone import *
from hoist_segmentation import *

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

catalog_name_staging = "staging"
catalog_name_landing = "landing"
catalog_name_landed = "landed"

env_prefix = ssm_client.get_parameter(
    Name='/cfn/bucket_prefix'
)["Parameter"]["Value"]     # p

redis_server = ssm_client.get_parameter(
    Name='/cfn/redis/primary/address'
)["Parameter"]["Value"]     

redis_port = ssm_client.get_parameter(
    Name='/cfn/redis/primary/port'
)["Parameter"]["Value"]     # 6379

redis_host = f"{redis_server}:{redis_port}"

# COMMAND ----------

logging.info(redis_host)

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
dbutils.widgets.text('ingestion_date', 'None', '06-Ingestion Date')
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
ingestion_date = dbutils.widgets.get('ingestion_date')
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
#port_dwh = get_keyvalue_from_scope(secret_scope_dwh,"port")
#print(port_dwh)
jdbcUserName = get_keyvalue_from_scope(secret_scope_dwh,"user")
jdbcPassword = get_keyvalue_from_scope(secret_scope_dwh,"password")

# COMMAND ----------

#host = "10.240.2.230"
port = "1433"

catalog = f"`{env_prefix}-cdh`"

# environment ????????????????????????????????????????????????????????????
schema_catalog_stg = f"`{catalog_name_staging}`"    
schema_catalog_lng = f"`{catalog_name_landing}`"
schema_catalog_lnd = f"`{catalog_name_landed}`"
print(schema_catalog_stg)
print(schema_catalog_lng)
print(schema_catalog_lnd)
print(catalog)

# COMMAND ----------

from datetime import datetime

current_date = datetime.now().date()

# COMMAND ----------

cdh_location_lng = f"{catalog}.{schema_catalog_lng}.{folder_name}"

extfile_args= {
    "table_schema": table_schema,
    "file_name": file_name,
    "schema_catalog": schema_catalog_lng,
    "source_bucket": f"{env_prefix}-ec1-cepal-landing-zone",
    "folder_name": folder_name,
    "cdh_location_lng": cdh_location_lng
}
print(cdh_location_lng)

# COMMAND ----------

if external_files == "True":
    #Landing - pick the loader based on the sender
    if sender_name == "newsphone":
        landing_df = load_newsphone_external_file(spark, extfile_args)
    elif sender_name == "Hoist Segmentation":
        landing_df = load_hoist_segmentation_external_file(spark, extfile_args)
    elif sender_name == "rcg":
        landing_df = load_rcg_external_file(spark, extfile_args)
    else:
        raise ValueError(f"Unsupported external file sender_name: {sender_name!r}")

    #Landed - shared for all external-file senders
    table_name = folder_name
    cdh_location_lnd = f"{catalog}.{schema_catalog_lnd}.{table_name}"
    ingestion_case = get_table_datamode(table_name, env_redis, redis_server, redis_port)
    print(f"Ingestion case: {ingestion_case}")
    primary_keys = get_incremental_table_keys(table_name, env_redis, redis_server, redis_port)
    print(primary_keys)

    load_table_to_landed(
        spark,
        landing_df,
        table_name,
        cdh_location_lnd,
        ingestion_case,
        primary_keys,
        business_date,
        schema_name=schema_name,
        group_name=group_name,
        env_prefix=env_prefix,
    )

else:     
    #split tables_to_process_group based on ',' to take the tables schemas 
    for tables_total_names in tables_to_process_group:

        #split the seperated tables tables_total_names based on '.' to take database's and table's name
        table_splitted = tables_total_names.split('.')
        if len(table_splitted) == 1:
            raise ValueError("You should include database name in configuration.")
        table_database = table_splitted[0] #example: dwh_stg
        table_name = table_splitted[1] #example: arotron_eaisig_be

        logging.info(f"Processing {table_name} to staging...")

        total_table_parts = table_name.split("_")  #list of the parts example: [arotron,eaisig,be]
        table_parts = total_table_parts[1:] #get table's name parts
        logging.info(table_parts)
        table_schema = total_table_parts[0] #get schema - example: arotron

        length = len(table_parts)
        
        if length == 1:
                table_name_split = f"{table_parts[0]}"
        elif length > 1:
            for i in range(length):
                if i == 0:
                    table_name_split_p = f"{table_parts[i]}"
                else:
                    table_name_split_p = f"{table_name_split_p}_{table_parts[i]}"
                    logging.info(f"{i}, {table_name_split_p}")
                    table_name_split = table_name_split_p
        
        table_str = f"{table_schema}.{table_name_split}"
        database_name = table_database

        print(f"database_name: {database_name}")
        print(f"table_str: {table_str}")

    # START LOADING TO STAGING FROM DWH_STG FOR ALL TABLES TO PROCESS
    # IN CATALOG AND SCHEMA DEFINED

        cdh_location_stg = f"{catalog}.{schema_catalog_stg}.{database_name}_{table_name}"
        dwh_tbl_df = load_data_to_staging(
            spark,
            cdh_location_stg,
            host,
            port,
            database_name,
            jdbcUserName,
            jdbcPassword,
            table_str,
            business_date=business_date,
            schema_name=schema_name,
            group_name=group_name,
            ingestion_date=ingestion_date,
            env_prefix=env_prefix) 

    # WRITE TO LANDING UNITY CATALOG THE PROCESSED TABLE
    # AFTER CLUSTERING WITH LOADING DATE IN COLUMN "Partition_0"

        logging.info(f"Processing {table_name} to landing...")
        cdh_location_lng = f"{catalog}.{schema_catalog_lng}.{table_name}"
        try:
            partition_column = "Cluster_0"
            tbl_lng = with_PartitionDate(dwh_tbl_df, partition_column, ingestion_date)
            write_to_landing(spark, tbl_lng, cdh_location_lng, partition_column, ingestion_date,
                             business_date=business_date, schema_name=schema_name, group_name=group_name, env_prefix=env_prefix)
        except Exception as e:
            logging.error(f"Error writing data to {cdh_location_lng} server: {str(e)}")
            raise

    # WRITE TO LANDED UNITY CATALOG THE PROCESSED TABLE
    # AFTER PARTIOTION WITH BUSINESS DATE AND ADDING 2 COLUMNS: BUSINESS DATE AND INGESTION TIMESTAMP"

        logging.info(f"Processing {table_name} to landed...")

        primary_keys = []
        cdh_location_lnd = f"{catalog}.{schema_catalog_lnd}.{table_name}"
        ingestion_case = get_table_datamode(table_name, env_redis, redis_server, redis_port)
        if ingestion_case == "incremental":
            primary_keys = get_incremental_table_keys(table_name, env_redis, redis_server, redis_port)

        load_table_to_landed(
            spark,
            tbl_lng,
            table_name,
            cdh_location_lnd,
            ingestion_case,
            primary_keys,
            business_date,
            schema_name=schema_name,
            group_name=group_name,
            ingestion_date=ingestion_date,
            env_prefix=env_prefix,
        )

# COMMAND ----------


