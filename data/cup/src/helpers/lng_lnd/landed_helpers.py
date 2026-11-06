"""
The `transform.py` module is used for transforming data.

The module contains the following functions:

* `with_BusinessDate`:
    This function adds a `BusinessDate` column to the DataFrame.
* `with_LoadingDateTime`:
    This function adds a `LoadingDateTime` column to the DataFrame.
* `drop_Columns`:
    This function drops the specified columns from the DataFrame.
* `trim_string_columns`:
    This function trims leading/trailing whitespace from all string columns.
* `transform_df`:
    This function transforms the DataFrame by adding the `BusinessDate`
    and `LoadingDateTime` columns, and then dropping the specified columns.

"""

from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, to_date, from_utc_timestamp, date_sub, date_add, date_format, col, regexp_replace
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from typing import Any, List
from datetime import datetime
import logging

def with_BusinessDate(
    business_date: str,
) -> Callable[[DataFrame], DataFrame]:
    """Adds a `BusinessDate` column to the DataFrame.
    It will not check if there is a column named as partition column already in the dataframe

    Args:
        business_date: The business date.

    Returns:
        A function that adds the `BusinessDate` column to the DataFrame.

    """

    def _(df: DataFrame) -> DataFrame:
        # fmt: off

        df = (
            df
            .withColumn(
                "BusinessDate",
                to_date(lit(business_date), "yyyy-MM-dd")
            )
        )
        # fmt: on

        return df

    return _

def with_LoadingDateTime(df: DataFrame) -> DataFrame:
    """Adds a `LoadingDateTime` column to the DataFrame.

    Args:
        df: The DataFrame.

    Returns:
        The DataFrame with the `LoadingDateTime` column added.

    """
    # fmt: off
    df = (
        df
        .withColumn(
            "LoadingDateTime",
            from_utc_timestamp(current_timestamp(), "Europe/Athens")
        )
    )
    # fmt: on

    return df


def with_PartitionDate(df: DataFrame, partition_column, ingestion_date) -> DataFrame:
    """Adds a `Partition_0 ` column to the DataFrame.

    Args:
        df: The DataFrame.

    Returns:
        The DataFrame with the `partition column` column for landing added.

    """
    # fmt: off
    #current_date = datetime.now()
    #formatted_date = current_date.strftime("%Y%m%d")

    df = (
        df
        .withColumn(
            partition_column,
            to_date(lit(ingestion_date), "yyyy-MM-dd")
        )
    )
    # fmt: on
    
    return df

def with_PartitionNextDate(df: DataFrame, partition_column, ingestion_date) -> DataFrame:
    """Adds a `Partition_0 ` column to the DataFrame.
    It will not check if there is a column named as partition column already in the dataframe

    Args:
        df: The DataFrame.

    Returns:
        The DataFrame with the `partition column` column for landing added.

    """
    # fmt: off
    #current_date = datetime.now() 
    #next_date = current_date + timedelta(days=1)
    #
    #formatted_date = next_date.strftime("%Y%m%d")
    df = (
        df
        .withColumn(
            partition_column,
            to_date(lit(ingestion_date), "yyyy-MM-dd")
        )
    )
    # fmt: on

    return df


def drop_Columns(
    columns_todrop: list,
) -> Callable[[DataFrame], DataFrame]:
    """Drops the specified columns from the DataFrame.

    Args:
        columns_todrop: The columns to drop.

    Returns:
        A function that drops the specified columns from the DataFrame.

    """

    def _(df: DataFrame) -> DataFrame:
        # fmt: off
        df = (
            df
            .drop(*columns_todrop)
        )
        # fmt: on

        return df

    return _



def fetch_previous_business_date(
    spark: SparkSession,
    landed_table_path: str,
    business_date: str,
) -> DataFrame:
    """Fetches the previous business date's data from the landed delta table.

    Args:
        spark: A spark session.
        landed_table_path: The path to the landed delta table.
        business_date: The current business date.

    Returns:
        A `DataFrame` containing the data from the previous business date.

    """
    # Create temporary view from delta table
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY VIEW delta_table_view as \
            SELECT * FROM {landed_table_path}"
    )

    # Get the maximum date value less than the current business date
    df_previous_business_date = spark.sql(
        f"SELECT * \
            FROM delta_table_view \
            WHERE BusinessDate = (SELECT MAX(BusinessDate) \
            FROM delta_table_view WHERE BusinessDate < \
            to_date({business_date!r}, 'yyyy-MM-dd'))" 
    )
#change add !r for corrective date format by GP
    return df_previous_business_date


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trims leading and trailing whitespace from all string columns.

    Removes spaces, tabs, newlines, and carriage returns from the
    beginning and end of each string column value. Internal content
    is left unchanged.

    Args:
        df: The DataFrame to process.

    Returns:
        The DataFrame with trimmed string columns.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                regexp_replace(col(f"`{field.name}`"), r"^[\s]+|[\s]+$", "")
            )
    return df


def transform_df(
    df_landing: DataFrame, business_date: str
) -> DataFrame:
    """Chains the custom transforms

    Transforms the DataFrame by adding the `BusinessDate` and `LoadingDateTime`
    columns, and then dropping the specified columns.

    Args:
        df_landing: The DataFrame to transform.
        business_date: The business date.

    Returns:
        The transformed DataFrame.

    """
    # fmt: off
    columns_todrop = ["sk_id", "Cluster_0", "BusinessDate"]
    df_transformed = (
        df_landing
        .transform(trim_string_columns)
        .transform(drop_Columns(columns_todrop))
        .transform(with_BusinessDate(business_date))
        .transform(with_LoadingDateTime)
    )
    # fmt: on

    return df_transformed



"""
The `load.py` module is used for loading data to the landed zone.

The module contains the following functions:

* `load`:
    This function loads the data to the landed delta tables.
* `write_to_landed`:
    This function writes the transformed DataFrame to the landed delta table.

"""

from py4j.java_gateway import JavaObject  # type: ignore
from pyspark.sql import DataFrame, SparkSession


#### needed modification saveAsTable
def load(
    spark: SparkSession,
    df_transformed: DataFrame,
    table_path: str,
    business_date: str,
) -> None:
    """Loads the data to the landed delta tables.

    Deletes existing records for this business date and subsequently,
    appends the data to the landed delta table.

    Args:
        spark: A spark session.
        df_transformed: The DataFrame to load.
        table_path: The path to the landed delta table.
        business_date: The business date.

    Returns:
        None.

    """

    print("Before deleting if data exist  {business_date}")
    businessd = datetime.strptime(business_date, '%Y-%m-%d').date()

    print(f"Business date after to date: {businessd}")
    spark.sql(
        f"DELETE FROM {table_path} \
            WHERE BusinessDate = \
            to_date({business_date!r}, 'yyyy-MM-dd')"

    )

                
    #change above this DATE '{business_date}'" for corrective date format by GP
    
    print("Before writing to the table")
    df_transformed.write        \
            .format("delta")        \
            .clusterBy("BusinessDate")    \
            .mode("append")         \
            .saveAsTable(table_path)
        

###### Needed modification !!!!!!!!!!!!!!!
def write_to_area(
    spark: SparkSession,
    df_transformed: DataFrame,
    table_path: str,
    business_date: str,
) -> None:
    """Writes the transformed DataFrame to the landed delta table.

    Attempts to write the transformed DataFrame to the landed delta table
    emulating a SQL transaction. It gets the delta table version before
    the load() operation, then tries to execute load, which involves a
    delete and an append. In case of a failure, it rollbacks to the
    delta table version before any of the changes took place. If no delta
    table exists, it creates one using the transformed DataFrame.

    Args:
        spark: A spark session.
        df_transformed: The DataFrame to write.
        table_path: The path to the  delta table.
        business_date: The business date.
    Returns:
        None.

    """
    if spark.catalog.tableExists(table_path) is True:
        version_no = (
            spark.sql(f"DESCRIBE HISTORY {table_path}")
            .selectExpr("MAX(version)")
            .first()[0]  # type: ignore
        )

        try:
            load(
                spark,
                df_transformed,
                table_path,
                business_date,
            )
        except Exception as e:
            spark.sql(
                f"RESTORE TABLE {table_path} \
                    TO VERSION AS of {version_no}"
            )

            logging.error(e)
            raise e
    else:
        print(table_path)

        ####### It creates the table at this point only for landed for the first time
        ####### Needs to get modified to create a table with liquid cluster capabilities
        ####### and tableproperties with retention period of 5 weeks 
        #######  The same can be used for landing tables
        df_transformed.write        \
            .format("delta")        \
            .clusterBy("BusinessDate")    \
            .option('delta.deletedFileRetentionDuration' , 'interval 5 weeks')      \
            .option('delta.deletedFileRetentionDuration' , 'interval 5 weeks')      \
            .option('delta.minReaderVersion', '2')            \
            .option('delta.minWriterVersion', '5')            \
            .option('delta.columnMapping.mode', 'name')        \
            .option('delta.dataSkippingStatsColumns', 'BusinessDate')       \
            .mode("append")         \
            .saveAsTable(table_path)
        spark.sql(
            f"""REPAIR TABLE {table_path} SYNC METADATA"""   
        )

#### loading landed
def load_lng_tbl(
    spark: SparkSession,
    df_landing: DataFrame,
    table_path: str,
    cluster_column: str,
    ingestion_date: str
) -> None:
    """Loads the data to the landed delta tables.

    Deletes existing records for this business date and subsequently,
    appends the data to the landed delta table.

    Args:
        spark: A spark session.
        df_landing: The DataFrame to load.
        table_path: The path to the landed delta table.
        ingestion_date: The business date.

    Returns:
        None.

    """
    spark.sql(
        f"DELETE FROM {table_path} \
            WHERE Cluster_0 = \
                cast({ingestion_date!r} as date)"
    )

    df_landing.write        \
            .format("delta")        \
            .clusterBy(cluster_column)    \
            .mode("append")         \
            .saveAsTable(table_path)


###### Needed modification !!!!!!!!!!!!!!!
def write_to_landing(
    spark: SparkSession,
    df_landing: DataFrame,
    table_path: str,
    cluster_column: str,
    ingestion_date: str,
    business_date: str = None,
    schema_name: str = None,
    group_name: str = None,
    env_prefix: str = None,
) -> None:
    """Writes the transformed DataFrame to the landed delta table.

    Attempts to write the transformed DataFrame to the landed delta table
    emulating a SQL transaction. It gets the delta table version before
    the load() operation, then tries to execute load, which involves a
    delete and an append. In case of a failure, it rollbacks to the
    delta table version before any of the changes took place. If no delta
    table exists, it creates one using the transformed DataFrame.

    Args:
        spark: A spark session.
        df_landing: The DataFrame to write.
        table_path: The path to the  delta table.
        ingestion_date: The business date.
        business_date: The business date (for task logging).
        schema_name: Source schema name (for task logging).
        group_name: Group name (for task logging).
        env_prefix: Environment prefix (for task logging).
    Returns:
        None.

    """
    # Extract table_name from path for logging
    task_name = table_path.split(".")[-1] if "." in table_path else table_path
    _log_enabled = business_date is not None

    if _log_enabled:
        try:
            from pipeline_log_helpers import log_task_start, log_task_end
            log_task_start(spark, "landing", business_date, task_name,
                          schema_name=schema_name, group_name=group_name,
                          env_prefix=env_prefix)
        except Exception as log_err:
            logging.warning(f"Task log start failed (non-blocking): {log_err}")

    if spark.catalog.tableExists(table_path) is True:
        version_no = (
            spark.sql(f"DESCRIBE HISTORY {table_path}")
            .selectExpr("MAX(version)")
            .first()[0]  # type: ignore
        )

        try:
            load_lng_tbl(
                spark,
                df_landing,
                table_path,
                cluster_column,
                ingestion_date
            )
        except Exception as e:
            spark.sql(
                f"RESTORE TABLE {table_path} \
                    TO VERSION AS of {version_no}"
            )

            if _log_enabled:
                try:
                    from pipeline_log_helpers import log_task_end
                    log_task_end(spark, "landing", business_date, task_name,
                                status="failed", error_message=str(e)[:500], env_prefix=env_prefix)
                except Exception:
                    pass

            logging.error(e)
            raise e
    else:
        print(table_path)

        df_landing.write        \
            .format("delta")        \
            .clusterBy("Cluster_0")    \
            .option('delta.deletedFileRetentionDuration' , 'interval 5 weeks')      \
            .option('delta.deletedFileRetentionDuration' , 'interval 5 weeks')      \
            .option('delta.minReaderVersion', '2')            \
            .option('delta.minWriterVersion', '5')            \
            .option('delta.columnMapping.mode', 'name')         \
            .option('delta.dataSkippingStatsColumns', 'Cluster_0')       \
            .mode("append")         \
            .saveAsTable(table_path)
        spark.sql(
            f"""REPAIR TABLE {table_path} SYNC METADATA"""   
        )

    if _log_enabled:
        try:
            from pipeline_log_helpers import log_task_end
            log_task_end(spark, "landing", business_date, task_name,
                        status="completed", env_prefix=env_prefix)
        except Exception as log_err:
            logging.warning(f"Task log end failed (non-blocking): {log_err}")





def process_full_or_initial_incremental(
    spark: SparkSession,
    df_transformed: DataFrame,
    landed_table_path: str,
    business_date: str,
) -> None:
    """Performs the ETL for full tables and initialization of incremental tables.

    Args:
        spark: A spark session to perform the landing->landed ETL.
        table_date_path: The path to landing files for a specific ingestion date.
        landed_table_path: The path to the landed table where data will be written.
        business_date: The business date.
    Returns:
        None.

    """

    write_to_area(
        spark, df_transformed, landed_table_path, business_date
    )
    logging.info(f"Delta Table in {landed_table_path} successfully updated.")


def process_incremental(
    spark: SparkSession,
    df_transformed: DataFrame,
    landed_table_path: str,
    business_date: str,
    primary_keys: List[str],
) -> None:
    """Performs the ETL for incremental tables.

    Writes the compined result-set of previous business date's records
    - with updated `BusinessDate` and `LoadingDateTime` columns - and current
    business date's data to the landed delta table. Duplicates are excluded.

    Args:
        spark: A spark session to perform the landing->landed ETL.
        table_date_path: The path to landing files for a specific ingestion date.
        landed_table_path: The path to the landed table where data will be written.
        business_date: The business date.
        sum_file_size_bytes: The total size of the landing files in bytes
        logger: An instance of Glue logger.

    Returns:
        None.

    """
    df_landing_incremental_transformed = df_transformed
    df_previous_business_date = fetch_previous_business_date(
        spark, landed_table_path, business_date
    )
    #df_previous_business_date.show(5)

    #print("Before tranforming previous date data in process incremental")
    df_previous_business_date_transformed = df_previous_business_date

    #print("After transform")
    #df_previous_business_date_transformed.show(5)

    target_columns = [
        col(f"target_df.{column_name}")
        for column_name in df_previous_business_date_transformed.columns
    ]
    #print(f"Target df columns: {target_columns}")
    source_columns = [
        col(f"source_df.{column_name}")
        for column_name in df_landing_incremental_transformed.columns
    ]
    #print(f"Source df columns: {source_columns}")

    # Define the key columns
    key_columns = primary_keys
    
    df_landing_incremental_transformed_distinct_keys = df_landing_incremental_transformed.select(*key_columns).distinct()

    df_previous_business_date_transformed_distinct_keys = df_previous_business_date_transformed.select(*key_columns).distinct()

    # Join condition
    join_condition = [
        col(f"target_df.{k}") == col(f"source_df.{k}") for k in key_columns
    ]

    # Join the DataFrames
    joined_df_updates = df_previous_business_date_transformed_distinct_keys.alias("target_df").join(
        df_landing_incremental_transformed.alias("source_df"),
        join_condition,
        "left_outer",
    )
    # new 
    # Join the DataFrames
    joined_df_unchanged = df_previous_business_date_transformed.alias("target_df").join(
        df_landing_incremental_transformed_distinct_keys.alias("source_df"),
        join_condition,
        "left_outer",
    )


    # Determine records to insert (present in source_df but not in target_df)
    inserts_df = (
        df_landing_incremental_transformed.alias("source_df")
        .join(
            df_previous_business_date_transformed_distinct_keys.alias("target_df"), ## change
            join_condition,
            "left_anti",
        )
        .select(*source_columns)
    )

    # Determine records to update (present in both)
    updates_df = joined_df_updates.filter(
        col(f"source_df.{key_columns[0]}").isNotNull()
    ).select(*source_columns)

    # Determine unchanged records
    unchanged_df = joined_df_unchanged.filter(col(f"source_df.{key_columns[0]}").isNull()).select(
        *target_columns
    )

    # Prepare the final DataFrame
    df_union = unchanged_df.unionByName(
        inserts_df, allowMissingColumns=True
    ).unionByName(updates_df, allowMissingColumns=True)

    #print("This is unionbyname df")

    #print("Before tranforming previous date data before union in incremental")
    df_union = transform_df(
        df_union, business_date, 
    )
    logging.info("Columns BusinessDate, LoadingDateTime added.")

    write_to_area(
        spark, df_union, landed_table_path, business_date
    )
    logging.info(f"Delta Table in {landed_table_path} successfully updated.")



def load_table_to_landed(
    spark: SparkSession,
    tbl_lng : DataFrame,
    table_name: str,
    cdh_location_lnd: str,
    ingestion_case: str,
    primary_keys: str,
    business_date: str,
    schema_name: str = None,
    group_name: str = None,
    ingestion_date: str = None,
    env_prefix: str = None,
    ):
        
    logging.info(f"Processing {table_name} to landed...")

    # Task logging
    _log_enabled = False
    try:
        from pipeline_log_helpers import log_task_start, log_task_end
        log_task_start(spark, "landed", business_date, table_name,
                      schema_name=schema_name, group_name=group_name,
                      env_prefix=env_prefix)
        _log_enabled = True
    except Exception as log_err:
        print(f"[WARNING] Task log start failed (non-blocking): {log_err}")

    try:
        # Determine the ingestion case (full or incremental)
        logging.info(f"Ingestion case for {table_name}: {ingestion_case}")
        logging.info(f"Business date: {business_date}")
        logging.info(f"Landed table location: {cdh_location_lnd}")

        # Transform the DataFrame
        df_transformed = transform_df(tbl_lng, business_date)

        if ingestion_case == "full" or (
            ingestion_case == "incremental" and not spark.catalog.tableExists(cdh_location_lnd)
        ):
            logging.info("Processing full table or initial incremental load")
            process_full_or_initial_incremental(
                spark,
                df_transformed,
                cdh_location_lnd,
                business_date
            )

        elif ingestion_case == "incremental" and spark.catalog.tableExists(cdh_location_lnd):
            logging.info(f"Primary keys for incremental load: {primary_keys}")

            if not primary_keys:
                logging.error("No primary keys have been provided")
                raise ValueError("Primary keys are required for incremental processing")

            process_incremental(
                spark,
                df_transformed,
                cdh_location_lnd,
                business_date,
                primary_keys
            )

        else:
            logging.error("Datamode does not exist or is unsupported")
            raise ValueError(f"Unsupported ingestion case: {ingestion_case}")

        if _log_enabled:
            try:
                log_task_end(spark, "landed", business_date, table_name,
                            status="completed", env_prefix=env_prefix)
            except Exception as log_err:
                print(f"[WARNING] Task log end failed (non-blocking): {log_err}")

    except Exception as e:
        logging.error(f"Error processing {table_name} to landed: {str(e)}")
        if _log_enabled:
            try:
                log_task_end(spark, "landed", business_date, table_name,
                            status="failed", error_message=str(e)[:500], env_prefix=env_prefix)
            except Exception:
                pass
        raise