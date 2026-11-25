from typing import Optional

from pyspark.sql import SparkSession


def create_delta_table(
    spark: SparkSession,
    curated_table_path: str,
    partition_columns: Optional[list[str]],
    schema: str,
) -> None:
    schema = schema

    sql_query = f"""
        CREATE TABLE IF NOT EXISTS delta.`{curated_table_path}` ({schema})
        USING DELTA
    """

    if partition_columns:
        sql_query = sql_query + f"PARTITIONED BY ({', '.join(partition_columns)})"

    spark.sql(sql_query)


def create_unity_delta_table(
    spark: SparkSession,
    curated_table_path: str,
    partition_columns: Optional[list[str]],
    schema: str,
) -> None:
    schema = schema

    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {curated_table_path} ({schema})
        USING DELTA
    """

    if partition_columns:
        sql_query = sql_query + f"PARTITIONED BY ({', '.join(partition_columns)})"

    spark.sql(sql_query)
    spark.sql(f"REPAIR TABLE {curated_table_path} SYNC METADATA")

