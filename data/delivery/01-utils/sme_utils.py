from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from pyspark.sql.functions import trim

def load_landed_delta_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_filters: str,
    table_selected_fields: list[str],
) -> DataFrame:
    
    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .filter(table_filters)
        .select(table_selected_fields)
    )

    # Trim all string columns
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df


def load_landed_portfolio_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_filters: str,
    table_selected_fields: list[str],
) -> DataFrame:
    
    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .filter(table_filters)
        .select(table_selected_fields)
    )

    # Trim all string columns
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df


def create_primary_delta_table(
    spark: SparkSession,
    primary_table_path: str,
    cluster_columns: Optional[list[str]],
    schema: str,
) -> None:

    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {primary_table_path} ( 
        {schema}
        )
        USING DELTA
    """
    
    if cluster_columns:
        sql_query = sql_query + f"CLUSTER BY ({', '.join(cluster_columns)})"

    sql_query = (
        sql_query
        + """
        TBLPROPERTIES (
            'delta.logRetentionDuration' = 'interval 5 weeks',
            'delta.deletedFileRetentionDuration' = 'interval 5 weeks',
            'delta.minReaderVersion' = '3',
            'delta.minWriterVersion' = '7',
            'delta.columnMapping.mode' = 'name',
            'delta.dataSkippingStatsColumns' = 'BusinessDate'
        )
    """
    )

    spark.sql(sql_query)
    spark.sql(
            f"""REPAIR TABLE {primary_table_path} SYNC METADATA"""   
        )
