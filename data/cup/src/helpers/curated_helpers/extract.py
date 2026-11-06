from pyspark.sql import DataFrame, SparkSession
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


def load_landed_specificationparams_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_selected_fields: list[str],
) -> DataFrame:
    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .select(table_selected_fields)
    )

    # Trim all string columns
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df


def load_previous_businessdate_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_filters: list[str],
    run_date: str,
    table_selected_fields: list[str],
) -> DataFrame:
    
    max_previous_date = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .where(f"BusinessDate < '{run_date}'")
        .selectExpr("MAX(BusinessDate) as max_date")
        .collect()[0]["max_date"]
        )
    
    previous_date_filter = f"BusinessDate = '{max_previous_date}'"
    
    if table_filters is not None:
        table_filters.append(previous_date_filter)
        previous_final_filters = " AND ".join(table_filters)
    else:
        previous_final_filters = previous_date_filter

    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .filter(previous_final_filters)
        .select(table_selected_fields)
    )

    # Trim all string columns
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df