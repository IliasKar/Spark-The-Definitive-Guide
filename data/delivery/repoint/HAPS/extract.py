from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import trim


def load_landed_delta_table(
    spark: SparkSession,
    catalog_name: str,
    table_name: str,
    table_filters: str,
    table_selected_fields: list[str],
) -> DataFrame:
    df = (
        spark.read.table(f"`{catalog_name}`.{table_name}")
        .filter(table_filters)
        .select(table_selected_fields)
    )

    # Trim all string columns
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df


