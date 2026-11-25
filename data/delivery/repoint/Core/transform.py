from typing import Optional

import delta  # type: ignore
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def delete_from_curated_table(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    source_system: Optional[str] = None,
) -> None:
    delta_table = delta.DeltaTable.forPath(spark, curated_table_path)

    if source_system is None:
        delta_table.delete(F.col("BusinessDate") == run_date)
    else:
        delta_table.delete(
            (F.col("BusinessDate") == run_date)
            & (F.col("SourceSystem") == source_system)
        )

def delete_from_curated_derivative_table(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
) -> None:
    delta_table = delta.DeltaTable.forPath(spark, curated_table_path)

    if source_system is None:
        delta_table.delete(F.col("BusinessDate") == run_date)
    else:
        delta_table.delete(
            (F.col("BusinessDate") == run_date)
        )


def delete_from_unity_table(
    spark: SparkSession,
    table_path: str,
    run_date: str,
) -> None:
    
    spark.sql (f"""
        DELETE FROM {table_path}
        WHERE BusinessDate = {run_date!r}
        """
    )