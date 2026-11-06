from typing import Optional, List
import delta  # type: ignore
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def delete_from_curated_table(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    source_system: Optional[str] = None,
) -> None:
    
    #print(f"Deleting records from {curated_table_path} for {run_date} and {source_system}")
    
    if source_system is None:
        #print("No source system specified, deleting all records")
        delete_query = f"DELETE FROM {curated_table_path} WHERE BusinessDate = '{run_date}'"
        spark.sql(delete_query)
    else:
        #print(f"Deleting records for {source_system} in else zweig")
        delete_query = f"DELETE FROM {curated_table_path} WHERE BusinessDate = '{run_date}' AND SourceSystem = '{source_system}'"
        spark.sql(delete_query)



 
def delete_from_curated_table_nj(
    spark: SparkSession,
    curated_table_path: str,
    run_date: str,
    source_system: Optional[str] = None,
    portfolioName: Optional[List[str]] = None,
) -> None:
   
    if source_system is None:
        if not portfolioName or len(portfolioName) == 0:
            delete_query = f"""
                DELETE FROM {curated_table_path}
                WHERE BusinessDate = '{run_date}'
            """
            spark.sql(delete_query)
        else:
            portfolio_list = ",".join([f"'{p}'" for p in portfolioName])
            delete_query = f"""
                DELETE FROM {curated_table_path}
                WHERE BusinessDate = '{run_date}'
                  AND PortfolioName IN ({portfolio_list})
            """
            spark.sql(delete_query)
    else:
        if not portfolioName or len(portfolioName) == 0:
            delete_query = f"""
                DELETE FROM {curated_table_path}
                WHERE BusinessDate = '{run_date}'
                  AND SourceSystem = '{source_system}'
            """
            spark.sql(delete_query)
        else:
            portfolio_list = ",".join([f"'{p}'" for p in portfolioName])
            delete_query = f"""
                DELETE FROM {curated_table_path}
                WHERE BusinessDate = '{run_date}'
                  AND SourceSystem = '{source_system}'
                  AND PortfolioName IN ({portfolio_list})
            """
            spark.sql(delete_query)

def strip_newlines(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Removes newline and carriage return characters from the specified columns.
    """
    for col_name in columns:
        df = df.withColumn(
            col_name,
            F.translate(F.trim(F.col(col_name)), "\n\r", "")
        )
    return df