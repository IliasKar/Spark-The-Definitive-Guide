from typing import Optional

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

def create_delta_table(
    spark: SparkSession,
    curated_table_path: str,
    cluster_columns: Optional[list[str]],
    schema: str,
) -> None:
    

    ## Set column BusinessDate as the first column
    #schema_list = schema.split(",")
    #schema_length = len(schema_list)
    #bd_index = schema_list.index('BusinessDate Date')
    #schema_business_column = schema_list[bd_index]
    #print(f"bd index: {bd_index}")
    #if bd_index == 0 or bd_index > 32:
    #    schema_rest = [col for col in schema_list[:bd_index-1]]
    #    new_schema = schema_business_column +"," + ", ".join(schema_rest)

    #schema_length = len(schema_list)
    #print(f"Length of columns: {schema_length}")
    #schema_business_column = schema_list[schema_length-2]
    #print(f"this is the last column: {schema_business_column}")
    #schema_rest = [col for col in schema_list[:-1]]
    #new_schema = schema_business_column +"," + ", ".join(schema_rest)
    print(schema)
    print(curated_table_path)

    sql_query = f"""
        CREATE TABLE IF NOT EXISTS {curated_table_path} ( 
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
            f"""REPAIR TABLE {curated_table_path} SYNC METADATA"""   
        )