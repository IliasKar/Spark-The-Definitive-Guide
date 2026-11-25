import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import trim

# Configure logging
logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.appName("example").getOrCreate()

def load_dataframe_from_sql_server(
    server: str,
    table: str
) -> DataFrame:
    # Define the server IP based on the provided server type
    if server == 'dwhcore':
        server_ip = 'jdbc:sqlserver://10.240.2.230:1433'
        user_name = 'DEUser'
        password = 'MM998fqLvS9H2'
    elif server == 'dwhdatamarts':
        server_ip = 'jdbc:sqlserver://10.240.2.231:1433'
        user_name = 'PBIAgent'
        password = 'PB1Ag3nt!'
    else:
        raise ValueError("Only available servers are: dwhcore, dwhdatamarts") 
    # Read data from SQL Server using Spark JDBC
    try:
        df = (
            spark.read.format("jdbc") 
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("url", f"{server_ip};encrypt=false")
            .option("user", user_name)
            .option("password", password)
            .option("dbtable", table)
            .load()
        )
        logging.info(f"Successfully loaded data from {server} server.")
        return df
    except Exception as e:
        logging.error(f"Error loading data from {server} server: {str(e)}")
        raise

