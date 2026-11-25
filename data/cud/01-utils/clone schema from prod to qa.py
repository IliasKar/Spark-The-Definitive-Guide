# Databricks notebook source
import requests

# Define source and target catalogs
source_catalog = "`p-cdh`"
target_catalog = "`qa-cdh`"

# List of schemas to process
schemas = ["primaryactive", "derivativeactive"]

# Databricks instance and authentication token
databricks_instance = "dbc-7a36af91-f82b.cloud.databricks.com"
token = "dapi8ec412165d5d95f2697bb02be096a24a"

# Job ID to run
job_id = "881341524492198"

# Loop through each schema
for schema in schemas:
    # Get list of tables in the current schema
    tables = spark.sql(f"SHOW TABLES IN {source_catalog}.{schema}").select("tableName").collect()
    
    # Loop through each table
    for table in tables:
        print(f"Processing {table}")
        
        # Extract table name
        table_name = table["tableName"]
        
        # Define source and target table paths
        source = f"{source_catalog}.{schema}.{table_name}"
        target = f"{target_catalog}.{schema}.{table_name}"

        # Define job arguments
        arguments = {
            "job_id": job_id,
            "job_parameters": {
                "source": source,
                "target": target
            }
        }

        # Make API request to run the job
        response = requests.post(
                f"https://dbc-7a36af91-f82b.cloud.databricks.com/api/2.0/jobs/run-now",
                json=arguments,
                headers={"Authorization": f"Bearer {token}"}
            )
        
        # Check response status
        if response.status_code == 200:
            print(f"Job with ID {job_id} called successfully for source {source} and target {target}")
        else:
            print(f"Failed to call job with ID {job_id} for source {source} and target {target}")
            print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC do not clone the table on target but create a view with it

# COMMAND ----------

import requests

# Define source and target catalogs
source_catalog = "`p-cdh`"
target_catalog = "`qa-cdh`"

# List of schemas to process
schemas = ["primary","derivative"]

# Loop through each schema
for schema in schemas:
    # Get list of tables in the current schema
    tables = spark.sql(f"SHOW TABLES IN {source_catalog}.{schema}").select("tableName").collect()
    
    # Loop through each table
    for table in tables:

                # Check if table exists using SQL query.
        target_table_exists = spark.sql(f"SELECT EXISTS (SELECT * FROM {target_catalog}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table['tableName']}')").first()[0]

        if not target_table_exists:

          print(f"Processing {table}")

          # Extract table name
          table_name = table["tableName"]
          
          # Define source and target table paths
          source = f"{source_catalog}.{schema}.{table_name}"
          target = f"{target_catalog}.{schema}.{table_name}"

          spark.sql(f"CREATE OR REPLACE VIEW {target_catalog}.{schema}.{table_name} AS select * from {source_catalog}.{schema}.{table_name}")

