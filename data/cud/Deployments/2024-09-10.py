# Databricks notebook source
# MAGIC %md
# MAGIC ## D360-1804
# MAGIC ## Update column PortfolioName in all cud tables
# MAGIC *PortfolioName : values from 'GXR One Holding' to 'GLX One Iris DAC'*
# MAGIC
# MAGIC ## *** !!! Please note that changes of the current pull request are due to changes in glue.app repository.***
# MAGIC *(relevant pull request in glue repo: [1642] (https://eu-central-1.console.aws.amazon.com/codesuite/codecommit/repositories/gr.cepal.dna.de.glue.app/pull-requests/1642/details?region=eu-central-1)
# MAGIC Please proceed to the steps of current pull request, only if pull request of glue has been merged!)*

# COMMAND ----------

# Set the database name and the column name you want to update
database_name = "`p-cdh`.derivative"
column_name = "PortfolioName"
old_value = "GXR One Holding"
new_value = "GLX One Iris DAC"

# List all tables in the database
tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()

for table in tables:
    table_name = table["tableName"]
    
    # Check if the column exists in the current table by describing the schema
    columns = spark.sql(f"DESCRIBE TABLE {database_name}.{table_name}").collect()
    column_names = [col["col_name"] for col in columns]
    
    if column_name in column_names:
        # If the column exists, perform the update where PortfolioName has the old value
        try:
            update_query = f"""
                UPDATE {database_name}.{table_name}
                SET {column_name} = '{new_value}'
                WHERE {column_name} = '{old_value}'
            """
            spark.sql(update_query)
            print(f"Updated column '{column_name}' in table '{table_name}' where '{old_value}' was found.")
        except Exception as e:
            print(f"Error updating table {table_name}: {e}")
    else:
        print(f"Column '{column_name}' does not exist in table '{table_name}'")
