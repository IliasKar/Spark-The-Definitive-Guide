# Databricks notebook source
from datetime import datetime
from typing import List

# COMMAND ----------

# Configuration

try:
    qa_schema_to_drop = dbutils.widgets.get("qa_schema_to_drop")
except Exception as e:
    raise ValueError("No QA schema provided. Please provide a schema to drop.")

qa_schemas_to_drop = [qa_schema_to_drop]


prod_schemas = [schema.replace('qa-', 'p-') for schema in qa_schemas_to_drop]
current_date = datetime.now().strftime('%Y%m%d')

# COMMAND ----------

def get_all_tables_and_views(schema: str) -> tuple:
    """
    Returns all tables and views in the given schema using information_schema
    """
    clean_schema = schema.replace('`', '')
    catalog, schema_name = clean_schema.split('.')

    print(f"Processing catalog:{catalog}.{schema_name}")
    
    # Get views
    views_query = f"""
    SELECT table_name 
    FROM `{catalog}`.information_schema.views
    WHERE table_schema = '{schema_name}'
    """
    
    # Get tables
    tables_query = f"""
    SELECT table_name 
    FROM `{catalog}`.information_schema.tables
    WHERE table_schema = '{schema_name}'
    """

    print(views_query)

    print(tables_query)
    
    views = spark.sql(views_query).collect()
    tables = spark.sql(tables_query).collect()
    
    view_list = [f"`{catalog}`.`{schema_name}`.`{view.table_name}`" for view in views]
    table_list = [f"`{catalog}`.`{schema_name}`.`{table.table_name}`" for table in tables]
    
    return view_list, table_list

# COMMAND ----------

def drop_views(views: List[str]) -> None:
    """
    Drops all views in the provided list
    """
    for view in views:

        spark.sql(f"ALTER TABLE {view} SET OWNER TO `ikaralis@cepal.gr`")
        print(f"Owner changed: {view}")

        spark.sql(f"DROP VIEW IF EXISTS {view}")
        print(f"Dropped view: {view}")

# COMMAND ----------

def backup_tables(tables: List[str], date_suffix: str) -> None:
    """
    Renames tables to backup format
    """
    for table in tables:
        # if the table is not backuped form a previous run then back it up.
        if '_backup_' not in table:
            spark.sql(f"ALTER TABLE {table} OWNER TO `npapastefanakis@cepal.gr`")
            print(f"Owner changed: {table}")
        
            new_name = f"{table}_backup_{date_suffix}"
            new_name = f"{new_name.replace('`_backup_', '_backup_')}`"

            print(f"new_name: {new_name}")

            spark.sql(f"ALTER TABLE {table} RENAME TO {new_name}")
            print(f"Renamed table to: {new_name}")

# COMMAND ----------

def create_views_from_prod(prod_schema: str) -> None:
    """
    Creates views in QA schema pointing to production tables
    """
    _, prod_tables = get_all_tables_and_views(prod_schema)
    qa_schema = prod_schema.replace('p-', 'qa-')
    
    for prod_table in prod_tables:
        table_name = prod_table.split('.')[-1]
        qa_view = f"{qa_schema}.{table_name}"
               
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {qa_view}
        AS SELECT * FROM {prod_table}
        """
        spark.sql(create_view_sql)
        print(f"Created view: {qa_view}")

# COMMAND ----------

# Main execution
def main():
    for qa_schema in qa_schemas_to_drop:
        print(f"\nProcessing schema: {qa_schema}")
        
        views, tables = get_all_tables_and_views(qa_schema)

        if views:            
            print("QA views found.")
            # display(views)
            drop_views(views)
        else:
            print("No QA views found.")

        if tables:
            print("QA tables found.")
            # display(tables)
            backup_tables(tables, current_date)
        else:
            print("No QA tables found.")


    for prod_schema in prod_schemas:
        print(f"\nProcessing schema: {prod_schema}")        
        create_views_from_prod(prod_schema)


# COMMAND ----------

# Execute the main function
main()
