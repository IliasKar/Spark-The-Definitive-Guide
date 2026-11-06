# Databricks notebook source
# DBTITLE 1,Parameters
# Set the parameters
database_uc = '`p-cdh`.primaryactive'
database_hive = 'hive_metastore.`p-ec1-cepal-curated-area`'

# COMMAND ----------

# DBTITLE 1,Transfer curated data from hive to uc
# Select the table names of uc and hive
tables_uc = spark.sql(f"SHOW TABLES IN {database_uc}")
tables_hive = spark.sql(f"SHOW TABLES IN {database_hive}")
table_names_df_uc = tables_uc.select('tableName').withColumnRenamed('tableName', 'TableNameUC')
table_names_df_hive = tables_hive.select('tableName').withColumnRenamed('tableName', 'TableNameHive')

# Map the tables between uc and hive
mapping_tables = table_names_df_uc.join(table_names_df_hive
                                              , table_names_df_uc['TableNameUC'] == table_names_df_hive['TableNameHive']
                                              , "inner" 
                                             )

# Normalize column names for each hive table in order to map the columns of uc
for row in mapping_tables.collect():
    hive_table_name = row['TableNameHive']
    uc_table_name = row['TableNameUC']
    
    try:
        # Read the hive table schema to get the columns
        hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

        # Get the list of columns in hive
        hive_columns = spark.sql(f"DESCRIBE {database_hive}.{hive_table_name}").select('col_name')
        total_rows_count = hive_columns.count()
        hive_columns = hive_columns.limit(total_rows_count - 3) # exclude 3 last columns that exist in describe command (`# Clustering Information`, `# col_name`, `BusinessDate`)
        hive_columns = [row['col_name'] for row in hive_columns.collect()]

        # Create a temp view for hive table
        hive_table_df.createOrReplaceTempView('hive_table_df')


        # Get the list of columns in uc
        uc_columns = spark.sql(f"DESCRIBE {database_uc}.{uc_table_name}").select('col_name')
        total_rows_count = uc_columns.count()
        uc_columns = uc_columns.limit(total_rows_count - 3) # exclude 3 last columns that exist in describe command (`# Clustering Information`, `# col_name`, `BusinessDate`)
        uc_columns = [row['col_name'] for row in uc_columns.collect()]



        # Select columns exist in both uc and hive normalized tables
        columns_exist_in_hive_and_uc = [
            item.lower() for item in uc_columns 
                if item.lower() in [col.lower() for col in hive_columns]
            ]

        common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


        # Check columns do not exist in both uc and hive normalized tables
        columns_in_uc_and_not_in_hive = [
            item.lower() for item in uc_columns 
                if item.lower() not in [col.lower() for col in hive_columns]
            ]
        columns_in_hive_and_not_in_uc = [
            item.lower() for item in hive_columns 
                if item.lower() not in [col.lower() for col in uc_columns]
            ]
        
        # Skip tables that normalized columns are different
        if columns_in_uc_and_not_in_hive != []:
            print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
        if columns_in_hive_and_not_in_uc != []:
            print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)
            continue # skip to the next table in the loop


        
        # Overwrite data from hive to uc (for tables that have only common normalized columns)
        a = spark.sql(f"""
                INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                SELECT {common_columns}
                FROM hive_table_df
            """)

        print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 



    except Exception as e:
        # Catches any exception
        print(f"Error processing table {database_uc}.{uc_table_name} : {e}")
