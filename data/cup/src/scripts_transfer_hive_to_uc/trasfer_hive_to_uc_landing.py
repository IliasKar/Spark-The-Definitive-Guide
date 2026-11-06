# Databricks notebook source
# DBTITLE 1,Function to normalize column names
# Function to normalize column names of hive tables
def normalize_column_names(hive_col):
    if "#" in hive_col:  
        return hive_col.replace("_", " ").split("#")[0] # replace "_" with space and keep the part before #
    return hive_col  # return the original string if '#' is not present

# COMMAND ----------

# DBTITLE 1,Parameters
# Set the parameters
database_uc = '`p-cdh`.landing'
database_hive = 'hive_metastore.`p-ec1-cepal-landing-area`'
business_date = '2024-10-31' # defines the date that uc flow started daily

# COMMAND ----------

# DBTITLE 1,Transfer landing data from hive to uc
from pyspark.sql.functions import to_date, concat, substring

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
# Filter mapping tables for specific table names
# mapping_tables = mapping_tables.filter(mapping_tables.TableNameUC.isin('cti_dialerreport'))

# Normalize column names for each hive table in order to map the columns of uc
for row in mapping_tables.collect():
    hive_table_name = row['TableNameHive']
    uc_table_name = row['TableNameUC']
    
    try:
        # Read the hive table schema and rename columns
        hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                                        FROM {database_hive}.{hive_table_name}''')
        hive_table_df = hive_table_df.drop("businessdate"
                                    ).drop("partition_0")
    
        
        # Create a list with the normaliized hive column names
        hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

        # Normalize the column names of hive table and create a temp view
        normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
        normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


        # Get the list of columns in uc
        uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

        uc_columns = [col for col in uc_table_df.columns if col not in {'source_BusinessDate', 'BusinessDate', 'BusinessDATE'}]
        

        # Select columns exist in both uc and hive normalized tables
        columns_exist_in_hive_and_uc = [
            item.lower() for item in uc_columns 
                if item.lower() in [col.lower() for col in hive_columns_normalized]
            ]

        common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


        # Check columns do not exist in both uc and hive normalized tables
        columns_in_uc_and_not_in_hive = [
            item.lower() for item in uc_columns 
                if item.lower() not in [col.lower() for col in hive_columns_normalized]
            ]
        columns_in_hive_and_not_in_uc = [
            item.lower() for item in hive_columns_normalized 
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
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

        b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

        print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 



    except Exception as e:
        # Catches any exception
        print(f"Error processing table {database_uc}.{uc_table_name} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Special cases following

# COMMAND ----------

# DBTITLE 1,biteam_liquidationvalue
hive_table_name = 'biteam_liquidationvalue' 
uc_table_name = 'biteam_liquidationvalue'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                                    'Recoveries_from_Unsecured_Claims__2_5%_of_REV_#4', 'Recoveries from Unsecured Claims (2,5% of REV)'
                                    ).drop("businessdate"
                                    ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
norπmali zed χωρί ς_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'buρρ  .γ   ... φ
sinessdateν' in hive_columns_normalized: 
       ψ     hive_columns_nόμωςormalized.remove('businessdate')
ψ

# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_file_auctionrecoveries
hive_table_name = 'extfile_file_auctionrecoveries' 
uc_table_name = 'extfile_file_auctionrecoveries'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('τιμή_α΄προσφοράς___μόνο_στην_περίπτωση_όπου_υπερ_στής_είναι_alpha_bank_#9', 'Τιμή Α΄Προσφοράς  (μόνο στην περίπτωση όπου Υπερ στής είναι Alpha Bank)'
                                                ).drop("businessdate"
                                                ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_file_recoveries_total
hive_table_name = 'extfile_file_recoveries_total' 
uc_table_name = 'extfile_file_recoveries_total'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('εξοδα_εκτελεσεωσ_προσ_αποδοση_στο_επισπευδων_spv_alpha_bank#3', 'ΕΞΟΔΑ ΕΚΤΕΛΕΣΕΩΣ ΠΡΟΣ ΑΠΟΔΟΣΗ ΣΤΟ ΕΠΙΣΠΕΥΔΩΝ SPV ALPHA BANK'
                                    ).drop("businessdate"
                                    ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_judicialunderlaw3869
hive_table_name = 'extfile_judicialunderlaw3869' 
uc_table_name = 'extfile_judicialunderlaw3869'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('τυποσ_δικαστηριου#4', 'ΤΥΠΟΣ ΔΙΚΑΣΤΗΡΙΟΥ'
                                                ).drop("businessdate"
                                                ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_kama
hive_table_name = 'extfile_kama' 
uc_table_name = 'extfile_kama'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('min_bucket_nckag_kamab3#6', 'Min bucket NCKAG_KAMAB3'
                                    ).drop("businessdate"
                                    ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_kama_vw
hive_table_name = 'extfile_kama_vw' 
uc_table_name = 'extfile_kama_vw'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('Min_bucket_NCKAG_KAMAB3#2', 'Min bucket NCKAG_KAMAB3'
                                    ).drop("businessdate"
                                    ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_cl
hive_table_name = 'extfile_report_unified_cl' 
uc_table_name = 'extfile_report_unified_cl'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'Στοιχεία_χρηστών_validator_Processor_ανά_αίτημα#46', 'Στοιχεία χρηστών validator,Processor ανά αίτημα'
                ).withColumnRenamed(
                'registrator_name_#57', 'registrator_name '
                ).drop("businessdate"
                ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_ml
hive_table_name = 'extfile_report_unified_ml' 
uc_table_name = 'extfile_report_unified_ml'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
    'Στοιχεία_χρηστών_validator_Processor_ανά_αίτημα#1', 'Στοιχεία χρηστών validator,Processor ανά αίτημα'
                                    ).drop("businessdate"
                                    ).drop("partition_0")

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_sb_vw
hive_table_name = 'extfile_report_unified_sb_vw' 
uc_table_name = 'extfile_report_unified_sb_vw'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'First_Receipt_Date_ApprovalCenter_1#0', 'First_Receipt_Date_ApprovalCenter 1'
                ).withColumnRenamed(
                'Channel_Detail_2#1', 'Channel_Detail 2'
                ).withColumnRenamed(
                'HasAutomatedPrinting__anchor_settings_#3', 'HasAutomatedPrinting_(anchor_settings)'
                ).withColumnRenamed(
                'ExceptionFromAutomatedPrinting__anchor_settings_#4', 'ExceptionFromAutomatedPrinting_(anchor_settings)'
                ).drop("businessdate"
                ).drop("partition_0")
                
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,repointqcab_coc_fields
hive_table_name = 'repointqcab_coc_fields' 
uc_table_name = 'repointqcab_coc_fields'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'UPDATED_BY__1st_LETTER_#0', 'UPDATED_BY (1st_LETTER)'
                ).withColumnRenamed(
                'Updated_by__1st_reminder_letter_#3', 'Updated by (1st reminder letter)'
                ).withColumnRenamed(
                'UPDATED_BY__2nd_LETTER_#4', 'UPDATED_BY (2nd_LETTER)'
                ).withColumnRenamed(
                'UPDATED_BY__3rd_LETTER_#5', 'UPDATED_BY (3rd_LETTER)'
                ).withColumnRenamed(
                'CoC_Outstanding_Balance_LCY_#6', 'CoC_Outstanding_Balance(LCY)'
                ).withColumnRenamed(
                'Letter__Conduct_Code_#7', 'Letter_(Conduct_Code)'
                ).withColumnRenamed(
                'Letter_Date__Conduct_Code_#8', 'Letter_Date_(Conduct_Code)'
                ).withColumnRenamed(
                'Committee_Decision__LoV_#11', 'Committee Decision (LoV)'
                ).withColumnRenamed(
                'Exclusion_Reason_from_CoC_Letters__LoV_#14', 'Exclusion Reason from CoC Letters (LoV)'
                ).drop("businessdate"
                ).drop("partition_0")
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,repointqcab_exposuresn
hive_table_name = 'repointqcab_exposuresn' 
uc_table_name = 'repointqcab_exposuresn'

# Read the hive table schema to get the columns
hive_table_df = spark.sql(f'''SELECT *, to_date(concat(
                                                        cast(substring(partition_0, 1, 4) AS STRING), '-',
                                                        cast(substring(partition_0, 5, 2) AS STRING), '-',
                                                        cast(substring(partition_0, 7, 2) AS STRING)
                                                            )) AS Cluster_0  
                            FROM {database_hive}.{hive_table_name}''')

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('BANK_NO_BANK#0', 'BANK NO_BANK'
                                    ).drop("businessdate"
                                    ).drop("partition_0")
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')
if 'businessdate' in hive_columns_normalized: 
            hive_columns_normalized.remove('businessdate')


# Get the list of columns in uc
uc_table_df = spark.sql(f'''SELECT * FROM {database_uc}.{uc_table_name}''')

uc_columns = uc_table_df.columns
if 'source_BusinessDate' in uc_columns: 
    uc_columns.remove('source_BusinessDate')
        

# Select columns exist in both uc and hive normalized tables
columns_exist_in_hive_and_uc = [
    item.lower() for item in uc_columns 
        if item.lower() in [col.lower() for col in hive_columns_normalized]
    ]

common_columns = ",".join([f"`{col}`" for col in columns_exist_in_hive_and_uc])


# Check columns do not exist in both uc and hive normalized tables
columns_in_uc_and_not_in_hive = [
    item.lower() for item in uc_columns 
        if item.lower() not in [col.lower() for col in hive_columns_normalized]
    ]
columns_in_hive_and_not_in_uc = [
    item.lower() for item in hive_columns_normalized 
        if item.lower() not in [col.lower() for col in uc_columns]
    ]
        
# Skip tables that normalized columns are different
if columns_in_uc_and_not_in_hive != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in uc and not in hive :", columns_in_uc_and_not_in_hive)
if columns_in_hive_and_not_in_uc != []:
    print(f"Skipping table : {database_uc}.{uc_table_name}: \n\tDiff in columns --> Columns exist in hive and not in uc :", columns_in_hive_and_not_in_uc)

# Overwrite data from hive to uc (for tables that all of their columns map to uc)
else:    
    a = spark.sql(f"""
                      DELETE FROM {database_uc}.{uc_table_name}
                      WHERE Cluster_0 < date('{business_date}')
                      """)

    b = spark.sql(f"""
                    INSERT INTO {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                    WHERE Cluster_0 < date('{business_date}')
            """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 
