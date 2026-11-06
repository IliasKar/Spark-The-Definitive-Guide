# Databricks notebook source
# DBTITLE 1,Alter tables in uc in order to map with hive
# Add columns to uc table --> landed.biteam_biteamlienanalysispropertycontract

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysispropertycontract
          ADD COLUMN StagingFlowProcessKey INT AFTER StagingDateInserted
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysispropertycontract
          ADD COLUMN StagingFlowProcessDataflowKey INT AFTER StagingFlowProcessKey
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysispropertycontract
          ADD COLUMN StagingFlowProcessExecutionKey INT AFTER StagingFlowProcessDataflowKey
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysispropertycontract
          ADD COLUMN StagingFlowProcessDataflowExecutionKey INT AFTER StagingFlowProcessExecutionKey
          """)



# Add columns to uc table --> landed.biteam_biteamlienanalysisltv

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysisltv
          ADD COLUMN StagingFlowProcessKey INT AFTER StagingDateInserted
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysisltv
          ADD COLUMN StagingFlowProcessDataflowKey INT AFTER StagingFlowProcessKey
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysisltv
          ADD COLUMN StagingFlowProcessExecutionKey INT AFTER StagingFlowProcessDataflowKey
          """)

spark.sql(f"""
          ALTER TABLE  `p-cdh`.landed.biteam_biteamlienanalysisltv
          ADD COLUMN StagingFlowProcessDataflowExecutionKey INT AFTER StagingFlowProcessExecutionKey
          """)

# COMMAND ----------

# DBTITLE 1,Function to normalize column names
# Function to normalize column names of hive tables
def normalize_column_names(hive_col):
    if "#" in hive_col:  
        return hive_col.replace("_", " ").split("#")[0] # replace "_" with space and keep the part before #
    return hive_col  # return the original string if '#' is not present

# COMMAND ----------

# DBTITLE 1,Parameters
# Set the parameters
database_uc = '`p-cdh`.landed'
database_hive = 'hive_metastore.`p-ec1-cepal-landed-area`'

# COMMAND ----------

# DBTITLE 1,Transfer landed data from hive to uc
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
        hive_table_df = spark.sql(f'''SELECT * FROM {database_hive}.{hive_table_name}''')
        
        # Create a list with the normaliized hive column names
        hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

        # Normalize the column names of hive table and create a temp view
        normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
        normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
            continue # skip to the next table in the loop


        
        # Overwrite data from hive to uc (for tables that have only common normalized columns)
        spark.sql(f"""
                INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                SELECT {common_columns}
                FROM normalized_hive_table_df
            """)

        print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 



    except Exception as e:
        # Catches any exception
        print(f"Error processing table {database_uc}.{uc_table_name}")

# COMMAND ----------

# DBTITLE 1,Check schemas mapping (datatypes & column names)
# Imports
from pyspark.sql.types import  IntegerType, DataType, ShortType, StringType, StructType, StructField

# Function to normalize the column names
def normalize_schema(schema):
    return StructType([
        StructField(field.name.lower(),  # Convert the field name to lowercase
                    IntegerType() if isinstance(field.dataType, ShortType) else  # Treat smallint as int
                    StringType() if isinstance(field.dataType, StringType) else  # Treat nvarchar as string
                    field.dataType, 
                    field.nullable)
        for field in schema.fields
    ])


for row in mapping_tables.collect():
    hive_table_name = row['TableNameHive']
    uc_table_name = row['TableNameUC']
    
    try:
        # Read the hive table schema to get the DataFrame
        hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

        # Normalize the column names of hive table and create a temp view
        hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]
        normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
        
        # Get the UC table DataFrame and drop 'source_BusinessDate'
        uc_table_df = spark.read.table(f"{database_uc}.{uc_table_name}").drop('source_BusinessDate')
        uc_table_df.createOrReplaceTempView('uc_table_df')        

        # Extract and Sort schemas by column name (lowercase)
        sorted_hive_schema = sorted(normalized_hive_table_df.schema.fields, key=lambda field: field.name.lower())
        sorted_uc_schema = sorted(uc_table_df.schema.fields, key=lambda field: field.name.lower())

        # Normalize both schemas using function normalize_schema
        normalized_hive_schema = normalize_schema(StructType(sorted_hive_schema))
        normalized_uc_schema = normalize_schema(StructType(sorted_uc_schema))
      

        # Compare schemas
        if normalized_hive_schema == normalized_uc_schema:
            print(f"Schemas match for table '{uc_table_name}' .")
        else:
            print(f"Schemas do not match for table '{uc_table_name}' .")

        if normalized_hive_schema != normalized_uc_schema:
            print(f"Schemas do not match for table '{uc_table_name}' .")


    except Exception as e:
        # Catches any exception
        print(f"Error processing table {uc_table_name} : {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Special cases following

# COMMAND ----------

# DBTITLE 1,biteam_liquidationvalue
hive_table_name = 'biteam_liquidationvalue' 
uc_table_name = 'biteam_liquidationvalue'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('Recoveries_from_Unsecured_Claims__2_5%_of_REV_#4', 'Recoveries from Unsecured Claims (2,5% of REV)')

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_kama_vw
hive_table_name = 'extfile_kama_vw' 
uc_table_name = 'extfile_kama_vw'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('Min_bucket_NCKAG_KAMAB3#2', 'Min bucket NCKAG_KAMAB3')

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_cl
hive_table_name = 'extfile_report_unified_cl' 
uc_table_name = 'extfile_report_unified_cl'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'Στοιχεία_χρηστών_validator_Processor_ανά_αίτημα#46', 'Στοιχεία χρηστών validator,Processor ανά αίτημα'
                ).withColumnRenamed(
                'registrator_name_#57', 'registrator_name '
                )

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_ml
hive_table_name = 'extfile_report_unified_ml' 
uc_table_name = 'extfile_report_unified_ml'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed('Στοιχεία_χρηστών_validator_Processor_ανά_αίτημα#1', 'Στοιχεία χρηστών validator,Processor ανά αίτημα')

# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,extfile_report_unified_sb_vw
hive_table_name = 'extfile_report_unified_sb_vw' 
uc_table_name = 'extfile_report_unified_sb_vw'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'First_Receipt_Date_ApprovalCenter_1#0', 'First_Receipt_Date_ApprovalCenter 1'
                ).withColumnRenamed(
                'Channel_Detail_2#1', 'Channel_Detail 2'
                ).withColumnRenamed(
                'HasAutomatedPrinting__anchor_settings_#3', 'HasAutomatedPrinting_(anchor_settings)'
                ).withColumnRenamed(
                'ExceptionFromAutomatedPrinting__anchor_settings_#4', 'ExceptionFromAutomatedPrinting_(anchor_settings)'
                )
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,repointqcab_coc_fields
hive_table_name = 'repointqcab_coc_fields' 
uc_table_name = 'repointqcab_coc_fields'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

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
                )
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 

# COMMAND ----------

# DBTITLE 1,repointqcab_exposuresn
hive_table_name = 'repointqcab_exposuresn' 
uc_table_name = 'repointqcab_exposuresn'

# Read the hive table schema to get the columns
hive_table_df = spark.read.table(f"{database_hive}.{hive_table_name}")

# Rename the columns in order to map to uc
hive_table_df = hive_table_df.withColumnRenamed(
                'BANK_NO_BANK#0', 'BANK NO_BANK'
                )
# Create a list with the normalized hive column names
hive_columns_normalized = [normalize_column_names(col) for col in hive_table_df.columns]

# Normalize the column names of hive table and create a temp view
normalized_hive_table_df = hive_table_df.toDF(*hive_columns_normalized)
normalized_hive_table_df.createOrReplaceTempView('normalized_hive_table_df')


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
    spark.sql(f"""
                    INSERT overwrite TABLE {database_uc}.{uc_table_name} ({common_columns})
                    SELECT {common_columns}
                    FROM normalized_hive_table_df
                """)

    print(f"Data successfully inserted into table : {database_uc}.{uc_table_name}") 
