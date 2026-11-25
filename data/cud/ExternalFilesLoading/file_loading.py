from typing import Any
import chardet
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType,StringType, StructField,StructType, DateType
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_utc_timestamp,
    lit,
    input_file_name, 
    regexp_extract
)
import re

# Import the module
import sys
sys.path.append("../aws_utils")
import aws_utils
# Get Environment Prefix
env = aws_utils.get_env_prefix()

def load_file (
    spark: SparkSession, args: dict[str, Any] ,)  -> None:
        arg_keys = [
            "relative_root_path",
            "file_name",
            "file_extension",
            "destination_bucket",
            "source_bucket",
            "input_path",
            "output_path",
            "separator",
            "output_file_format",
            "unity",
            "incremental",
        ]
        (   
            relative_root_path,
            file_name,
            file_extension,
            destination_bucket,
            source_bucket,
            input_path,
            output_path,
            separator,
            output_file_format,
            unity,
            incremental,
        ) = [args.get(key) for key in arg_keys]


        if file_extension.lower() == 'csv' or file_extension == 'json' or file_extension.lower() == 'txt':
            object_key = f"{input_path}/{file_name}.{file_extension}"
        else:
            object_key = f"{input_path}/{file_name}"
            
        file_path = f"s3a://{source_bucket}/{object_key}"
  
        destination_table_path = f"s3a://{destination_bucket}/{output_path}/in/{relative_root_path}/{file_name}"

        # Define the relative_path, which is the main folder
        relative_path = relative_root_path.split("/")[0]

        metadata_sql = None 
        has_set_sepatator = None
        has_header = None
        encoding = None
        previous_business_date = None
        needs_header_mapping = None

        # Define the table name in case the external file has a date in the file name
        table_name_without_date = relative_root_path.split("/")[1] if len(relative_root_path.split("/")) > 1 else None # the files that have business date are inside a subfolder (of the main folder)
        Flag_BusinessDt = spark.sql(f"""SELECT DISTINCT HasBusinessDate /*true or false values*/
                                                FROM `{env}-cdh`.`workflow`.ExternalFilesConfiguration 
                                                WHERE LOWER(TableName) = LOWER('{table_name_without_date}') 
                                                AND TableName IS NOT NULL""").collect()

        table_has_business_date = Flag_BusinessDt[0]['HasBusinessDate'] if Flag_BusinessDt else None

        if table_has_business_date: 
            #Check if there is metadata for this table in externalfilesmetadata table
            metadata_sql = spark.sql(f"""SELECT Separator, Header, Encoding, PreviousBusinessDate, NeedsHeaderMapping FROM `{env}-cdh`.`workflow`.ExternalFilesColumnConfiguration
                                                WHERE UPPER(TableName) = UPPER('{table_name_without_date}') """).collect()
            
            if metadata_sql:
                metadata = metadata_sql[0]
                has_set_sepatator = metadata["Separator"]
                has_header = metadata["Header"]
                encoding = metadata["Encoding"]
                previous_business_date = metadata["PreviousBusinessDate"]
                needs_header_mapping = metadata["NeedsHeaderMapping"]

            table_name = f"ext{table_name_without_date}"    # because table_name_without_date != none (has subfolder name because len(relative_root_path.split("/")) > 1)

        elif  table_name_without_date is not None:
            
            table_name = table_name_without_date

        else:
            table_name = file_name

        print(table_name)

        if incremental == 'False': 
            mode_incremental = "overwrite"
            option_incremental = "overwriteSchema"
            current_flow = 'Snapshot'
        else: 
            mode_incremental = "append"
            option_incremental = "mergeSchema"
            current_flow = 'Incremental' 

        # Read the configuration table
        try:
            # Original Condition
            generic_condition = f"UPPER(RelativePath) = UPPER('{relative_path}') AND UPPER(Flow) = UPPER('{current_flow}')"

            # Find if external files table name exists in configuration table
            external_files_table_name_exists = (spark.sql(f"""SELECT DISTINCT tablename from `{env}-cdh`.`workflow`.ExternalFilesConfiguration""").filter(f"UPPER(tablename) = UPPER('{table_name_without_date}')").count()>0)

            # Condition if tablename exists
            if external_files_table_name_exists:
                final_condition = f"{generic_condition} AND UPPER(tablename)=UPPER('{table_name_without_date}')"
            else:
                final_condition = f"{generic_condition} AND tablename IS NULL"  # generic_condition
            # Retrieve the Unity Catalog and Schema information
            unitycatalog_info_sql = spark.sql(f"""SELECT UnityCatalog, UnitySchema, TableName, Flow, string_columns
                                                FROM `{env}-cdh`.`workflow`.ExternalFilesConfiguration
                                                WHERE  {final_condition}""").collect()[0]

            
            # If the query succeeds, access fields normally
            unity_catalog = unitycatalog_info_sql["UnityCatalog"]
            unity_schema = unitycatalog_info_sql["UnitySchema"]
            string_columns = unitycatalog_info_sql["string_columns"] # Get the comma delimnited string with the column names we want to preserve as strings
        except IndexError:
            raise ValueError("No records found in ExternalFilesConfiguration for the given RelativePath and Flow. Check these values and try again!")
        

        if file_extension.lower() == 'csv' or file_extension.lower()=='txt':
            # First, infer the schema without loading the data
            
            separator = has_set_sepatator if has_set_sepatator is not None else separator 
            header = has_header if has_header is not None else "true"
            encoding = "UTF-8" if encoding is None or encoding.lower() == "utf-8-sig" else encoding
            has_bom = encoding.lower() == "utf-8-sig"

            temp_df = (
                    spark.read.format("csv")
                    .option("header", header)
                    .option("sep", separator)
                    .option("encoding", encoding)
                    .option("inferSchema", "true")
                    .option("samplingRatio", 0.1)
                    .csv(file_path)
            )
            
            
            #Check if the columns need to be retrieved because the file has no headers or the headers are greek
            if metadata_sql and needs_header_mapping:
            #Retrieve the Column Mapping
                column_mapping_sql = spark.sql(f"""SELECT GreekColumn,Englishcolumn FROM `{env}-cdh`.`workflow`.ExternalFilesColumnConfiguration
                                                WHERE UPPER(TableName) = UPPER('{table_name_without_date}') ORDER BY ColumnIndex ASC""").collect()
            
                #Create column mapping dic
                column_mapping = {}
                for row in column_mapping_sql:
                    greek_column = row["GreekColumn"]
                    english_column = row["Englishcolumn"]

                #In some cases the headers are completely missing, and in other cases the columns are greek. Hence the following logic
                    if greek_column.isdigit():
                        column_name = f"_c{int(greek_column)}"
                    else:
                        column_name = greek_column

                    column_mapping[column_name] = english_column
                

                
                for old_col_name in temp_df.columns:                    
                    rename_column = column_mapping.get(old_col_name)
                    if rename_column:

                        temp_df = temp_df.withColumnRenamed(old_col_name, rename_column)

            schema = temp_df.schema

            # Create a new schema with modified column types
            modified_schema = []
            

            # Convert the string_columns to an array of strings
            string_columns_to_preserve = []
            if string_columns and string_columns.strip():
                # Split by comma and strip whitespace from each column name
                string_columns_to_preserve = [col.strip() for col in string_columns.split(',')]
            
            if table_has_business_date:   
                 modified_schema = [StructField(column, StringType(), True) for column in temp_df.columns]
            else:
                for field in schema.fields:
                    if field.name in string_columns_to_preserve:
                        modified_schema.append(StructField(field.name, StringType(), field.nullable))
                    else:
                        modified_schema.append(field)
            
            final_schema = StructType(modified_schema)


            # Now read with the modified schema
            input_df = (
                spark.read.format("csv")
                .option("header", header)
                .option("sep", separator)
                .option("encoding", encoding)
                .schema(final_schema)
                .option("nullValue", "null")
                .option("quote", '"')                  # newlines with quotes
                .option("escape", '"')                 # escape character (for values with quotes inside quotes)
                .option("multiLine", "true")           # read multiline records                
                .csv(file_path)
            )

            input_df = input_df.replace('NULL', None)

            #Remove bom from column name in case encoding is UTF-8 BOM 
            if has_bom:
                input_df = input_df.toDF(*[column.replace('\ufeff','') for column in input_df.columns])

        elif file_extension == 'parquet':
            input_df = spark.read.parquet(file_path)
        elif file_extension == 'json':
            input_df = spark.read.json(file_path)
        elif file_extension == 'delta':
            input_df = spark.read.delta(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

                
        for old_col_name in input_df.columns:
            new_col_name = old_col_name.replace(" ", "")
            input_df = input_df.withColumnRenamed(old_col_name, new_col_name)
        

 
        # Find BusinessDate from filename
        business_date=None
        if table_has_business_date:
            find_date = re.search(r"(\d{8})",file_name)
            if find_date:
                try: 
                    business_date = datetime.strptime(find_date.group(1),"%Y%m%d").strftime("%Y-%m-%d")
                # Make sure 8 digits are valid date
                except ValueError:
                    business_date = None


                                          
        # Adding IngestionTimestamp column
        final_df = input_df.withColumn(
            "IngestionTimestamp",
            lit(current_timestamp()).cast(TimestampType())
        )
        #Find previous business day
        if business_date:
            previous_business_day_sql = spark.sql(f""" SELECT PreviousBusinessDay FROM `{env}-cdh`.`reference`.ref_calendar WHERE CalendarDate= DATE('{business_date}')""").collect()

        # Add BusinessDate column if the value exists
        if business_date:
            if previous_business_date:
                final_df = final_df.withColumn("BusinessDate",lit(previous_business_day_sql[0]["PreviousBusinessDay"]).cast(DateType()))
            else:
                final_df = final_df.withColumn("BusinessDate",lit(business_date).cast(DateType()))

        #final_df.printSchema()


        # Write output                     
        if unity == 'False':
            final_df.write \
                    .format(output_file_format) \
                    .mode(mode_incremental) \
                    .option(option_incremental, True) \
                    .option("encoding","UTF-8") \
                    .save(destination_table_path)
        else:
            destination_bucket = f"`{unity_catalog}`.`{unity_schema}`"
            if option_incremental == "overwriteSchema":
                # Check if table exists before truncating
                table_exists = spark.catalog.tableExists(f"{destination_bucket}.`{table_name}`")
                if table_exists:
                    spark.sql(f"DROP TABLE {destination_bucket}.`{table_name}`")
            final_df.write \
                    .format(output_file_format) \
                    .mode(mode_incremental) \
                    .option(option_incremental, False) \
                    .option("encoding","UTF-8") \
                    .saveAsTable(f"{destination_bucket}.`{table_name}`") 
            spark.sql(
                f"""REPAIR TABLE {destination_bucket}.`{table_name}` SYNC METADATA;"""
            )
            


def file_to_landing_to_reference (
    spark: SparkSession, args: dict[str, Any] ,)  -> None:
        arg_keys = [
            "relative_root_path",
            "file_name",
            "file_extension",
            "incoming_folder",
            "output_folder",
            "separator",
            "unity",
            "incremental",
        ]
        (   
            relative_root_path,
            file_name,
            file_extension,
            incoming_folder,
            output_folder,
            separator,
            unity,
            incremental,
        ) = [args.get(key) for key in arg_keys]
     
        landing_bucket = f"{env}-ec1-cepal-landing-zone"
        reference_bucket = f"{env}-ec1-cepal-reference-zone"
        input_path = f"{incoming_folder}/in/{relative_root_path}"

        # Load to Landing
        # Put the file you need to upload to reference area as csv or json format in 'd-ec1-cepal-landing-zone.incoming/one_off folder
        file_extension = file_name.split(".")[-1]
        file_name = file_name.split(".")[0]
        
        arg_keys = {
                "relative_root_path": relative_root_path,
                "file_name": file_name,
                "file_extension": file_extension,
                "destination_bucket": reference_bucket,
                "source_bucket": landing_bucket,
                "input_path": input_path,
                "output_path": output_folder,
                "separator": separator,
                "output_file_format": 'delta',
                "unity" : unity,
                "incremental": incremental
        }
        
        load_file(spark,arg_keys)

    