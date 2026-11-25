# Databricks notebook source
# MAGIC %md
# MAGIC ## Initial authorization cell
# MAGIC
# MAGIC [Documentation https://cepalhellas.atlassian.net/wiki/spaces/DE/pages/113213614/GEMI+GEMH-GemiFilesToDB+notebook+specs](https://cepalhellas.atlassian.net/wiki/spaces/DE/pages/113213614/GEMI+GEMH-GemiFilesToDB+notebook+specs)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import collect_set,explode_outer, collect_list, concat_ws,concat, first, col, when, rank, datediff, months_between, lit, to_timestamp, abs as pyspark_abs
from pyspark.sql import Window,Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, NullType, IntegerType, ArrayType, LongType, TimestampType
from datetime import datetime 
import boto3
from pyspark.sql.utils import AnalysisException
import os
import botocore.session
from botocore.exceptions import NoCredentialsError
import traceback
import requests
import json
import random
from user_agents import parse


#takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env_prefix = os.environ["S3PREFIX"] (not working always.. deprecated. Moved to SSM)

# Get the current date to add to the csv filename
curr_date = datetime.now().strftime("%Y-%m-%d")


# Connect to AWS using boto3
ssm_client = boto3.client('ssm', region_name='eu-central-1')

s3_client = boto3.client('s3', region_name='eu-central-1')                           

# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

    
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## INPUTS

# COMMAND ----------

# Get the json files source directory path
source_directory = dbutils.widgets.get("json_data_path")  

# Get the batch size
batch_size = int(dbutils.widgets.get("batch_size"))  

# Get the minimum reference date based on which we are going to decide if the geminumer will be refreshed or not. If the gemi number is already stored with reference date > min_reference_date then it will be skipped.
try:
    min_reference_date = dbutils.widgets.get("min_reference_date")
except Exception as e:
    min_reference_date = "" 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the list of the already registered Gemi numbers

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Registered geminumbers query returning filenames
registered_gemi_numbers_query = """
SELECT DISTINCT CONCAT(geminumber, '.json') AS filename    FROM `gemi`.default.gemi_company_info 
"""

if min_reference_date != "":
    registered_gemi_numbers_query = registered_gemi_numbers_query + f" WHERE referencedate >= '{min_reference_date}'"


# Use distinct() function to select the distinct values of geminumber column and save it in a new DataFrame
registered_gemi_numbers_df = spark.sql(registered_gemi_numbers_query).cache()

registered_gemi_numbers_df = registered_gemi_numbers_df.withColumn("filename", regexp_replace("filename", "^0+", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema definition

# COMMAND ----------

tables = [
    "gemi_company_info",
    "gemi_capital",
    "gemi_management_persons",
    "gemi_decisions",
    "gemi_modification_history",
    "gemi_kad_data",
    "gemi_representation",
    "gemi_documents",
    "gemi_company_financials",
    "gemi_balancesheets",
    "gemi_auditors"
]

gemi_vat_processed_schema = StructType([
    StructField("afm", StringType(), nullable=False)
])

# Define the schema for company_info DataFrame
company_info_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("companyName", StringType(), nullable=True),
    StructField("afm", StringType(), nullable=True),
    StructField("addressCity", StringType(), nullable=True),
    StructField("isSuspended", StringType(), nullable=True),
    StructField("branchParentOrigin", StringType(), nullable=True),  # Update for new field
    StructField("titles", StringType(), nullable=True),  # Update for new field
    StructField("namei18n", StringType(), nullable=True),  # Update for new field
    StructField("companyStatus", StringType(), nullable=True),  # Update for new field
    StructField("legalType", StringType(), nullable=True),  # Update for new field
    StructField("branchType", StringType(), nullable=True),  # Update for new field
    StructField("dateGemiRegistered", StringType(), nullable=True),  # Update for new field
    StructField("company_municipality", StringType(), nullable=True),  # Update for new field
    StructField("company_zip_code", StringType(), nullable=True),  # Update for new field
    StructField("companyWebsite", StringType(), nullable=True),  # Update for new field
    StructField("companyEshop", StringType(), nullable=True),  # Update for new field
    StructField("companyContactNumber", StringType(), nullable=True),  # Update for new field
    StructField("ChamberResponsibleForCompany", StringType(), nullable=True),  # Update for new field
    StructField("branchData", StringType(), nullable=True),  # Update for new field
    StructField("telephone", StringType(), nullable=True),  # Add telephone field to the schema
    StructField("email", StringType(), nullable=True)  # Add email field to the schema
])

# Define the schema for capital DataFrame
capital_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("descr", StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),
    StructField("nominal_price", StringType(), nullable=True),  # Update for new field
    StructField("capital_stock", StringType(), nullable=True),  # Update for new field
    StructField("company_id", StringType(), nullable=True),  # Update for new field
    StructField("corporate_status_id", StringType(), nullable=True),  # Update for new field
    StructField("currency", StringType(), nullable=True)
])

# Define the schema for management_persons DataFrame
management_persons_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("firstName", StringType(), nullable=True),
    StructField("lastName", StringType(), nullable=True),
    StructField("active", StringType(), nullable=True),  # Update for new field
    StructField("afm", StringType(), nullable=True),
    StructField("dateFrom", StringType(), nullable=True),
    StructField("dateElection", StringType(), nullable=True),  # Update for new field
    StructField("dateTo", StringType(), nullable=True),
    StructField("percentage", StringType(), nullable=True),  # Update for new field
    StructField("capacityDescr", StringType(), nullable=True),
    StructField("shared", StringType(), nullable=True),  # Update for new field
    StructField("nonShared", StringType(), nullable=True),  # Update for new field
    StructField("tableName", StringType(), nullable=True)  # Update for new field
])

    # Define the schema for decisions DataFrame
decisions_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("body_date_of_assembly", StringType(), nullable=True),
    StructField("body_type", StringType(), nullable=True),
    StructField("descr", StringType(), nullable=True),
    StructField("publish_date", StringType(), nullable=True),
    StructField("deposit_date", StringType(), nullable=True),
    StructField("orderDate", StringType(), nullable=True),
    StructField("decision_id", StringType(), nullable=True),
    StructField("system_file", StringType(), nullable=True),
    StructField("prfile_is_public", StringType(), nullable=True)
])


# Define the schema for modification_history DataFrame
modification_history_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("announcement_date", StringType(), nullable=True),
    StructField("registration_date", StringType(), nullable=True),
    StructField("orderDate", StringType(), nullable=True),
    StructField("modification_id", StringType(), nullable=True),
    StructField("system_file", StringType(), nullable=True),
    StructField("descr", StringType(), nullable=True)
])

# Define the schema for kadData DataFrame
kad_data_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("objective", StringType(), nullable=True),
    StructField("activities", StringType(), nullable=True),
    StructField("kad", StringType(), nullable=True),
    StructField("descr", StringType(), nullable=True)
])

# Define the schema for representation DataFrame
representation_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("afm", StringType(), nullable=True),
    StructField("active", StringType(), nullable=True),
    StructField("activeFrom", StringType(), nullable=True),
    StructField("activeTo", StringType(), nullable=True)
])

# Define the schema for documents DataFrame
documents_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("descr", StringType(), nullable=True),
    StructField("cost", IntegerType(), nullable=True)
])    

# Create DataFrames for each related table
company_financial_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("referencePeriod", StringType(), nullable=True),
])

balancesheet_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("referencePeriod", StringType(), nullable=True),
    StructField("id", IntegerType(), nullable=True),
    StructField("balancesheet_id", IntegerType(), nullable=True),
    StructField("bal_date", TimestampType(), nullable=True),
    StructField("bal_file_system_file_path", StringType(), nullable=True)
])

auditors_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
    StructField("referencePeriod", StringType(), nullable=True),
    StructField("auditorName", StringType(), nullable=True),
    StructField("balancesheet_id", IntegerType(), nullable=True),
    StructField("companyName", StringType(), nullable=True),
    StructField("companyId", LongType(), nullable=True),        
    StructField("afm", StringType(), nullable=True),
    StructField("hasRelations", BooleanType(), nullable=True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lists and basic functions

# COMMAND ----------

def get_date_val(date_string):
    
    dateformat_strings = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]

    # Try parsing the date string with each format string
    for format_string in dateformat_strings:
        try:
            date_val = datetime.strptime(date_string, format_string)
            # print("Date parsed successfully using format:", format_string)
            break  # Exit the loop if date is parsed successfully
        except ValueError:
            # If parsing fails, try the next format string
            pass

    # If date_val is defined, it contains the parsed datetime object
    if date_val:
        return(date_val)
    else:
        return(None)


def get_company_info(company_data):

    # Extract company info
    company_info = {
        "gemiNumber": company_data['company']['id'],
        "companyName": company_data['company']['name'],
        "afm": company_data['company']['afm'],
        "addressCity": company_data['company']['company_city'],
        "isSuspended": company_data['company']['issuspended'],
        "branchParentOrigin": company_data['company'].get('branchParentOrigin'),
        "titles": company_data['company'].get('titles'),
        "namei18n": company_data['company'].get('namei18n'),
        "companyStatus": company_data['company'].get('companyStatus'),
        "legalType": company_data['company'].get('legalType'),
        "branchType": company_data['company'].get('branchType'),
        "dateGemiRegistered": company_data['company'].get('dateGemiRegistered'),
        "company_municipality": company_data['company'].get('company_municipality'),
        "company_zip_code": company_data['company'].get('company_zip_code'),
        "companyWebsite": company_data['company'].get('companyWebsite'),
        "companyEshop": company_data['company'].get('companyEshop'),
        "companyContactNumber": company_data['company'].get('companyContactNumber'),
        "ChamberResponsibleForCompany": company_data['company'].get('ChamberResponsibleForCompany'),
        "branchData": company_data['company'].get('branchData'),
        "telephone": company_data['moreInfo']['telephone'],  # Extract telephone from moreInfo
        "email": company_data['moreInfo']['email']  # Extract email from moreInfo
    }
    company_info_data.append(company_info)

    return 

def get_capital_info(company_data):

    # Extract capital info
    for capital in company_data.get('capital', []):
        capital_info = {
            "gemiNumber": company_data['company']['id'],
            "descr": capital['descr'],
            "amount": capital['amount'],
            "nominal_price": capital['nominal_price'],
            "capital_stock": capital['capital_stock'],
            "company_id": capital['company_id'],
            "corporate_status_id": capital['corporate_status_id'],
            "currency": capital['currency']
        }
        capital_data.append(capital_info)

    return

def get_persons(company_data):
    # Extract management persons info
    for person in company_data.get('managementPersons', []):
        person_info = {
            "gemiNumber": company_data['company']['id'],
            "firstName": person['firstName'],
            "lastName": person['lastName'],
            "active": person['active'],
            "afm": person['afm'],
            "dateFrom": person['dateFrom'],
            "dateElection": person['dateElection'],
            "dateTo": person['dateTo'],
            "percentage": person['percentage'],
            "capacityDescr": person['capacityDescr'],
            "shared": person['shared'],
            "nonShared": person['nonShared'],
            "tableName": person['tableName']
        }
        management_persons_data.append(person_info)

    return

def get_decisions(company_data):
    # Extract decisions info
    for decision in company_data.get('decisions', []):
        decision_info = {
            "gemiNumber": company_data['company']['id'],
            "body_date_of_assembly": decision['body_date_of_assembly'],
            "body_type": decision['body_type'],
            "descr": decision['descr'],
            "publish_date": decision['publish_date'],
            "deposit_date": decision['deposit_date'],
            "orderDate": decision['orderDate'],
            "decision_id": decision['id'],
            "system_file": decision['system_file'],
            "prfile_is_public": decision['prfile_is_public']
        }
        decisions_data.append(decision_info)

    return

def get_history_info(company_data):
    # Extract modification history info
    for history in company_data.get('ModificationHistoryData', []):
        history_info = {
            "gemiNumber": company_data['company']['id'],
            "announcement_date": history['announcement_date'],
            "registration_date": history['registration_date'],
            "orderDate": history['orderDate'],
            "modification_id": history['id'],
            "system_file": history['system_file'],
            "descr": history['descr']
        }
        modification_history_data.append(history_info)

    return

def get_kad_data(company_data):
    # Extract kadData
    for kad in company_data.get('kadData', []):
        kad_info = {
            "gemiNumber": company_data['company']['id'],
            "objective": kad['objective'],
            "activities": kad['activities'],
            "kad": kad['kad'],
            "descr": kad['descr']
        }
        kad_data.append(kad_info)

    return

def get_representatives(company_data):
    # Extract representation info
    for rep in company_data.get('representation', []):
        representation_info = {
            "gemiNumber": company_data['company']['id'],
            "name": rep['name'],
            "afm": rep['afm'],
            "active": rep['active'],
            "activeFrom": rep['activeFrom'],
            "activeTo": rep['activeTo']
        }
        representation_data.append(representation_info)

    return

def get_branches(company_data):
    # Extract branch info
    for branch in company_data.get('branch', []):
        branch_info = {
            "gemiNumber": branch['gemi_number'],
            "coName": branch['co_name'],
            "dateIncorporated": branch['date_incorporated'],
            "address": branch['address']
        }
        branch_data.append(branch_info)         

    return

def get_documents(company_data):
    # Extract documents info
    for document in company_data.get('documents', []):
        document_info = {
            "gemiNumber": company_data['company']['id'],
            "title": document['title'],
            "descr": document['descr'],
            "cost": document['cost']
        }
        documents_data.append(document_info)        

    return


def get_financial_data(company_data):
    # Extract financial data info
    for financial_record in company_data.get("companyFinancial", []):


        reference_period = financial_record.get("referencePeriod")
        files_and_auditors = financial_record.get("FilesAndAuditors", [])[0]  # Assuming there's only one element in FilesAndAuditors list                
        
        company_financial_info = {
            "gemiNumber": company_data['company']['id'],
            "referencePeriod": reference_period
        }

        company_financial_data.append(company_financial_info)

        # Extract balancesheet data
        for balancesheet_record in files_and_auditors.get("balancesheet", []):

            date_val = None
            if balancesheet_record.get("bal_date"): 
                date_val = get_date_val(balancesheet_record.get("bal_date"))
                if date_val:
                    balancesheet_info = {
                        "gemiNumber": company_data['company']['id'],
                        "referencePeriod": reference_period,
                        "id": balancesheet_record.get("id"),
                        "balancesheet_id": balancesheet_record.get("balancesheet_id"),
                        "bal_date": date_val,
                        "bal_file_system_file_path": balancesheet_record.get("bal_file_system_file_path")
                    }
                    balancesheet_data.append(balancesheet_info)
                else:
                    continue

        # Extract auditors data
        for auditor_record in files_and_auditors.get("auditors", []):
            auditor_info = {
                "gemiNumber": company_data['company']['id'],
                "referencePeriod": reference_period,
                "auditorName": auditor_record.get("auditorName"),
                "companyId": auditor_record.get("companyId"),
                "companyName": auditor_record.get("companyName"),
                "afm": auditor_record.get("afm"),
                "hasRelations": auditor_record.get("hasRelations")
            }

            # Append extracted data to respective lists
            
            auditors_data.append(auditor_info)        
            
    return

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main function to process a Gemi file

# COMMAND ----------


# Define function to process VAT numbers
def process_gemi_number(file_path):

    # Open and read the file
    with open(file_path, 'r') as file:
        company_response = file.read()
        try:
            company_data = json.loads(company_response)['companyInfo']['payload']
            get_company_info(company_data)
            get_capital_info(company_data)
            get_persons(company_data)
            get_decisions(company_data)
            get_history_info(company_data)
            get_kad_data(company_data)
            get_representatives(company_data)
            get_branches(company_data)
            get_documents(company_data)
            get_financial_data(company_data)
        except Exception as e:
            print(f"ERROR process_gemi_number file:{file_path} - {str(e)}")
            return

# COMMAND ----------

def print_dataframe_count(dataframe_name):
    print(f"""{dataframe_name}:{globals()[dataframe_name].count()}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXECUTION BLOCK

# COMMAND ----------

import csv
import sys
import requests
import json
import os
from datetime import datetime
import concurrent
import time
import random
import re
import shutil
import pandas as pd


# Get list of files in the source directory
files_to_parse = os.listdir(source_directory)

# Convert files_to_parse to a DataFrame
files_df = pd.DataFrame(files_to_parse, columns=['filename'])

#TODO
# Convert Spark DataFrame to Pandas DataFrame for filtering
already_registered_gemi_numbers_pd_df = registered_gemi_numbers_df.toPandas()

# Filter files_df to keep only the filenames which are NOT present in already_registered_gemi_numbers_pd_df
files_to_parse = files_df[~files_df['filename'].isin(already_registered_gemi_numbers_pd_df['filename'])]

# Mapping each DataFrame to its corresponding table name
table_mapping = {
    "company_info_df": "gemi_company_info",
    "capital_df": "gemi_capital",
    "management_persons_df": "gemi_management_persons",
    "decisions_df": "gemi_decisions",
    "modification_history_df": "gemi_modification_history",
    "kad_data_df": "gemi_kad_data",
    "representation_df": "gemi_representation",
    "documents_df": "gemi_documents",
    "company_financial_df": "gemi_company_financials",
    "balancesheet_df": "gemi_balancesheets",
    "auditors_df": "gemi_auditors"
}

# List of DataFrame names
dataframes_list = [
    "company_info_df",
    "capital_df",
    "management_persons_df",
    "decisions_df",
    "modification_history_df",
    "kad_data_df",
    "representation_df",
    "documents_df",
    "company_financial_df",
    "balancesheet_df",
    "auditors_df"
]

batch_count = 0
batch_dataframes_dict = {}

while not files_to_parse.empty:
    
    batch = files_to_parse[:batch_size]
    batch_count = batch_count + 1  
    print(f"batch {batch_count} starts:")
    files_to_parse = files_to_parse[batch_size:]

    # Initialize a dictionary to store dataframes for this batch
    dataframes_dict = {}

    # Initialize lists to store batch data    
    company_info_data = []
    print("company_info_data reset")

    capital_data = []
    management_persons_data = []    
    decisions_data = []
    modification_history_data = []
    branch_data = []
    representation_data = []
    kad_data = []
    documents_data = []
    company_financial_data = []
    auditors_data = []
    balancesheet_data = []
    gemi_vat_processed_data = []

    for filename in batch['filename']:            
        print(f'filename:{filename}')

        process_gemi_number(f'{source_directory}{filename}')

    #write batch to DB
    if not company_info_data:
        print("company_info_data is empty. Nothing to load")
    else:            
        #We have data. Store everything to the database
        dataframes_dict["company_info_df"] = spark.createDataFrame(company_info_data, schema=company_info_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["management_persons_df"] = spark.createDataFrame(management_persons_data, schema=management_persons_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["capital_df"] = spark.createDataFrame(capital_data, schema=capital_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["decisions_df"] = spark.createDataFrame(decisions_data, schema=decisions_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["modification_history_df"] = spark.createDataFrame(modification_history_data, schema=modification_history_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["kad_data_df"] = spark.createDataFrame(kad_data, schema=kad_data_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["representation_df"] = spark.createDataFrame(representation_data, schema=representation_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["documents_df"] = spark.createDataFrame(documents_data, schema=documents_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["company_financial_df"] = spark.createDataFrame(company_financial_data, schema=company_financial_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["balancesheet_df"] = spark.createDataFrame(balancesheet_data, schema=balancesheet_schema).withColumn("referencedate", lit(curr_date))
        dataframes_dict["auditors_df"] = spark.createDataFrame(auditors_data, schema=auditors_schema).withColumn("referencedate", lit(curr_date))

        print(f"save batch {batch_count} to db")

        # Store the dataframes dictionary in the batch_dataframes_dict using the batch number as the key
        batch_dataframes_dict[batch_count] = dataframes_dict

        for df_name, df in batch_dataframes_dict[batch_count].items():        
            if not df.isEmpty():
                table_name = table_mapping[df_name]
                df.write.format("delta").mode("append").saveAsTable(f"gemi.default.{table_name}")
                print(f"{df_name} persisted for batch {batch_count}")
            else:
                print(f"{df_name} is empty or not found for batch {batch_count}")
                                
        # Remove the processed dataframe from the dictionary and from memory
        del batch_dataframes_dict[batch_count]
        # del dataframes_dict

        '''
        # Save DataFrames to Databricks Unity Catalog only if they contain data
        # Loop through the DataFrame names
        for data_df in batch_dataframes_dict[batch_count]:
            
            # Dynamically get the DataFrame using globals()
            df = globals().get(df_name)
            if df is not None and not df.isEmpty():
                table_name = table_mapping[df_name]
                df.write.format("delta").mode("append").saveAsTable(f"gemi.default.{table_name}")
                print(f"{df_name} persisted")                
                # Remove the DataFrame from memory
                del globals()[df_name]
                print(f"{df_name} removed from memory")
            else:
                print(f"{df_name} is empty or not found")
        '''
        
        print(f"batch {batch_count} db saved")  

    print(f"batch {batch_count} end")
        

