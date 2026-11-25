# Databricks notebook source
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


# we have an issue with the broadcast values in cmd16
spark.conf.set("spark.jvm.class.allowlist", "spark.databricks.pyspark.enablePy4JSecurity")

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

# Run Gemi-schemas notebook
%run ./GEMI-schemas
%run ./GEMI-logger

# COMMAND ----------

export = dbutils.widgets.get("export")
targets_file = dbutils.widgets.get("targets_file")
batch_size = 100

# COMMAND ----------

def get_data_for_gemi_number(gemi_number):

    # List of Chrome user agents
    chrome_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
    ]

    # Select a random Chrome user agent
    random_agent = random.choice(chrome_agents)

    # Set the user agent in the headers
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': random_agent
    }

    company_url = f"https://publicity.businessportal.gr/api/company/details"
    company_payload = json.dumps({"query": {"arGEMI": gemi_number}, "token": None, "language": "el"})
    response = requests.request("POST", company_url, headers=headers, data=company_payload)
    
    log_data(f"get_data_for_gemi_number:{gemi_number}-response:{response}")
    return response


# Define function to process VAT numbers
def process_gemi_number(gemi_number):
    company_response = get_data_for_gemi_number(gemi_number)

    print(company_response)

    if company_response.status_code == 200:
        company_data = company_response.json()['companyInfo']['payload']

        log_data(f"Gemi:{gemi_number}. success")
        
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
    else:
        log_data(f"Gemi:{gemi_number}. Failed {company_response.status_code}")
        print(f"Failed to fetch data for gemiNumber: {gemi_number} - gemi flow")

def store_results():
    #TODO take the arrays loaded with data and store the results to the GEMI tables
    print(company_info_data)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get  the target gemi numbers

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import StructType, StructField, StringType
import concurrent.futures

# Define the schema for company_info DataFrame

# zip,recommendationDateFrom,recommendationDateTo,gemiNumber,afm

gemi_targets_schema = StructType([
    StructField("gemiNumber", StringType(), nullable=False),
])

# Load VAT numbers from your query
gemi_targets_query = f"""
SELECT targets.gemiNumber FROM read_files(
  "{targets_file}",
  format => 'csv',
  header => true) targets
  left outer join `d-cdh`.reference.gemi_company_info ci on ci.geminumber = targets.geminumber
  where isnull(ci.geminumber)
ORDER BY TO_DATE(targets.recommendationDateFrom, 'MM/dd/yyyy') DESC, targets.afm desc"""

print(vat_query)

gemi_targets_df = spark.sql(gemi_targets_query)

print(gemi_targets_df)

# COMMAND ----------

# Process VAT numbers in parallel using ThreadPoolExecutor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)  # Adjust max_workers as needed

offset = 0

while offset < gemi_targets_df.count():
    # Get next batch of VAT numbers
    batch_df = gemi_targets_df.select("gemiNumber").na.drop().withColumn("row_num", row_number().over(Window.orderBy("gemiNumber")))

    # Filter the batch DataFrame based on the offset and batch size
    batch_df = batch_df.filter((col("row_num") > offset) & (col("row_num") <= offset + batch_size)).drop("row_num")

    # Submit tasks for processing VAT numbers in parallel
    futures = []
    for gemi_number_row in batch_df.collect():
        gemi_number = gemi_number_row["VAT_Number"]
        print("gemi_number:", gemi_number)
        futures.append(executor.submit(process_gemi_number, gemi_number, None))

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)

    #before going to the next batch store the results to the database
    store_results


    # Update offset
    offset += batch_size

    # Write offset to a file (to be used for restoring the run)
    with open("/dbfs/tmp/offset.txt", "w") as f:
        f.write(str(offset))
        print("offset written:",offset)

# COMMAND ----------

import json 

notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
jobId = "0"

try: 
    #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
    jobId = notebook_info["tags"]["jobId"] 
except: 
    jobId = "0"

def log_data(message):
    # Generate log file name with run ID
    instance_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId()
    

    instance_id_str = instance_id if instance_id is not None else "000"
    log_file_name = f"gemi_log_{instance_id_str}.txt"
    log_file_path = f"dbfs:/FileStore/Gemi/logs/{log_file_name}"  # Note the "dbfs:" prefix for DBFS

    # Check if the file already exists, if not create it
    try:
        dbutils.fs.head(log_file_path)
    except Exception as e:
        if 'FileNotFoundException' in str(e):
            dbutils.fs.put(log_file_path, '', overwrite=False)  # Create file if it doesn't exist

    # Read existing content of the file
    existing_content = dbutils.fs.head(log_file_path)

    # Append new message to existing content
    updated_content = existing_content + f"[{log_file_name}]: {message}" + '\n'

    # Write updated content back to the file
    dbutils.fs.put(log_file_path, updated_content, overwrite=True)

# COMMAND ----------

if vat_input is not None:

    # check if vat input exists already in the table `d-cdh`.`reference`.gemi_company_info (column afm) and if it does exist and it is quite recent (businessdate column is less than 2 weeks old), then exit the notebook execution with an error "VAT already exists in the database and it is quite recent."
    existing_vat = spark.sql(f"SELECT afm FROM `gemi`.default.gemi_vat_processed WHERE afm = '{vat_input}' AND referencedate >= DATE_SUB(CURRENT_DATE(), {retention_d})").first()

    # If the VAT input already exists and is recent, exit the notebook execution with an error message
    if existing_vat is not None:
        log_data(f"VAT:{vat_input}. VAT is skipped")
        raise ValueError("VAT already exists in the database and it is quite recent. Nothing to do")

# COMMAND ----------

# List of Chrome user agents
chrome_agents = [
   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

# Select a random Chrome user agent
random_agent = random.choice(chrome_agents)


# COMMAND ----------

if clean_data == 'true':
    for table in tables:
        spark.sql("DROP TABLE  `gemi`.default.`{table}`".format(env_prefix=env_prefix, table=table))

# COMMAND ----------


if vat_input is not None:
  url = "https://publicity.businessportal.gr/api/search"

  payload = json.dumps({
    "dataToBeSent": {
      "inputField": f"{vat_input}",
      "city": None,
      "postcode": None,
      "legalType": [],
      "status": [],
      "suspension": [],
      "category": [],
      "specialCharacteristics": [],
      "employeeNumber": [],
      "armodiaGEMI": [],
      "kad": [],
      "recommendationDateFrom": None,
      "recommendationDateTo": None,
      "closingDateFrom": None,
      "closingDateTo": None,
      "alterationDateFrom": None,
      "alterationDateTo": None,
      "person": [],
      "personrecommendationDateFrom": None,
      "personrecommendationDateTo": None,
      "radioValue": "all",
      "places": []
    }
  })


  # Set the user agent in the headers
  headers = {
    'Content-Type': 'application/json',
    'User-Agent': random_agent
  }

  search_response = requests.request("POST", url, headers=headers, data=payload)
  print(search_response.text)

# COMMAND ----------



# COMMAND ----------

try:

    if vat_input is not None:
        
        gemi_vat_processed_data.append({"afm": vat_input})

        response_json = json.loads(search_response.text)
        companies = response_json['company']['hits']

        for company in companies:

            if company['afm'] == vat_input:
                gemi_number = company['gemiNumber']

                company_response = get_data_for_gemi_number(gemi_number)

                print(company_response)

                if company_response.status_code == 200:
                    company_data = company_response.json()['companyInfo']['payload']

                    log_data(f"Vat:{vat_input}. success")

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
                else:
                    log_data(f"VAT:{vat_input}. Failed {company_response.status_code}")
                    print(f"Failed to fetch data for gemiNumber: {gemi_number} - vat flow")
            else:
                print(f"The afm number doesn't match to: {vat_input}")

        #mark the vat number as processed
        gemi_vat_processed_df = spark.createDataFrame(gemi_vat_processed_data, schema=gemi_vat_processed_schema)

        gemi_vat_processed_df = gemi_vat_processed_df.withColumn("referencedate", lit(curr_date)).withColumn("comment", lit("processed"))
        gemi_vat_processed_df.write.format("delta").mode("append").saveAsTable(f"`gemi`.default.gemi_vat_processed")

    if not company_info_data:
        log_data(f"Vat:{vat_input}. No Company found in GEMI")
        print("company_info_data is empty. Nothing to load")
    else:
        #We have data. Store everything to the database
        company_info_df = spark.createDataFrame(company_info_data, schema=company_info_schema)
        capital_df = spark.createDataFrame(capital_data, schema=capital_schema)
        management_persons_df = spark.createDataFrame(management_persons_data, schema=management_persons_schema)
        decisions_df = spark.createDataFrame(decisions_data, schema=decisions_schema)
        modification_history_df = spark.createDataFrame(modification_history_data, schema=modification_history_schema)
        kad_data_df = spark.createDataFrame(kad_data, schema=kad_data_schema)
        representation_df = spark.createDataFrame(representation_data, schema=representation_schema)
        documents_df = spark.createDataFrame(documents_data, schema=documents_schema)
        company_financial_df = spark.createDataFrame(company_financial_data, schema=company_financial_schema)
        balancesheet_df = spark.createDataFrame(balancesheet_data, schema=balancesheet_schema)
        auditors_df = spark.createDataFrame(auditors_data, schema=auditors_schema)

        # Add referencedate column with current date to all DataFrames
        company_info_df = company_info_df.withColumn("referencedate", lit(curr_date))
        capital_df = capital_df.withColumn("referencedate", lit(curr_date))
        management_persons_df = management_persons_df.withColumn("referencedate", lit(curr_date))
        decisions_df = decisions_df.withColumn("referencedate", lit(curr_date))
        modification_history_df = modification_history_df.withColumn("referencedate", lit(curr_date))
        kad_data_df = kad_data_df.withColumn("referencedate", lit(curr_date))
        representation_df = representation_df.withColumn("referencedate", lit(curr_date))
        documents_df = documents_df.withColumn("referencedate", lit(curr_date))
        company_financial_df = company_financial_df.withColumn("referencedate", lit(curr_date))
        balancesheet_df = balancesheet_df.withColumn("referencedate", lit(curr_date))
        auditors_df = auditors_df.withColumn("referencedate", lit(curr_date))    

        if export == 'true':
            # Save DataFrames to Databricks Unity Catalog
            company_info_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_company_info`")
            capital_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_capital`")
            management_persons_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_management_persons`")
            decisions_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_decisions`")
            modification_history_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_modification_history`")
            kad_data_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_kad_data`")
            representation_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_representation`")
            documents_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_documents`")
            company_financial_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_company_financials`")
            balancesheet_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_balancesheets`")
            auditors_df.write.format("delta").mode("append").saveAsTable(f"`{env_prefix}-cdh`.`reference`.`gemi_auditors`") 

except json.JSONDecodeError as e:
    print("Error decoding JSON:", str(e))

# COMMAND ----------

# Define the tables and execute repair statements
'''
for table in tables:
    spark.sql("REPAIR TABLE `{env_prefix}-cdh`.`reference`.`{table}` SYNC METADATA".format(env_prefix=env_prefix, table=table))
'''

# COMMAND ----------

# %fs rm -r /FileStore/Gemi/logs
