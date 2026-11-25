# Databricks notebook source
import boto3
from botocore.exceptions import NoCredentialsError
import traceback
import random
import requests

# COMMAND ----------

# Run Gemi-schemas notebook
%run ./GEMI-schemas

print(gemi_vat_processed_schema)

# COMMAND ----------

dbutils.widgets.text("run_date", '2024-08-27')

dbutils.widgets.text("VAT",'003728006')
dbutils.widgets.text("clean_data", 'false')
dbutils.widgets.text("export", 'true')
dbutils.widgets.text("retention_d", '100')

# COMMAND ----------


export = dbutils.widgets.get("export")
clean_data = dbutils.widgets.get("clean_data")
vat_input = dbutils.widgets.get("VAT")
retention_d = dbutils.widgets.get("retention_d")

# COMMAND ----------


"""
Accessing the Hive metastore requires specific considerations:

1. Hive metastore access is not possible via the instance profile if the cluster is on shared access mode.
   The cluster must be in shared access mode to interact with the Unity catalog.

2. To access both Unity catalog and AWS services, an access token from Amazon is required.

3. To retrieve the token secret key, access the secrets scope 'aws-databricks,' where the access key is stored per environment.
    https://docs.databricks.com/en/security/secrets/index.html

4. Once AWS credentials are obtained, clients for services like S3 and SSM can be created.

5. After you authenticate successfully querying the Hive metastore is also working.
"""

try:
    mode = dbutils.widgets.get("mode")
except Exception as e:
    mode = "shared" 

# Function to get AWS credentials from Databricks secrets
def get_aws_credentials(secrets_scope, key_name):
    try:
        # Databricks secrets configuration
    
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        # if aws_secret_key is not None:
        #     print("AWS secret key retrieved successfully.")
        # else:
        #     print("AWS secret key is None. Please check Databricks secrets configuration.")

        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None
    
# Function to get AWS credentials from Databricks secrets
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration

        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    print("mode is shared")
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)                           
else:
    # Connect to AWS using boto3
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')                           


# Function to connect to AWS and access SSM parameter
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Buckewt prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')

# COMMAND ----------

import json 

jobId = "0"

try: 
    #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
    jobId = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookId().get() 
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


# Initialize lists to store data
company_info_data = []
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


def get_data_for_gemi_number(gemi_number):
    company_url = f"https://publicity.businessportal.gr/api/company/details"
    company_payload = json.dumps({"query": {"arGEMI": gemi_number}, "token": None, "language": "el"})
    response = requests.request("POST", company_url, headers=headers, data=company_payload)
    
    log_data(f"get_data_for_gemi_number:{gemi_number}-response:{response}")
    return response


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

        print("a1:", financial_record.get("referencePeriod") )

        reference_period = financial_record.get("referencePeriod")
        files_and_auditors = financial_record.get("FilesAndAuditors", [])[0]  # Assuming there's only one element in FilesAndAuditors list                
        
        company_financial_info = {
            "gemiNumber": gemi_number,
            "referencePeriod": reference_period
        }

        company_financial_data.append(company_financial_info)

        # Extract balancesheet data
        for balancesheet_record in files_and_auditors.get("balancesheet", []):

            date_val = None
            if balancesheet_record.get("bal_date"): 
                date_val = datetime.strptime(balancesheet_record.get("bal_date"), "%Y-%m-%dT%H:%M:%S.%f")

            print("a11:",balancesheet_record.get("id"), date_val)

            balancesheet_info = {
                "gemiNumber": gemi_number,
                "referencePeriod": reference_period,
                "id": balancesheet_record.get("id"),
                "balancesheet_id": balancesheet_record.get("balancesheet_id"),
                "bal_date": date_val,
                "bal_file_system_file_path": balancesheet_record.get("bal_file_system_file_path")
            }
            balancesheet_data.append(balancesheet_info)


        # Extract auditors data
        for auditor_record in files_and_auditors.get("auditors", []):
            print("a12:",auditor_record.get("auditorName"),auditor_record.get("companyName"))
            auditor_info = {
                "gemiNumber": gemi_number,
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
