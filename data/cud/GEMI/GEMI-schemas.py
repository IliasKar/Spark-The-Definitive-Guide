# Databricks notebook source
# MAGIC %md 
# MAGIC ## setup initial values

# COMMAND ----------

# MAGIC %md ## SCHEMAS definition

# COMMAND ----------

tables = [
    "gemi_vat_processed",
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
# MAGIC ## Basic functions and data containers

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
