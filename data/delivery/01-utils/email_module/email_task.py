# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Email Notification System Documentation
# MAGIC
# MAGIC > ## Input Parameters
# MAGIC
# MAGIC - **email_subject:** (string) The subject of the email message.
# MAGIC
# MAGIC - **body_template:** (string) The file path to the HTML template used for the email body ```ex: /Volumes/p-de-sandbox/temp/tmp/report1.html```
# MAGIC
# MAGIC - **body_data_json:** (json) Data passed to the template for rendering. The format depends on the template. The input json must have Quotation marks (") instead of apostrofes (') otherwise the notebook will fail to parse the parameter.
# MAGIC
# MAGIC - **recipients_to:** (to) The primary recipients of the email (to).
# MAGIC
# MAGIC - **recipients_cc:** Additional recipients of the email (cc)
# MAGIC
# MAGIC - **log_table:** The target table to store the log stream. ```ex: `p-de-sandbox`.temp.email_log_data```
# MAGIC
# MAGIC
# MAGIC > ## Example Email Template
# MAGIC
# MAGIC Save your html template to a volume folder (for more info see documentation at https://jinja.palletsprojects.com/en/stable/templates/)
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC template path: `/Volumes/p-de-sandbox/temp/tmp/report1.html`:
# MAGIC
# MAGIC code:
# MAGIC ```
# MAGIC <!DOCTYPE html>
# MAGIC
# MAGIC <html>
# MAGIC
# MAGIC <head>
# MAGIC
# MAGIC <title>Email Template</title>
# MAGIC
# MAGIC </head>
# MAGIC
# MAGIC <body>
# MAGIC
# MAGIC <h1>Hello {{name}}!</h1>
# MAGIC
# MAGIC <p>We would like to inform you that the report {{report}} is processed.</p>
# MAGIC
# MAGIC <p>Data are:</p>
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC {% for entry in report_data %}
# MAGIC
# MAGIC <li>Line Number: {{ entry.line_number }}, Exception Message: {{ entry['exception_message ' ~ entry.line_number] }}</li>
# MAGIC
# MAGIC {% endfor %}
# MAGIC
# MAGIC </ul>
# MAGIC
# MAGIC </body>
# MAGIC
# MAGIC </html>
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC The data for the above template to render properly must be the following:
# MAGIC   
# MAGIC data:
# MAGIC ```markdown
# MAGIC {
# MAGIC 	"name": "Nikos Papastefanakis",     
# MAGIC 	"report": "Finance upload 4/11/2024",     
# MAGIC 	"report_data": [         
# MAGIC 		{"line_number": 1, "exception_message 1": "Error: 5044"},
# MAGIC 		{"line_number": 2, "exception_message 2": "Error: 5066"},
# MAGIC 		{"line_number": 3, "exception_message 3": "Error: 5099"}     
# MAGIC 	] 
# MAGIC }
# MAGIC ```
# MAGIC final html output is:
# MAGIC ```markdown
# MAGIC
# MAGIC <!DOCTYPE html>
# MAGIC
# MAGIC <html>
# MAGIC
# MAGIC <head>
# MAGIC
# MAGIC <title>Email Template</title>
# MAGIC
# MAGIC </head>
# MAGIC
# MAGIC <body>
# MAGIC
# MAGIC <h1>Hello Nikos Papastefanakis!</h1>
# MAGIC
# MAGIC <p>We would like to inform you that the report Finance upload 4/11/2024 is processed.</p>
# MAGIC
# MAGIC <p>Data are:</p>
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC <li>Line Number: 1, Exception Message: Error: 5044</li>
# MAGIC
# MAGIC <li>Line Number: 2, Exception Message: Error: 5066</li>
# MAGIC
# MAGIC <li>Line Number: 3, Exception Message: Error: 5099</li>
# MAGIC
# MAGIC </ul>
# MAGIC
# MAGIC </body>
# MAGIC
# MAGIC </html>
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Authentication cell

# COMMAND ----------

import boto3
from botocore.exceptions import NoCredentialsError
import traceback

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

# Function to get AWS credentials from Databricks secrets.
def get_aws_credentials(secrets_scope, key_name):
    try:
        aws_secret_key = dbutils.secrets.get(scope=secrets_scope, key=key_name)
        return aws_secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {str(e)}")
        return None
    
# Function to get AWS credentials from Databricks secrets.
def get_scope_key_names(secrets_scope):
    try:
        # Databricks secrets configuration.
        aws_keys = dbutils.secrets.list(scope=secrets_scope)
        key_names = [key.key for key in aws_keys] if aws_keys else []
        return key_names
    
    except Exception as e:
        print(f"Error retrieving AWS keys: {str(e)}")
        return None    

secrets_scope = "aws-databricks"
aws_keys = get_scope_key_names(secrets_scope)

aws_key = aws_keys[0] if aws_keys else None

# Get AWS secret key from Databricks secrets.
aws_secret_key = get_aws_credentials(secrets_scope,aws_key)

if mode == "shared":
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)

    S3_client = boto3.client('s3', region_name='eu-central-1',
                                aws_access_key_id=aws_key,
                                aws_secret_access_key=aws_secret_key)                           
else:
    # Connect to AWS using boto3.
    ssm_client = boto3.client('ssm', region_name='eu-central-1')

    S3_client = boto3.client('s3', region_name='eu-central-1')                           


# Function to connect to AWS and access SSM parameter.
def connect_to_aws_and_access_ssm(ssm_key):

    if aws_secret_key is None:
        print("AWS secret key not available.")
        return

    # Access SSM parameter /cfn/a360/reports_output_path.
    response = ssm_client.get_parameter(
        Name=ssm_key,
        WithDecryption=False
    )

    return response['Parameter']['Value']

# Get the Bucket prefix from SSM to be sure.
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')



# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve SMTP settings

# COMMAND ----------

secrets_scope = "email-smtp"

# Get AWS secret key from Databricks secrets.
SMTP_IP = dbutils.secrets.get(scope=secrets_scope, key="ip")
SMTP_PORT = dbutils.secrets.get(scope=secrets_scope, key="port")
EMAIL_ACCOUNT = dbutils.secrets.get(scope=secrets_scope, key="from")

# COMMAND ----------

# MAGIC %md
# MAGIC PREPARE THE EMAIL MESSAGE

# COMMAND ----------

import jinja2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from pyspark.sql import Row
from datetime import datetime

class TemplateHandler:
    def __init__(self, template_base_path):
        self.template_base_path = template_base_path
        self.template_load = jinja2.FileSystemLoader(template_base_path)

    # method that returns a jinja template
    def get_template(self, template_name):
        return jinja2.Environment(loader=self.template_load).get_template(template_name)

def log_exception(status, log_message):
    # List all widgets
    widget_names = ['recipients_to', 'recipients_cc', 'email_subject', 'body_template', 'body_data_json', 'log_table']

    # Initialize a dictionary to hold parameter values
    params = {}

    # Loop through each widget and retrieve its value
    for widget in widget_names:
        param_name = widget
        param_value = dbutils.widgets.get(param_name)
        params[param_name] = param_value

    execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_row = Row(message=params, status=status, log_message=log_message, execution_date=execution_date)
    log_df = spark.createDataFrame([log_row])
    log_table = dbutils.widgets.get("log_table")
    log_df.write.mode("append").insertInto(log_table)

# Create the email message
message = EmailMessage()


# Email parameters
recipients_to = dbutils.widgets.get("recipients_to")

# Check if the parameter exists
if not recipients_to:
    log_exception("Failed", "The parameter 'recipients_to' does not exist.")
    dbutils.notebook.exit("The parameter 'recipients_to' does not exist.")

to_email = recipients_to.split(',')

recipients_cc = dbutils.widgets.get("recipients_cc")
if not recipients_cc:
    cc_email = []
else:
    cc_email = recipients_cc.split(',')

subject = dbutils.widgets.get("email_subject")

# Check if the parameter exists
if not subject:
    log_exception("Failed", "The parameter 'email_subject' does not exist.")
    dbutils.notebook.exit("The parameter 'email_subject' does not exist.")

message['From'] = EMAIL_ACCOUNT
message['To'] = ', '.join(to_email)  # Join list into a comma-separated string
message['Cc'] = ', '.join(cc_email)  # Join list into a comma-separated string
message['Subject'] = subject

body_template = dbutils.widgets.get("body_template")

# Check if the parameter exists
if not body_template:
    log_exception("Failed", "The parameter 'body_template' does not exist.")
    dbutils.notebook.exit("The parameter 'body_template' does not exist.")

# Split the body_template into path and template name
template_base_path, template_name = body_template.rsplit('/', 1)

template_handler = TemplateHandler(template_base_path)

# Data to pass to the template
# Retrieve the notebook parameter
body_data_json = dbutils.widgets.get("body_data_json")
print(body_data_json)

# Check if the parameter exists
if not body_data_json:
    log_exception("Failed", "The parameter 'body_data_json' does not exist.")
    dbutils.notebook.exit("The parameter 'body_data_json' does not exist.")

# Parse the JSON string into a Python dictionary
import json
template_data = json.loads(body_data_json)

# Render the template with data
template = template_handler.get_template(template_name)
message_body = template.render(template_data)

# Attach the HTML message body
message.set_content(message_body, subtype='html')

# COMMAND ----------

# MAGIC %md
# MAGIC SEND THE EMAIL

# COMMAND ----------


# Send the email using SMTP
try:
    with smtplib.SMTP(SMTP_IP, SMTP_PORT) as server:  # Use your SMTP server details here
        server.starttls()  # Upgrade to a secure connection
        # server.login('xx', 'ccc')  # Uncomment and use if authentication is needed

        # Send the email
        server.send_message(message)        
    
    log_exception("Success", "Email sent successfully!")
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
    log_exception("Failed", f"Failed to send email: {e}")
