# Databricks notebook source
# MAGIC %md
# MAGIC In this notebook we demonstrate the usage of the email_task notebook to understand exactly how to use it

# COMMAND ----------

import json

# prepare the json data for the email template
exceptions_found = [         
    {"line_number": 1, "exception_message 1": "Error: 5044"},
    {"line_number": 2, "exception_message 2": "Error: 5066"},
    {"line_number": 3, "exception_message 3": "Error: 5099"}     
] 

notification_params = {
    "email_subject": "test email subject",
    "body_template": "/Volumes/d-de-sandbox/tmp/tmp/report1.html",
    "recipients_to": "npapastefanakis+to1@cepal.gr,npapastefanakis+to2@cepal.gr",
    "recipients_cc": "npapastefanakis+cc1@cepal.gr,npapastefanakis+cc2@cepal.gr",
    "log_table": "",
    "body_data_json": json.dumps({
        "name": "Nikos Papastefanakis",
        "report": "Finance upload 4/11/2024",
        "report_data": exceptions_found
    })
}

# Print the notification_params to verify the format
print(json.dumps(notification_params, indent=4))



# COMMAND ----------

# Run the email_task notebook using dbutils
result = dbutils.notebook.run("../email_task", 60, notification_params)

# Print the result from the email_task notebook
print(result)
