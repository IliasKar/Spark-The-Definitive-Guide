# Databricks notebook source
# MAGIC %md 
# MAGIC ## GEMI LOGGER

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
