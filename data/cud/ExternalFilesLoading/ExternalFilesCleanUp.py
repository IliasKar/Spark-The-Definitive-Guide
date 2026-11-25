# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will delete files from all root folders in DWHSIDEAPPS that have been uploaded to Databricks and then move them to a "processed" folder within DWHSIDEAPPS.

# COMMAND ----------

# MAGIC %pip install smbprotocol

# COMMAND ----------

# Import the module
import sys
sys.path.append("../aws_utils")

import aws_utils
# Get Environment Prefix

env = aws_utils.get_env_prefix()

# Redefine the env variable for the dwhsideapps folder
match env:
    case 'd':
        env = 'Dev'
    case 'qa':
        env = 'QA'
    case 'p':
        env = 'Prod'

# COMMAND ----------

file_path = dbutils.widgets.get("file_path") # File path relative to root folder
incremental = dbutils.widgets.get("incremental") # True or False 
match incremental:
    case 'True':
        Mechanism = 'IncrementalMechanism'
    case 'False':
        Mechanism = 'SnapshotMechanism'

# COMMAND ----------

import json
import logging
import smbclient

log = logging.getLogger()
log.setLevel(logging.INFO)

smb_share_host = aws_utils.get_databricks_job_id("/cfn/smb/host")
smb_domain = aws_utils.get_databricks_job_id("/cfn/smb/domain")
smb_username = aws_utils.get_databricks_job_id("/cfn/smb/username")
smb_password = aws_utils.get_databricks_job_id("/cfn/smb/password")
smb_share_name = "Share"
smb_directory_path = f"\\\\{smb_share_host}\\{smb_share_name}\\out\\{env}\\{Mechanism}"

smbclient.ClientConfig(
    username=smb_username,
    password=smb_password,
    domain=smb_domain
)

def list_files_in_smb_directory(path):
    log.info("Listing files in SMB directory...")
    files = smbclient.listdir(path)
    log.info(files)

def delete_file_from_smb(path):
    log.info(f"Attempting to delete file: {path}")
    smbclient.remove(path)
    log.info(f"Successfully deleted file: {path}")

def lambda_handler(event, context):
    log.info(event)
    try:
        list_files_in_smb_directory(smb_directory_path)
        s3_key = event['Records'][0]['s3']['object']['key']
        file_name = s3_key.split('/')[-1]
        smb_file_path = f"{smb_directory_path}\\{file_name}"
        delete_file_from_smb(smb_file_path)

    except Exception as e:
        log.exception(e)
        raise e

# def move_folder_to_processed(source_path):
#     # Replace forward slashes with backslashes and construct the full path
#     source_path = source_path.replace('/','\\')
#     full_source_path = f"\\\\{smb_share_host}\\{smb_share_name}\\out\\{env}\\{Mechanism}\\{source_path}"
#     # Split the source path to locate Mechanism and folder to move
#     path_parts = full_source_path.split('\\')
#     mechanism_index = path_parts.index(Mechanism)  # Find index of 'IncrementalMechanism' or 'SnapshotMechanism'
#     folder_to_move = path_parts[mechanism_index + 1]  # Folder after IncrementalMechanism or SnapshotMechanism e.g. 'EDA' 
    
#     # Construct the source directory path (up to the folder to move)
#     source_dir = '\\'.join(path_parts[:mechanism_index + 2])  # e.g: ...\\IncrementalMechanism\\EDA
    
#     # Construct the destination path within the 'processed' directory
#     dest_path = f"\\\\{smb_share_host}\\{smb_share_name}\\out\\{env}\\processed\\{Mechanism}\\{folder_to_move}"
    
#     print(f"Moving folder '{folder_to_move}' from {source_dir} to {dest_path}")
    
#     # # Move the entire folder
#     # try:
#     #     smbclient.rename(source_dir, dest_path)
#     #     print(f"Folder moved from {source_dir} to {dest_path} successfully.")
#     # except Exception as e:
#     #     print(f"Error moving folder: {e}")

# COMMAND ----------

def delete_file(source_path):
    # Replace forward slashes with backslashes
    source_path = source_path.replace('/', '\\')
    # Construct the full source file path
    full_source_path = f"\\\\{smb_share_host}\\{smb_share_name}\\out\\{env}\\{Mechanism}\\{source_path}"
    log.info(f"Attempting to delete file: {full_source_path}")
    smbclient.remove(full_source_path)
    log.info(f"Successfully deleted file: {full_source_path}")

delete_file(file_path)
