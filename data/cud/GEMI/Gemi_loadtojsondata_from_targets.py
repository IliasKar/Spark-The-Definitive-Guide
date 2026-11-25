# Databricks notebook source
import csv
import sys
import requests
import json
import os
import logging
from datetime import datetime
import concurrent
from concurrent.futures import ThreadPoolExecutor 
import concurrent.futures 
import time
import random
import re

# User agents list
user_agents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
  'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0',
]

gemi_catalog = dbutils.widgets.get("catalog")
targets_csv = dbutils.widgets.get("targets")
output_dir = dbutils.widgets.get("output_dir")

working_dir = f"/Volumes/{gemi_catalog}/default/json_targets/"
target_dir = f"/Volumes/{gemi_catalog}/default/json_targets/"

# Concurrent requests setup
executor = ThreadPoolExecutor(max_workers=10)

# Define the list of futures
futures = []

def process_gemi(gemi):
    # Pick random user agent
    user_agent = random.choice(user_agents)

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': user_agent
        }

    payload = {
        'query': {'arGEMI': gemi},
        'token': None,
        'language': 'el' 
        }

    try:
        url = "https://publicity.businessportal.gr/api/company/details"
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        filename = f'{gemi}.json'
        with open(os.path.join(output_dir, filename), 'w') as f:
            json.dump(response.json(), f)

        logging.info(f'ok {gemi}')

    except Exception as e:
        logging.error(f'Error {gemi}: {e}')


logging.basicConfig(filename=f"{working_dir}app.log", level=logging.INFO)

# collect the targets

# step 1: collect all target gemi numbers from file f"{targets}"

# Read target gemi numbers from the targets file using PySpark
target_gemi_numbers_df = spark.read.option("header", "true").csv(targets_csv)

# Extract the 'gemiNumber' column from the DataFrame
target_gemi_numbers_df = target_gemi_numbers_df.select("gemiNumber")

if target_gemi_numbers_df.count() == 0:
    print("No target gemi numbers found. Exiting...")
else:
    # Step 2: Find gemi numbers which do not exist in gemi_company_info table

    non_existing_gemi_numbers_df = spark.sql(f"""SELECT DISTINCT t.geminumber 
                                             FROM read_files("{targets_csv}", format => 'csv', header => true) t 
                                             LEFT ANTI JOIN `{gemi_catalog}`.default.gemi_company_info gci 
                                             ON t.geminumber = gci.geminumber""")

    data = non_existing_gemi_numbers_df.select("gemiNumber").rdd.flatMap(lambda x: x).collect()
    
    while data:
        batch = data[:5]
        data = data[5:]
        for gemi in batch:
            future = executor.submit(process_gemi, gemi)
            futures.append(future)

        concurrent.futures.wait(futures)
        time.sleep(5)
