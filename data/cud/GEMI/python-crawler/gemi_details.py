import csv
import sys
import requests
import json
import os
import logging
from datetime import datetime
import concurrent
from concurrent.futures import ThreadPoolExecutor 
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


# Concurrent requests setup
executor = ThreadPoolExecutor(max_workers=10)


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
        completed.append(gemi)

    except Exception as e:
        logging.error(f'Error {gemi}: {e}')


def process_row(row):
    gemi = row[3]
  
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
        failed.append(row)


if __name__ == '__main__':
    if "parse=all" in sys.argv:
        # Setup logging
        logging.basicConfig(filename='app.log', level=logging.INFO) 

        # Output directory
        output_dir = './data/outputdetails/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        failed = []  # List of failed rows    
        completed = [] # List of completed successfully
        with open('./app.log') as f:
            for line in f:
                if 'ok' in line:
                    gemi = line.split('ok ')[1]
                    # Remove newline 
                    gemi = re.sub(r'\n', '', gemi)

                    completed.append(gemi)
                else:
                    # ex: ERROR:root:Error 91976154001: 404 Client Error: Not Found for url: https://publicity.businessportal.gr/api/company/details
                    gemi = line.split('ERROR:root:Error ')[1]
                    gemi = gemi.split(':')[0]
                    failed.append(gemi)        

        # Main program loop
        with open('./data/gemi_numbers_all_distinct.csv') as f:
            reader = csv.reader(f)
            headers = next(reader)

            data = list(reader)
            # data.sort(key=lambda x: datetime.strptime(x[1], '%d/%m/%Y'), reverse=True)

            while data:

                batch = data[:5]
                data = data[5:]

                candidates = []
                for row in batch:
                    gemi = row[3]
                    if gemi not in completed:
                        candidates.append(row)
                
                if len(candidates) > 0:
                    futures = [executor.submit(process_row, row) for row in candidates]
                    concurrent.futures.wait(futures)
                    time.sleep(5)
                else:
                    # no new candidates in this batch, skip download
                    print("No new candidates in this batch")
                    continue
    elif "parse=failed" in sys.argv:     

        # Setup logging
        logging.basicConfig(filename='app_failed.log', level=logging.INFO) 

        # Output directory
        output_dir = './data/outputdetails/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        failed = []  # List of failed rows    
        completed = [] # List of completed successfully
        with open('./app.log') as f:
            for line in f:
                if 'ok' in line:
                    gemi = line.split('ok ')[1]
                    # Remove newline 
                    gemi = re.sub(r'\n', '', gemi)

                    completed.append(gemi)
                else:
                    # ex: ERROR:root:Error 91976154001: 404 Client Error: Not Found for url: https://publicity.businessportal.gr/api/company/details
                    gemi = line.split('ERROR:root:Error ')[1]
                    gemi = gemi.split(':')[0]
                    failed.append(gemi)          

        # Setup logging
        logging.basicConfig(filename='app_failed_reprocessing.log', level=logging.INFO) 

        data = failed
        while data:

            batch = data[:5]
            data = data[5:]

            candidates = []
            for gemi in batch:
                if gemi not in completed:
                    candidates.append(gemi)
            
            if len(candidates) > 0:
                futures = [executor.submit(process_gemi, gemi) for gemi in candidates]
                concurrent.futures.wait(futures)
                time.sleep(5)