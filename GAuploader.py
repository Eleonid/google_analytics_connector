# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 16:31:30 2017

@author: leonid.enov
"""

# Import libs to interact with Google Bigquery and Google Analytics API, create and manage files
import argparse
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from google.cloud import bigquery
import json
import csv
from datetime import datetime, date, time, timedelta
import schedule
import re

# Create client, dataset and table objects
client = bigquery.Client(project='XXXX-XXXX')
dataset = client.dataset('DATASET_NAME')
table = dataset.table('TABLE_NAME')

# Get a service that communicates to a Google API.
def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email, key_file_location, scopes=scope)

    http = credentials.authorize(httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http)

    return service

# Init necessary dimensions and metrics
industry = 'XXXXXXX'
Client = 'CLIENT_NAME' # Don't name "client" variables in lower case! That's a reserved variable name, name it "Client"
site = 'SITE_NAME'

# Init industry, client and site info to add it to the API responce
id = 'ga:XXXXXXXX'
dimensions = 'ga:date,ga:source,ga:medium,ga:campaign,ga:adContent,ga:deviceCategory'
metrics = 'ga:sessions,ga:pageviews,ga:bounces,ga:sessionDuration,ga:goal1Completions,ga:goal2Completions,ga:goal3Completions,ga:goal4Completions,ga:goal5Completions,ga:transactions'

# Save path to scope, account email и API key location
scope = ['https://www.googleapis.com/auth/analytics.readonly']
service_account_email = 'XXXXXXXXXXXXXX.iam.gserviceaccount.com'
key_file_location = 'PATH_TO_KEY_FILE'

# Config service object
service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)

# Send API request to Google Analytics API and receive rows with results
def analyticsInvocation(date):
    resultArr = [['industry','client','site','date','utm_source','utm_medium','utm_campaign','utm_content','device_category','visits','pageviews','bounces','session_duration','goal_product_page_visit','goal_add_to_cart','goal_cart_visit','goal_checkout_visit','goal_order','transactions']]
    
    result = service.data().ga().get(ids=id, start_date=date, end_date=date, metrics=metrics, dimensions=dimensions, max_results='10000').execute()
    
    for i in range(0, len(result['rows'])):
        for k in range(1, 5):
            result['rows'][i][k] = str(result['rows'][i][k])

    for i in range(0, len(result['rows'])):
        result['rows'][i].insert(0,industry)
        result['rows'][i].insert(1,Client)
        result['rows'][i].insert(2,site)
    
    for i in range(0, len(result['rows'])):
        resultArr.append(result['rows'][i])

    return resultArr

# Download yesterday's data from Google Analytics
date_now = datetime.now()
date_actual = date_now + timedelta(days=-1)
date_actual_as_date = date_actual.strftime('%Y-%m-%d')

result = analyticsInvocation(date_actual_as_date)

# Write result to CSV file
with open("PATH_TO_FILE", "w", encoding="utf-8") as output:
    writer = csv.writer(output, lineterminator='\n', delimiter=',')
    writer.writerows(result)
    print(writer)
    
# Upload actual file to Google Bigquery

# Create load job configuration
job_config = bigquery.LoadJobConfig()
job_config.source_format = 'CSV'
job_config.skip_leading_rows = 1
job_config.write_disposition = 'WRITE_APPEND'
job_config.create_disposition = 'CREATE_IF_NEEDED'
job_config.autodetect = True

# Select actual file as csv_file and run an upload job
with open('PATH_TO_FILE', 'r+b') as csv_file:
    job = client.load_table_from_file(
        csv_file, table, job_config=job_config)  # API request
