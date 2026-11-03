import clickhouse_connect
from clickhouse_connect import get_client
import requests
import json
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s') 


#Get ONS Trade Data API
response_API = requests.get('https://api.beta.ons.gov.uk/v1/datasets', params={'limit': 300})
text_API = response_API.text
json_API = json.loads(text_API)
print(json_API)

data = []

for i in range(len(json_API["items"])):
    row = []
    dataset_name = json_API["items"][i]["id"]
    latest_version = json_API["items"][i]["links"]["latest_version"]["href"]
    row = [dataset_name, latest_version]
    data.append(row)

# Create DataFrame
df = pd.DataFrame(data , columns=['dataset_name', 'latest_version'])
df.to_csv('debug_list_datasets.csv', index=False)