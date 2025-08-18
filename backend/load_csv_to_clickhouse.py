from tzlocal import get_localzone
import clickhouse_connect
import requests
import json
import csv
import pandas as pd
from io import BytesIO

client = clickhouse_connect.get_client(host='localhost', port=18123, username='default', password='changeme')
'''query_result = client.query('SELECT * FROM STG_ONS.fact_commodity')
query_result_df = query_result.result_rows
print(query_result_df)'''


dump = []

#Get ONS Trade Data API
response_API = requests.get('https://api.beta.ons.gov.uk/v1/datasets/trade')
text_API = response_API.text
json_API = json.loads(text_API)

#Get the link for ONS Trade's latest version
get_ver_API = json_API["links"]["latest_version"]["href"]

month = 'Nov-22'
SITC_code = '77K'
direction = 'IM'

def get_obs(ver_API, month, SITC_code, direction):
    return str(ver_API) + "/observations?time=" + month + "&geography=K02000001&standardindustrialtradeclassification=" + SITC_code + "&countriesandterritories=*&direction=" + direction


response_OBS = requests.get(get_obs)
text_OBS = response_OBS.text
#print(obs)
json_OBS = json.loads(text_OBS)

label_data = json_OBS["observations"][0]["dimensions"]["CountriesAndTerritories"]["id"]
obs_data = json_OBS["observations"][0]["observation"]
print("In {}, UK traded {} with {} with values £{}".format(month, SITC_code, label_data, obs_data))
