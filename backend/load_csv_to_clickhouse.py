import clickhouse_connect
import requests
import json
import pandas as pd
from datetime import datetime

# Initialize ClickHouse client
client = clickhouse_connect.get_client(host='localhost', port=18123, username='default', password='changeme', database='STG_ONS')


#Get ONS Trade Data API
response_API = requests.get('https://api.beta.ons.gov.uk/v1/datasets/trade')
text_API = response_API.text
json_API = json.loads(text_API)

#Get the link for ONS Trade's latest version
get_ver_API = json_API["links"]["latest_version"]["href"]

def get_obs(ver_API, month, SITC_code, direction):
    return str(ver_API) + "/observations?time=" + month + "&geography=K02000001&standardindustrialtradeclassification=" + SITC_code + "&countriesandterritories=*&direction=" + direction

mo = 'Nov-22'
SITC = '77K'
dir = 'IM'

response_OBS = requests.get(get_obs(get_ver_API, mo, SITC, dir))
text_OBS = response_OBS.text
#print(obs)
json_OBS = json.loads(text_OBS)

data = []


for i in range(len(json_OBS["observations"])):
    row = []
    country_i = json_OBS["observations"][i]["dimensions"]["CountriesAndTerritories"]["id"]
    val_i = json_OBS["observations"][i]["observation"]
    row = [datetime.strptime(mo, '%b-%y'), country_i, dir, SITC, val_i]
    data.append(row)


# Create DataFrame
df = pd.DataFrame(data , columns=['trade_month', 'country_code', 'direction_code', 'sitc_code', 'trade_value'])

client.insert_df('STG_ONS.dim_intl_trade', df, column_names=['trade_month', 'country_code', 'direction_code', 'sitc_code', 'trade_value'])