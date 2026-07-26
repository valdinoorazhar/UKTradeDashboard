import os
from pathlib import Path
from dotenv import load_dotenv
import ingest_xlsx

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Declare file path
file_path = '../data'

# Ingest Export dataset XLSX
export_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports/current/countrybycommodityexports.xlsx"
export_file_name = 'trade_export.xlsx'
ingest_xlsx.download_xlsx(export_url, export_file_name, file_path)

# Ingest Import dataset XLSX
import_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityimports/current/countrybycommodityimports.xlsx"
import_file_name = 'trade_import.xlsx'
ingest_xlsx.download_xlsx(import_url, import_file_name, file_path)

# Create DataFrame
#df = pd.DataFrame(data , columns=['trade_month', 'country_code', 'direction_code', 'sitc_code', 'trade_value'])

#client.insert_df('STG_ONS.dim_intl_trade', df, column_names=['trade_month', 'country_code', 'direction_code', 'sitc_code', 'trade_value'])