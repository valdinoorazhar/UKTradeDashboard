import os
from pathlib import Path
from dotenv import load_dotenv
import ingest_xlsx
import add_latest_trade_value
import trd_staging_to_trd_dwh

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

# Ingest the latest trade values into the database
add_latest_trade_value.main()

# Transform and load data from staging to DWH
trd_staging_to_trd_dwh.main()