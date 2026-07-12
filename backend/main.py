import ingest_csv

# Declare file path
file_path = '../data'

# Ingest Export dataset CSV
export_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports/current/countrybycommodityexports.xlsx"
export_file_name = 'trade_export.csv'
ingest_csv.download_csv(export_url, export_file_name, file_path)

# Ingest Import dataset CSV
import_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityimports/current/countrybycommodityimports.xlsx"
import_file_name = 'trade_import.csv'
ingest_csv.download_csv(import_url, import_file_name, file_path)