import ingest_csv
from datetime import datetime

# Import dataset CSV
export_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports/current/countrybycommodityexports.xlsx"
ingest_csv.download_csv(export_url, "trade_export.csv", "../data")