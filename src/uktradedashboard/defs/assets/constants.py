from pathlib import Path

export_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityexports/current/countrybycommodityexports.xlsx"
export_file_name = 'trade_export.xlsx'

import_url = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/balanceofpayments/datasets/uktradecountrybycommodityimports/current/countrybycommodityimports.xlsx"
import_file_name = 'trade_import.xlsx'

# Anchored to this file (src/uktradedashboard/data) so it doesn't depend on the process's working directory
data_file_path = str(Path(__file__).resolve().parents[2] / 'data')