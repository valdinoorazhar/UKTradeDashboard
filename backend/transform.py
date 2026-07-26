import pandas as pd
from pathlib import Path

'''
def transform_data(file_path, month_parameter, sheet):
    """
    Transform the data from the XLSX file.

    Args:
        file_path: Path to the XLSX file
        month_parameter: The month for which to filter the data

    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    df = pd.read_excel(file_path, sheet_name = sheet, skiprows = 4)
    # Add your transformation logic here
    return df
'''


sheet = '3. Monthly Imports'
file_path = Path('../data/trade_import.xlsx')
df = pd.read_excel(file_path, sheet_name = sheet, skiprows = 3)
print(df.head(5))
