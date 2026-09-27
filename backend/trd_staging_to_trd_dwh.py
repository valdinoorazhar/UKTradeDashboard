import os
import re
from pathlib import Path

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

from datetime import datetime

MONTH_PATTERN = re.compile(r"^(\d{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$")

month_input = input("Enter a date (YYYY-MM-DD): ")

def input_month(month_input):
    try:
        # Parse the input date
        input_date = datetime.strptime(month_input, "%Y-%m-%d")
        # Format the date to YYYYMON
        formatted_month = input_date.strftime("%Y%b").upper()
        return formatted_month
    except ValueError:
        print("Invalid date format. Please enter a date in YYYY-MM-DD format.")
        return None
