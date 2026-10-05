import os
import re
from pathlib import Path

import clickhouse_connect

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


from dotenv import load_dotenv

from datetime import datetime

MONTH_PATTERN = re.compile(r"^(\d{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$")

date_input = input("Enter a date (YYYY-MM-DD): ")

def input_month(month_input):
    try:
        # Parse the input date
        ori_date = datetime.strptime(month_input, "%Y-%m-%d")
        # Format the date to YYYYMON
        formatted_month = ori_date.strftime("%Y%b").upper()
        return formatted_month, ori_date
    except ValueError:
        logger.error("Invalid date format. Please enter a date in YYYY-MM-DD format.")
        return None

#formatted_month, ori_date = input_month(date_input)

def insert_to_dwh (target_table, source_table, formatted_month, ori_date):
    ROOT_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = ROOT_DIR / "data"
    TABLE = "TRADE_STAGING.trade"

    load_dotenv(ROOT_DIR / ".env")

    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

    query_delete = f"DELETE FROM {target_table} WHERE month = '{ori_date}'"

    query_insert = f"""
    INSERT INTO {target_table}  
    SELECT {ori_date} as trade_month
        , ctr.country_code as country_code
        , dir.direction_code as direction_code
        , comm.detailed_sitc_code as sitc_code
        , trd.{formatted_month} AS trade_value
    FROM {source_table} trd
    join TRADE_DWH.fact_country ctr
        ON splitByChar(' ',trd.Country) [1] = ctr.country_code
    join TRADE_DWH.fact_trd_direction dir
        ON splitByChar(' ',trd.Direction) [1] = dir.direction_code
    join TRADE_DWH.fact_commodity comm
        ON splitByChar(' ',trd.Commodity) [1] = comm.detailed_sitc_code
    """



    