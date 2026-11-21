import os
import re
from pathlib import Path

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

import datetime

import sys

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
TABLE = "TRADE_STAGING.trade"


load_dotenv(ROOT_DIR / ".env")


def quote_string(value: object) -> str:
    """Return a ClickHouse string literal."""
    # Replace " with \" and ' with \'
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def read_latest_month(workbook: Path, sheet_name: str, month_input: str) -> tuple[str, pd.DataFrame]:
    frame = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=3)
    frame.columns = [str(column).strip().upper() for column in frame.columns]

    # Check for commodity, country, and direction columns
    required_columns = {"COMMODITY", "COUNTRY", "DIRECTION"}
    missing = required_columns - set(frame.columns)
    if missing:
        raise ValueError(f"{workbook.name} is missing columns: {sorted(missing)}")

    # Check whether the month_input is 'latest' or a specific date
    if month_input.lower() == 'latest':
        '''Look for month columns in the format YYYYMON (e.g., 2023JAN, 2023FEB),
            by matching the column names against the MONTH_PATTERN regex
        '''
        MONTH_PATTERN = re.compile(r"^(\d{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$")
        month_columns = [column for column in frame.columns if MONTH_PATTERN.fullmatch(column)]
        if not month_columns:
            raise ValueError(f"No YYYYMON month columns found in {workbook.name}")

        #The month is the latest month column from Excel File based on the datetime comparison
        month = max(month_columns, key=lambda column: (datetime.datetime.strptime(column, "%Y%b")))
        logger.info(f"Latest month found in {workbook.name}: {month}")
        
    else:
        # Validate the month_input format
        try:
            ori_date = datetime.datetime.strptime(month_input, "%Y-%m-%d")
            month = ori_date.strftime("%Y%b").upper()
        except ValueError:
            raise ValueError("Invalid date format. Please enter a date in YYYY-MM-DD format.")

        # Check if the month exists in the Excel file, if not raise error
        if month not in frame.columns:
            raise ValueError(f"Month {month} not found in {workbook.name}")


    
    result = frame[["COMMODITY", "COUNTRY", "DIRECTION", month]].copy()
    result = result.rename(
        columns={
            "COMMODITY": "Commodity",
            "COUNTRY": "Country",
            "DIRECTION": "Direction",
            month: "trade_value",
        }
    )
    result["trade_value"] = pd.to_numeric(result["trade_value"], errors="coerce")
    result = result.dropna(subset=["Commodity", "Country", "Direction", "trade_value"])
    result = result.drop_duplicates(subset=["Commodity", "Country", "Direction"], keep="last")
    return month, result


def update_values(client, month: str, values: pd.DataFrame, batch_size: int = 500) -> None:
    # Add the month column to the ClickHouse table if it doesn't exist
    client.command(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS `{month}` Nullable(Decimal64(4))")
    logger.info(f"Execute Alter Table for column {month}.")

    # Update the ClickHouse table in batches
    for start in range(0, len(values), batch_size):
        batch = values.iloc[start : start + batch_size]
        cases = []
        keys = []
        for row in batch.itertuples(index=False):
            commodity = quote_string(row.Commodity)
            country = quote_string(row.Country)
            direction = quote_string(row.Direction)
            value = "NULL" if pd.isna(row.trade_value) else str(float(row.trade_value))
            cases.append(
                f"(Commodity = {commodity} AND Country = {country} AND Direction = {direction}), {value}"
            )
            keys.append(f"({commodity}, {country}, {direction})")

        update_expression = "multiIf(" + ", ".join(cases) + ", NULL)"
        query = f"""
            ALTER TABLE {TABLE}
            UPDATE `{month}` = {update_expression}
            WHERE (Commodity, Country, Direction) IN ({', '.join(keys)})
        """
        client.command(query)
        logger.info(f"Updated batch {start // batch_size + 1} for month {month}.")


def main(month_input, workbook, sheet) -> None:
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "18123")),
        username=os.getenv("DB_USER", "default"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "TRADE_STAGING"),
    )

    workbook = DATA_DIR / workbook
    month, values = read_latest_month(workbook, sheet, month_input)
    update_values(client, month, values)
    logger.info(f"Updated {len(values)} rows in {TABLE} from {workbook} rows for {month}.")


if __name__ == "__main__":
    # Check if the user passed any arguments
    if len(sys.argv) > 1:
        # Pass all arguments except the script name itself
        main(sys.argv[1:])
    else:
        logger.warning("No arguments were provided!")