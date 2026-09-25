import os
import re
from pathlib import Path

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
TABLE = "TRADE_STAGING.trade"
MONTH_PATTERN = re.compile(r"^(\d{4})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)$")

load_dotenv(ROOT_DIR / ".env")


def quote_string(value: object) -> str:
    """Return a ClickHouse string literal."""
    # Replace " with \" and ' with \'
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def read_latest_month(workbook: Path, sheet_name: str) -> tuple[str, pd.DataFrame]:
    frame = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=3)
    frame.columns = [str(column).strip().upper() for column in frame.columns]

    # Check for commodity, country, and direction columns
    required_columns = {"COMMODITY", "COUNTRY", "DIRECTION"}
    missing = required_columns - set(frame.columns)
    if missing:
        raise ValueError(f"{workbook.name} is missing columns: {sorted(missing)}")

    '''Look for month columns in the format YYYYMON (e.g., 2023JAN, 2023FEB),
    by matching the column names against the MONTH_PATTERN regex
    '''
    month_columns = [column for column in frame.columns if MONTH_PATTERN.fullmatch(column)]
    if not month_columns:
        raise ValueError(f"No YYYYMON month columns found in {workbook.name}")


    '''Read the latest month column'''
    latest_month = max(month_columns, key=lambda column: (int(column[:4]), column[4:]))
    result = frame[["COMMODITY", "COUNTRY", "DIRECTION", latest_month]].copy()
    result = result.rename(
        columns={
            "COMMODITY": "Commodity",
            "COUNTRY": "Country",
            "DIRECTION": "Direction",
            latest_month: "trade_value",
        }
    )
    result["trade_value"] = pd.to_numeric(result["trade_value"], errors="coerce")
    result = result.dropna(subset=["Commodity", "Country", "Direction", "trade_value"])
    result = result.drop_duplicates(subset=["Commodity", "Country", "Direction"], keep="last")
    return latest_month, result


def update_values(client, month: str, values: pd.DataFrame, batch_size: int = 500) -> None:
    client.command(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS `{month}` Nullable(Decimal64(4))")

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


def main() -> None:
    client = clickhouse_connect.get_client(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "18123")),
        username=os.getenv("DB_USER", "default"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "TRADE_STAGING"),
    )

    for direction, sheet_name in (("import", "3. Monthly Imports"), ("export", "3. Monthly Exports")):
        workbook = DATA_DIR / f"trade_{direction}.xlsx"
        month, values = read_latest_month(workbook, sheet_name)
        update_values(client, month, values)
        print(f"Updated {len(values)} {direction} rows for {month}.")


if __name__ == "__main__":
    main()