import requests
import time
import json
import os
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

API_KEY = os.getenv('ALPACA_API_KEY')
API_SECRET = os.getenv('ALPACA_API_SECRET')
TS_HOST = os.getenv('TS_HOST')
TS_PORT = os.getenv('TS_PORT', 5432)
TS_NAME = os.getenv('TS_DATABASE')
TS_USER = os.getenv('TS_USER')
TS_PASSWORD = os.getenv('TS_PASSWORD')


HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": API_SECRET}



def get_stock_data(ticker, start, end):
    filename = f"{ticker}_bars.json"
    timeframe = "1Hour"
    limit = 10000
    adjustment = "split"

    base_url = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"

    all_bars = []
    page_token = None

    while True:
        params = {
            "start": start,
            "end": end,
            "timeframe": timeframe,
            "limit": limit,
            "adjustment": adjustment,
        }
        if page_token:
            params["page_token"] = page_token

        r = requests.get(base_url, headers=HEADERS, params=params)
        data = r.json()
        bars = data.get("bars", [])
        all_bars.extend(bars)

        page_token = data.get("next_page_token", None)
        if not page_token:
            break

        time.sleep(0.5)

    with open(filename, "w") as jsonfile:
        json.dump(all_bars, jsonfile, indent=2)

    print(f"Data saved to {filename}")

    return filename

def upload_to_db(filename, ticker):
    # Connect to the TimescaleDB
    conn = psycopg2.connect(
        host=TS_HOST,
        port=TS_PORT,
        dbname=TS_NAME,
        user=TS_USER,
        password=TS_PASSWORD
    )
    cursor = conn.cursor()

    # Load the JSON file
    with open(filename, 'r') as f:
        bars = json.load(f)

    # Prepare the data for insertion
    rows = [
        (
            bar["t"],  # trade_time
            bar["c"],  # close_price
            bar["h"],  # high_price
            bar["l"],  # low_price
            bar["n"],  # num_trades
            bar["o"],  # open_price
            bar["v"],  # volume
            bar["vw"],  # vwap
            ticker      # ticker
        )
        for bar in bars
    ]

    # SQL insert statement
    sql = """
    INSERT INTO stocks (trade_time, close_price, high_price, low_price, num_trades, open_price, volume, vwap, ticker)
    VALUES %s
    ON CONFLICT (trade_time, id) DO NOTHING;
    """

    # Use execute_values for batch insert
    execute_values(cursor, sql, rows)

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Uploaded {len(rows)} rows to the database.")

# Modify the main function to call the upload_to_db function
def main():
    ticker = "TSLA"
    start = "2024-01-01T00:00:00Z"
    end = "2025-01-01T00:00:00Z"

    filename = get_stock_data(ticker, start, end)
    upload_to_db(filename, ticker)


if __name__ == '__main__':
    main()


