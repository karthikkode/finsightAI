import yfinance as yf
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import requests

# Correct relative import for files in the same package
from .database import get_db_connection

def clean_data_for_db(data_dict):
    """
    Converts numpy types to standard Python types and handles None values
    to prevent insertion errors.
    """
    cleaned_dict = {}
    for key, value in data_dict.items():
        if pd.isna(value) or value is None:
            cleaned_dict[key] = None
        elif isinstance(value, (np.integer, np.int64)):
            cleaned_dict[key] = int(value)
        elif isinstance(value, (np.floating, np.float64)):
            cleaned_dict[key] = float(value)
        else:
            cleaned_dict[key] = value
    return cleaned_dict

def get_nifty50_tickers():
    """
    Fetches the list of Nifty 50 tickers dynamically from the NSE India website.
    """
    print("Fetching latest Nifty 50 constituents...")
    try:
        # NSE website requires a browser-like user-agent to return data
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # This is the API endpoint NSE uses to populate its index dashboards
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        
        # Extract the ticker symbols and append '.NS' for yfinance compatibility
        tickers = [item['symbol'] + '.NS' for item in data['data']]
        
        print(f"✅ Successfully fetched {len(tickers)} tickers from Nifty 50.")
        return tickers
    except Exception as e:
        print(f"❌ Could not fetch Nifty 50 tickers dynamically: {e}")
        print("Falling back to a hardcoded list. This list may be outdated.")
        # Fallback list in case the API fails
        return [
            'ADANIENT.NS', 'ADANIPORTS.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
            'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BPCL.NS', 'BHARTIARTL.NS',
            'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
            'EICHERMOT.NS', 'GRASIM.NS', 'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
            'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 'ITC.NS',
            'INDUSINDBK.NS', 'INFY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LTIM.NS',
            'LT.NS', 'M&M.NS', 'MARUTI.NS', 'NTPC.NS', 'NESTLEIND.NS', 'ONGC.NS',
            'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 'SUNPHARMA.NS',
            'TCS.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS', 'TECHM.NS',
            'TITAN.NS', 'UPL.NS', 'ULTRACEMCO.NS', 'WIPRO.NS'
        ]

def process_stock_history(ticker_symbol):
    """
    Fetches the entire history for a stock and backfills the database
    with data that is always available from the yfinance history.
    """
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # 1. Fetch data from yfinance
            print(f"\nFetching data for {ticker_symbol}...")
            stock = yf.Ticker(ticker_symbol)
            info = stock.info
            
            # Fetch the ENTIRE history. auto_adjust=False gets raw OHLCV.
            hist = stock.history(period="max", auto_adjust=False)
            
            # Fetch history again with auto_adjust=True to get Adjusted Close
            adj_hist = stock.history(period="max", auto_adjust=True)
            hist['Adj Close'] = adj_hist['Close']

            if hist.empty:
                print(f"No historical data found for {ticker_symbol}. Skipping.")
                return
            
            print(f"Found {len(hist)} historical records for {ticker_symbol}.")

            # 2. Get the security ID, creating the record if it doesn't exist.
            print(f"Upserting '{info.get('longName', ticker_symbol)}' into securities table...")
            securities_sql = sql.SQL("""
                INSERT INTO securities (ticker, long_name, sector, industry, exchange, currency, business_summary, updated_at)
                VALUES (%(ticker)s, %(long_name)s, %(sector)s, %(industry)s, %(exchange)s, %(currency)s, %(summary)s, NOW())
                ON CONFLICT (ticker) DO UPDATE SET updated_at = NOW()
                RETURNING id;
            """)
            cursor.execute(securities_sql, clean_data_for_db({
                'ticker': ticker_symbol, 'long_name': info.get('longName'),
                'sector': info.get('sector'), 'industry': info.get('industry'),
                'exchange': info.get('exchange'), 'currency': info.get('currency'),
                'summary': info.get('longBusinessSummary')
            }))
            security_id = cursor.fetchone()['id']
            print(f"✅ Security '{info.get('longName', ticker_symbol)}' has ID: {security_id}")

            # 3. Get shares outstanding for historical market cap calculation
            shares_outstanding = info.get('sharesOutstanding')

            # 4. Loop through history and insert daily price data
            print("Backfilling historical data into 'daily_prices' table...")
            
            for trade_date, row in tqdm(hist.iterrows(), total=hist.shape[0]):
                # Calculate historical market cap
                historical_market_cap = row['Close'] * shares_outstanding if shares_outstanding else None

                data_to_insert = {
                    'security_id': security_id,
                    'trade_date': trade_date.date(),
                    'open': row['Open'],
                    'high': row['High'],
                    'low': row['Low'],
                    'close': row['Close'],
                    'adj_close': row['Adj Close'],
                    'volume': row['Volume'],
                    'market_cap': historical_market_cap, # Add market cap
                    'dividends': row['Dividends'],
                    'stock_splits': row['Stock Splits'],
                }

                cleaned_data = clean_data_for_db(data_to_insert)

                daily_prices_sql = sql.SQL("""
                    INSERT INTO daily_prices (
                        security_id, trade_date, open_price, high_price, low_price, 
                        close_price, adj_close_price, volume, market_cap, dividends, stock_splits
                    ) VALUES (
                        %(security_id)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, 
                        %(close)s, %(adj_close)s, %(volume)s, %(market_cap)s, %(dividends)s, %(stock_splits)s
                    )
                    ON CONFLICT (security_id, trade_date) DO NOTHING;
                """)
                cursor.execute(daily_prices_sql, cleaned_data)

            conn.commit()
            print("\n✅ Historical data backfill complete.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Error processing stock {ticker_symbol}: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    # Dynamically fetch the list of Nifty 50 tickers
    tickers_to_process = get_nifty50_tickers()

    if not tickers_to_process:
        print("❌ No tickers to process. Exiting.")
    else:
        print(f"Starting to process {len(tickers_to_process)} stocks...")
        for i, ticker in enumerate(tickers_to_process):
            print(f"\n--- Processing stock {i+1}/{len(tickers_to_process)}: {ticker} ---")
            try:
                process_stock_history(ticker)
                # Add a small delay to avoid getting rate-limited by the API provider
                time.sleep(1) 
            except Exception as e:
                print(f"❌❌❌ An unexpected error occurred while processing {ticker}: {e} ❌❌❌")
                print("Moving to the next stock.")
                continue # Continue to the next stock even if one fails

    print("\n🎉🎉🎉 All stocks processed! 🎉🎉🎉")
