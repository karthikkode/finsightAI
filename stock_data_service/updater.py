import logging
import time
from tqdm import tqdm
from datetime import date

from database import DatabaseManager
from .fetcher import StockDataFetcher
from . import config # Initializes logging

def run_daily_stock_update():
    """
    The main orchestration function for the daily stock data update process.
    This is the function you will schedule to run every day.
    It dynamically checks the last entry for each stock. If no data exists,
    it performs a full historical backfill. Otherwise, it fetches all
    data since the last known date.
    """
    logging.info("🚀 Starting daily stock price update process...")
    
    db_manager = None
    # Initialize summary variables before the try block to prevent UnboundLocalError
    total_records_inserted = 0
    tickers_failed = 0
    
    try:
        db_manager = DatabaseManager()
        fetcher = StockDataFetcher()

        # 1. Get all tickers that need to be updated from our database
        tickers = db_manager.get_all_tickers()
        if not tickers:
            logging.warning("No tickers found in the database to update.")
            return

        logging.info(f"Found {len(tickers)} tickers to check for updates.")
        
        # 2. Loop through each ticker, check for the last date, fetch, and update
        for ticker in tqdm(tickers, desc="Updating Stocks"):
            try:
                # Get the last date we have data for this stock
                latest_date_in_db = db_manager.get_latest_trade_date(ticker)

                new_price_records = []
                if latest_date_in_db is None:
                    # Case 1: No data exists for this stock. Perform a full historical backfill.
                    logging.info(f"No data found for {ticker}. Performing full historical backfill.")
                    new_price_records = fetcher.fetch_historical_data(ticker)
                else:
                    # Case 2: Data exists. Fetch all new data since that date.
                    new_price_records = fetcher.fetch_data_since(ticker, start_date=latest_date_in_db)
                
                if new_price_records:
                    # Insert each new day's data into the database
                    for price_data in new_price_records:
                        db_manager.upsert_daily_price(ticker, price_data)
                    
                    total_records_inserted += len(new_price_records)
                    logging.info(f"Inserted {len(new_price_records)} new records for {ticker}.")
                else:
                    logging.info(f"No new data to update for {ticker}.")
                
                # Be a good citizen and don't spam the API
                time.sleep(0.5) 

            except Exception as e:
                logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
                tickers_failed += 1
    
    except Exception as e:
        logging.critical(f"A critical error stopped the update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Daily stock price update process finished.")
        logging.info(f"Summary: Inserted {total_records_inserted} new records. {tickers_failed} tickers failed.")


if __name__ == '__main__':
    # This allows the script to be run directly for testing or via a scheduler.
    run_daily_stock_update()

