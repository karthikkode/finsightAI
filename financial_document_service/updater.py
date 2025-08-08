import logging
import time
from tqdm import tqdm

# Use the shared DatabaseManager from the root directory
from database import DatabaseManager
from .fetcher import DocumentFetcher
from . import config

def run_document_download():
    """
    Main orchestration function for downloading all available documents
    (Annual Reports, Credit Ratings, etc.) for all stocks.
    This is a long-running script designed for a full backfill.
    """
    logging.info("🚀 Starting financial documents download process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        fetcher = DocumentFetcher()
        
        tickers_info = db_manager.get_all_tickers()
        if not tickers_info:
            logging.warning("No tickers found to download documents for.")
            return

        logging.info(f"Found {len(tickers_info)} tickers to process.")
        
        for ticker_info in tqdm(tickers_info, desc="Downloading Documents"):
            ticker = ticker_info['ticker']
            try:
                # Fetch and download all document types for the current ticker
                logging.info(f"--- Processing {ticker} ---")
                # fetcher.fetch_annual_reports(ticker)
                fetcher.fetch_credit_ratings(ticker)
                # In the future, we can add fetch_concalls(ticker), etc. here
                
                # Add a reasonable delay to avoid getting blocked
                time.sleep(2)

            except Exception as e:
                logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
    
    except Exception as e:
        logging.critical(f"A critical error stopped the download process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Financial documents download process finished.")

if __name__ == '__main__':
    run_document_download()
