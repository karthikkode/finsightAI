import logging
import time
from tqdm import tqdm

from database import DatabaseManager
from .fetcher import AnnouncementsFetcher
from . import config

def run_announcements_update():
    """
    Main orchestration function for fetching and storing corporate announcements.
    """
    logging.info("🚀 Starting corporate announcements update process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        fetcher = AnnouncementsFetcher()
        
        # Get tickers and their names for a more reliable search
        tickers_info = db_manager.get_all_tickers()
        if not tickers_info:
            logging.warning("No tickers found to update announcements for.")
            return

        logging.info(f"Found {len(tickers_info)} tickers to process.")
        
        for info in tqdm(tickers_info, desc="Updating Announcements"):
            ticker = info['ticker']
            
            try:
                # --- FIX: Pass the 'ticker' instead of the 'company_name' ---
                # The Screener.in fetcher is designed to work with the ticker symbol.
                announcements = fetcher.fetch_announcements(ticker)
                
                if announcements:
                    for announcement in announcements:
                        db_manager.upsert_corporate_announcement(ticker, announcement)
                
                time.sleep(1)

            except Exception as e:
                logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
    
    except Exception as e:
        logging.critical(f"A critical error stopped the announcements update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Corporate announcements update process finished.")

if __name__ == '__main__':
    run_announcements_update()
