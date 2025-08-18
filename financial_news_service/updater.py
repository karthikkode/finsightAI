import logging
import time
from tqdm import tqdm
import concurrent.futures

from database import DatabaseManager
from .fetcher import NewsFetcher
from .embedder import EmbeddingGenerator
from . import config

def process_ticker_news(ticker_info, db_manager, news_fetcher, embedder):
    """
    Processes a single ticker: fetches news, generates embeddings, and stores.
    Designed to be run in a thread.
    """
    ticker = ticker_info['ticker']
    company_name = ticker_info['long_name']
    
    # Skip if there's no company name, as the fetcher needs it
    if not company_name:
        logging.warning(f"No company name for {ticker}, skipping news fetch.")
        return 0

    try:
        # --- FIX: Pass both the ticker and the company name to the fetcher ---
        articles = news_fetcher.fetch_news_for_ticker(ticker, company_name)
        if not articles:
            return 0

        articles_inserted = 0
        for article in articles:
            # Generate an embedding from the FULL article content
            embedding = embedder.generate_embedding(article['content'])
            if not embedding:
                logging.warning(f"Skipping article due to embedding failure: {article['title']}")
                continue
            
            article['embedding'] = embedding
            
            inserted = db_manager.upsert_news_article(ticker, article)
            if inserted:
                articles_inserted += 1
        
        if articles_inserted > 0:
            logging.info(f"Inserted {articles_inserted} new articles for {ticker}.")
        return articles_inserted

    except Exception as e:
        logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
        return 0

def run_news_update(tickers_to_process):
    """
    Main orchestration function for the financial news update process,
    using a thread pool for parallel processing.
    """
    logging.info("🚀 Starting financial news update process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        news_fetcher = NewsFetcher()
        embedder = EmbeddingGenerator()

        if not tickers_to_process:
            logging.warning("No tickers provided to update news for.")
            return

        logging.info(f"Starting to process {len(tickers_to_process)} tickers for news updates.")
        
        total_articles_inserted = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_ticker = {executor.submit(process_ticker_news, info, db_manager, news_fetcher, embedder): info['ticker'] for info in tickers_to_process}
            
            for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers_to_process), desc="Updating News"):
                try:
                    result = future.result()
                    total_articles_inserted += result
                except Exception as exc:
                    ticker = future_to_ticker[future]
                    logging.error(f'{ticker} generated an exception: {exc}')

    except Exception as e:
        logging.critical(f"A critical error stopped the news update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Financial news update process finished.")
        logging.info(f"Summary: Inserted a total of {total_articles_inserted} new articles.")

if __name__ == '__main__':
    # --- For Testing: Process only a single stock ---
    # We create a dictionary to match the format from db_manager.get_all_tickers()
    sample_tickers = [
        {'ticker': 'RELIANCE.NS', 'long_name': 'Reliance Industries Limited', 'id': 0} # id is a placeholder for this test
    ]
    run_news_update(sample_tickers)
