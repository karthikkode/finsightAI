import logging
import time
from tqdm import tqdm

from database import DatabaseManager
from .fetcher import NewsFetcher
from .embedder import EmbeddingGenerator
from . import config # Initializes logging

def run_news_update():
    """
    The main orchestration function for the financial news update process.
    This is the function you will schedule to run periodically.
    """
    logging.info("🚀 Starting financial news update process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        news_fetcher = NewsFetcher()
        embedder = EmbeddingGenerator()

        # 1. Get all tickers from our database
        tickers = db_manager.get_all_tickers()
        if not tickers:
            logging.warning("No tickers found in the database to update news for.")
            return

        logging.info(f"Found {len(tickers)} tickers to check for news updates.")
        
        total_articles_inserted = 0
        tickers_failed = 0

        # 2. Loop through each ticker, fetch news, generate embeddings, and store
        for ticker in tqdm(tickers, desc="Updating News"):
            try:
                articles = news_fetcher.fetch_news_for_ticker(ticker)
                if not articles:
                    continue

                articles_for_this_ticker = 0
                for article in articles:
                    # Generate an embedding for the article title
                    embedding = embedder.generate_embedding(article['title'])
                    if not embedding:
                        logging.warning(f"Skipping article due to embedding failure: {article['title']}")
                        continue
                    
                    # Add the embedding to the article dictionary
                    article['embedding'] = embedding
                    
                    # Insert the complete article data into the database
                    inserted = db_manager.upsert_news_article(ticker, article)
                    articles_for_this_ticker += 1 if inserted else 0
                
                if articles_for_this_ticker > 0:
                    logging.info(f"Inserted {articles_for_this_ticker} new articles for {ticker}.")
                    total_articles_inserted += articles_for_this_ticker
                
                time.sleep(1) # Be respectful to the news source

            except Exception as e:
                logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
                tickers_failed += 1
    
    except Exception as e:
        logging.critical(f"A critical error stopped the news update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Financial news update process finished.")
        logging.info(f"Summary: Inserted {total_articles_inserted} new articles. {tickers_failed} tickers failed.")


if __name__ == '__main__':
    run_news_update()
