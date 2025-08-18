import logging
from tqdm import tqdm
import time
import concurrent.futures

from database import DatabaseManager
from .agent import NewsAgent
from financial_news_service.embedder import EmbeddingGenerator # Reuse our embedder
from . import config

def process_ticker_with_agent(ticker_info, db_manager, embedder):
    """
    Uses the AI News Agent to find news for a single ticker and stores it.
    """
    ticker = ticker_info['ticker']
    company_name = ticker_info['long_name']
    if not company_name:
        return 0

    try:
        agent = NewsAgent()
        articles = agent.run(company_name, ticker)
        
        articles_inserted = 0
        for article in articles:
            # Generate an embedding for the full content
            embedding = embedder.generate_embedding(article['content'])
            if not embedding:
                continue
            
            article_data = {
                'title': article['title'],
                'link': article['url'],
                'published_at': None, # The agent doesn't reliably get this yet
                'content': article['content'],
                'embedding': embedding
            }
            
            if db_manager.upsert_news_article(ticker, article_data):
                articles_inserted += 1
        
        if articles_inserted > 0:
            logging.info(f"Agent inserted {articles_inserted} new articles for {ticker}.")
        return articles_inserted

    except Exception as e:
        logging.error(f"Agent failed for ticker {ticker}: {e}")
        return 0

def run_agent_update():
    """
    Main orchestration function for the AI News Agent update process.
    """
    logging.info("🚀 Starting AI News Agent update process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        embedder = EmbeddingGenerator()

        tickers_info = db_manager.get_all_tickers()
        if not tickers_info:
            logging.warning("No tickers found to update news for.")
            return

        # For the MVP, let's run it on a few key stocks
        tickers_to_process = [t for t in tickers_info if t['ticker'] in ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS']]
        
        # NOTE: Running the agent is slow and complex. It's best to run it
        # sequentially for the MVP to observe its behavior clearly.
        # Multi-threading can be added later if needed.
        for info in tqdm(tickers_to_process, desc="Running News Agent"):
            process_ticker_with_agent(info, db_manager, embedder)
            time.sleep(2) # Small delay between tickers

    except Exception as e:
        logging.critical(f"A critical error stopped the agent update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 AI News Agent update process finished.")

if __name__ == '__main__':
    run_agent_update()
