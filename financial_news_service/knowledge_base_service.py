import time
import psycopg2
from psycopg2.extras import DictCursor

# Import functions from our other service files
from .database import get_db_connection
from .news_service import scrape_news_for_ticker, generate_embedding

def get_security_id(cursor, ticker_symbol):
    """Fetches the security ID for a given ticker symbol."""
    cursor.execute("SELECT id FROM securities WHERE ticker = %s;", (ticker_symbol,))
    result = cursor.fetchone()
    return result['id'] if result else None

def update_knowledge_base(tickers):
    """
    Orchestrates the process of fetching news, generating embeddings,
    and storing them in the database for a list of tickers.
    """
    conn = get_db_connection()
    if not conn:
        print("❌ Could not connect to database. Aborting knowledge base update.")
        return

    try:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            for ticker in tickers:
                print(f"\n--- Processing news for {ticker} ---")
                
                security_id = get_security_id(cursor, ticker)
                if not security_id:
                    print(f"⚠️ Security ID for {ticker} not found. Skipping.")
                    continue

                # Step 1: Scrape news for the ticker
                articles = scrape_news_for_ticker(ticker)
                if not articles:
                    continue

                articles_inserted = 0
                for article in articles:
                    # Step 2: Generate an embedding for the article title
                    embedding = generate_embedding(article['title'])
                    if not embedding:
                        print(f"   -> Failed to generate embedding for: {article['title']}. Skipping article.")
                        continue
                    
                    # Step 3: Insert the article and its embedding into the database
                    # ON CONFLICT (url) DO NOTHING ensures we don't insert duplicate articles.
                    insert_query = """
                        INSERT INTO news_articles 
                            (security_id, title, url, published_at, embedding)
                        VALUES 
                            (%s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO NOTHING;
                    """
                    try:
                        cursor.execute(insert_query, (
                            security_id,
                            article['title'],
                            article['link'],
                            article['pubDate'],
                            embedding
                        ))
                        # cursor.rowcount will be 1 if a row was inserted, 0 if it was a conflict
                        articles_inserted += cursor.rowcount 
                    except Exception as e:
                        print(f"   -> ❌ DB Error inserting article '{article['title']}': {e}")
                        conn.rollback() # Rollback this specific transaction
                
                conn.commit() # Commit all successful inserts for this ticker
                print(f"✅ Successfully inserted {articles_inserted} new articles for {ticker}.")
                
                # Be a good citizen and don't spam the news source
                time.sleep(1)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ A critical error occurred: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    # For our MVP, we'll run this on the Nifty 50
    # For testing, let's just use the first 5 stocks
    nifty50_tickers = [
        'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'INFY.NS'
    ]
    
    print("🚀 Starting Knowledge Base update process...")
    update_knowledge_base(nifty50_tickers)
    print("\n🎉 Knowledge Base update process complete.")
