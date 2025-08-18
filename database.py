import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import DictCursor
from contextlib import contextmanager
import logging
from datetime import date
from typing import Dict, Any
import hashlib

import config

class DatabaseManager:
    """
    A production-ready class to manage PostgreSQL connections and operations.
    This is a shared service used by all other data services.
    It uses a connection pool for efficiency.
    """
    def __init__(self):
        """Initializes the connection pool."""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT
            )
            logging.info("Database connection pool created successfully.")
        except psycopg2.OperationalError as e:
            logging.critical(f"Could not create database connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Provides a database connection from the pool using a context manager.
        Ensures the connection is always returned to the pool.
        """
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        finally:
            if conn:
                self.pool.putconn(conn)

    def get_all_tickers(self) -> list[dict]:
        """
        Retrieves all stock tickers, their long names, and IDs from the securities table.
        """
        logging.info("Fetching all tickers from the database...")
        query = "SELECT id, ticker, long_name FROM securities ORDER BY ticker;"
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                tickers = [{'id': row['id'], 'ticker': row['ticker'], 'long_name': row['long_name']} for row in cursor.fetchall()]
                logging.info(f"Found {len(tickers)} tickers in the database.")
                return tickers

    def get_latest_trade_date(self, ticker_symbol: str) -> date | None:
        """
        Retrieves the most recent trade date for a given stock from the database.
        Returns None if no data exists for the ticker.
        """
        query = sql.SQL("""
            SELECT MAX(dp.trade_date)
            FROM daily_prices dp
            JOIN securities s ON s.id = dp.security_id
            WHERE s.ticker = %s;
        """)
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (ticker_symbol,))
                latest_date = cursor.fetchone()[0]
                return latest_date

    def upsert_daily_price(self, ticker: str, price_data: dict):
        """
        Inserts or updates a single day's price data for a given stock.
        """
        query = sql.SQL("""
            INSERT INTO daily_prices (
                security_id, trade_date, open_price, high_price, low_price, 
                close_price, adj_close_price, volume, market_cap, dividends, stock_splits
            )
            SELECT 
                s.id, %(trade_date)s, %(open)s, %(high)s, %(low)s, 
                %(close)s, %(adj_close)s, %(volume)s, %(market_cap)s, %(dividends)s, %(stock_splits)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (security_id, trade_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                adj_close_price = EXCLUDED.adj_close_price,
                volume = EXCLUDED.volume,
                market_cap = EXCLUDED.market_cap;
        """)
        
        price_data['ticker'] = ticker
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, price_data)
                conn.commit()
    
    def upsert_news_article(self, ticker: str, article_data: Dict[str, Any]) -> bool:
        """
        Inserts a news article with its full content and embedding into the database
        if its URL doesn't already exist.
        Returns True if a new row was inserted, False otherwise.
        """
        query = sql.SQL("""
            INSERT INTO news_articles (security_id, title, url, published_at, content, embedding)
            SELECT s.id, %(title)s, %(link)s, %(published_at)s, %(content)s, %(embedding)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (url) DO NOTHING;
        """)
        
        article_data['ticker'] = ticker

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, article_data)
                conn.commit()
                return cursor.rowcount > 0

    def upsert_income_statement(self, ticker: str, data: Dict[str, Any]):
        query = sql.SQL("""
            INSERT INTO income_statements_quarterly (security_id, report_date, total_revenue, cost_of_revenue, gross_profit, operating_income, operating_expense, net_income, ebit, ebitda, basic_eps)
            SELECT s.id, %(report_date)s, %(total_revenue)s, %(cost_of_revenue)s, %(gross_profit)s, %(operating_income)s, %(operating_expense)s, %(net_income)s, %(ebit)s, %(ebitda)s, %(basic_eps)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (security_id, report_date) DO NOTHING;
        """)
        data['ticker'] = ticker
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                conn.commit()

    def upsert_balance_sheet(self, ticker: str, data: Dict[str, Any]):
        query = sql.SQL("""
            INSERT INTO balance_sheets_quarterly (security_id, report_date, total_assets, current_assets, total_liabilities, current_liabilities, total_debt, net_debt, stockholders_equity)
            SELECT s.id, %(report_date)s, %(total_assets)s, %(current_assets)s, %(total_liabilities)s, %(current_liabilities)s, %(total_debt)s, %(net_debt)s, %(stockholders_equity)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (security_id, report_date) DO NOTHING;
        """)
        data['ticker'] = ticker
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                conn.commit()

    def upsert_cash_flow(self, ticker: str, data: Dict[str, Any]):
        query = sql.SQL("""
            INSERT INTO cash_flows_quarterly (security_id, report_date, operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow)
            SELECT s.id, %(report_date)s, %(operating_cash_flow)s, %(investing_cash_flow)s, %(financing_cash_flow)s, %(free_cash_flow)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (security_id, report_date) DO NOTHING;
        """)
        data['ticker'] = ticker
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                conn.commit()

    def upsert_corporate_announcement(self, ticker: str, data: Dict[str, Any]):
        """
        Inserts a corporate announcement into the database if its URL doesn't already exist.
        """
        query = sql.SQL("""
            INSERT INTO corporate_announcements (security_id, title, url, announcement_date, category)
            SELECT s.id, %(title)s, %(url)s, %(announcement_date)s, %(category)s
            FROM securities s WHERE s.ticker = %(ticker)s
            ON CONFLICT (url) DO NOTHING;
        """)
        data['ticker'] = ticker
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                conn.commit()
                
    def upsert_document_chunk(self, data: Dict[str, Any]):
        """
        Inserts a single document chunk into the database.
        Uses a hash of the chunk text to enforce uniqueness efficiently.
        """
        chunk_text = data['chunk_text']
        chunk_hash = hashlib.md5(chunk_text.encode('utf-8')).hexdigest()
        data['chunk_hash'] = chunk_hash

        query = sql.SQL("""
            INSERT INTO document_chunks (security_id, document_type, source_url, report_date, chunk_text, embedding, chunk_hash)
            VALUES (%(security_id)s, %(document_type)s, %(source_url)s, %(report_date)s, %(chunk_text)s, %(embedding)s, %(chunk_hash)s)
            ON CONFLICT (security_id, document_type, source_url, chunk_hash) DO NOTHING;
        """)
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                conn.commit()

    def delete_chunks_for_file(self, source_url: str):
        """
        Deletes all document chunks associated with a specific source file URL.
        This is used for cleanup when a file fails to process completely.
        """
        logging.warning(f"Deleting all existing chunks for failed file: {source_url}")
        query = sql.SQL("DELETE FROM document_chunks WHERE source_url = %s;")
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (source_url,))
                    conn.commit()
                    logging.info(f"Successfully deleted {cursor.rowcount} chunks for {source_url}.")
        except Exception as e:
            logging.error(f"Failed to delete chunks for {source_url}: {e}")
