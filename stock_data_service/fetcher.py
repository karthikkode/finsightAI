import yfinance as yf
import pandas as pd
import numpy as np
import logging
from datetime import date, timedelta
from typing import List, Dict, Any, Optional

class StockDataFetcher:
    """
    Handles all data fetching operations from the yfinance API.
    This class separates the logic for full historical backfills from
    daily incremental updates.
    """
    
    def fetch_historical_data(self, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Fetches the complete price history for a given stock ticker.
        This is intended for one-time backfilling of new securities.
        """
        logging.info(f"Performing full historical backfill for {ticker_symbol}...")
        try:
            stock = yf.Ticker(ticker_symbol)
            hist = stock.history(period="max", auto_adjust=False)
            
            if hist.empty:
                logging.warning(f"No historical data found for {ticker_symbol}.")
                return []

            # Process the fetched data
            return self._process_history_dataframe(stock, hist)
            
        except Exception as e:
            logging.error(f"Failed to fetch historical data for {ticker_symbol}: {e}")
            return []

    def fetch_data_since(self, ticker_symbol: str, start_date: date) -> List[Dict[str, Any]]:
        """
        Fetches all new price data for a stock since a specific start date.
        This is designed for daily "catch-up" jobs.
        """
        # yfinance's `start` is inclusive. We need data for the day *after* the last one we have.
        fetch_start_date = start_date + timedelta(days=1)
        logging.info(f"Fetching new data for {ticker_symbol} from {fetch_start_date.strftime('%Y-%m-%d')}...")
        
        try:
            stock = yf.Ticker(ticker_symbol)
            hist = stock.history(start=fetch_start_date.strftime('%Y-%m-%d'), auto_adjust=False)

            if hist.empty:
                logging.info(f"No new data found for {ticker_symbol} since {start_date}.")
                return []

            # Process the fetched data
            return self._process_history_dataframe(stock, hist)

        except Exception as e:
            logging.error(f"Failed to fetch new data for {ticker_symbol}: {e}")
            return []

    def _process_history_dataframe(self, stock_obj: yf.Ticker, hist_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        A private helper method to process a yfinance history dataframe.
        It adds adjusted close, calculates market cap, and cleans the data.
        """
        # Fetch adjusted close data separately for the same period
        adj_hist = stock_obj.history(start=hist_df.index.min().strftime('%Y-%m-%d'), end=hist_df.index.max().strftime('%Y-%m-%d'), auto_adjust=True)
        hist_df['Adj Close'] = adj_hist['Close']

        shares_outstanding = stock_obj.info.get('sharesOutstanding')
        
        processed_data = []
        for trade_date, row in hist_df.iterrows():
            market_cap = row['Close'] * shares_outstanding if shares_outstanding and shares_outstanding > 0 else None
            
            price_data = {
                'trade_date': trade_date.date(),
                'open': self._clean_value(row['Open']),
                'high': self._clean_value(row['High']),
                'low': self._clean_value(row['Low']),
                'close': self._clean_value(row['Close']),
                'adj_close': self._clean_value(row['Adj Close']),
                'volume': self._clean_value(row['Volume']),
                'market_cap': self._clean_value(market_cap),
                'dividends': self._clean_value(row['Dividends']),
                'stock_splits': self._clean_value(row['Stock Splits']),
            }
            processed_data.append(price_data)

        logging.info(f"Successfully processed {len(processed_data)} records for {stock_obj.ticker}.")
        return processed_data

    @staticmethod
    def _clean_value(value: Any) -> Optional[Any]:
        """Converts numpy types to standard Python types."""
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, (np.integer, np.int64)):
            return int(value)
        if isinstance(value, (np.floating, np.float64)):
            return float(value)
        return value

