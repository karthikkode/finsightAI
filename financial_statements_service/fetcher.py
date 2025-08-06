import yfinance as yf
import pandas as pd
import logging
from typing import Optional

class FinancialsFetcher:
    """
    Handles fetching of financial statements (Income Statement, Balance Sheet,
    Cash Flow) for a given stock ticker using the yfinance API.
    """

    def __init__(self, ticker_symbol: str):
        """
        Initializes the fetcher with a specific stock ticker.

        Args:
            ticker_symbol (str): The stock ticker (e.g., 'RELIANCE.NS').
        """
        self.ticker = yf.Ticker(ticker_symbol)
        logging.info(f"Initialized FinancialsFetcher for {ticker_symbol}")

    def get_quarterly_income_statement(self) -> Optional[pd.DataFrame]:
        """
        Fetches the quarterly income statement.
        Returns a pandas DataFrame or None if not available.
        """
        try:
            income_stmt = self.ticker.quarterly_income_stmt
            if income_stmt.empty:
                logging.warning(f"No quarterly income statement found for {self.ticker.ticker}.")
                return None
            return income_stmt
        except Exception as e:
            logging.error(f"Error fetching quarterly income statement for {self.ticker.ticker}: {e}")
            return None

    def get_quarterly_balance_sheet(self) -> Optional[pd.DataFrame]:
        """
        Fetches the quarterly balance sheet.
        Returns a pandas DataFrame or None if not available.
        """
        try:
            balance_sheet = self.ticker.quarterly_balance_sheet
            if balance_sheet.empty:
                logging.warning(f"No quarterly balance sheet found for {self.ticker.ticker}.")
                return None
            return balance_sheet
        except Exception as e:
            logging.error(f"Error fetching quarterly balance sheet for {self.ticker.ticker}: {e}")
            return None

    def get_quarterly_cash_flow(self) -> Optional[pd.DataFrame]:
        """
        Fetches the quarterly cash flow statement.
        Returns a pandas DataFrame or None if not available.
        """
        try:
            cash_flow = self.ticker.quarterly_cashflow
            if cash_flow.empty:
                logging.warning(f"No quarterly cash flow statement found for {self.ticker.ticker}.")
                return None
            return cash_flow
        except Exception as e:
            logging.error(f"Error fetching quarterly cash flow for {self.ticker.ticker}: {e}")
            return None

# --- Example of how to use this class for testing ---
if __name__ == '__main__':
    # Configure logging for standalone testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    sample_ticker = 'RELIANCE.NS'
    fetcher = FinancialsFetcher(sample_ticker)

    print("\n--- Fetching Quarterly Income Statement ---")
    income_statement = fetcher.get_quarterly_income_statement()
    if income_statement is not None:
        print(income_statement.head())

    print("\n--- Fetching Quarterly Balance Sheet ---")
    balance_sheet = fetcher.get_quarterly_balance_sheet()
    if balance_sheet is not None:
        print(balance_sheet.head())

    print("\n--- Fetching Quarterly Cash Flow ---")
    cash_flow = fetcher.get_quarterly_cash_flow()
    if cash_flow is not None:
        print(cash_flow.head())
