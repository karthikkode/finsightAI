import logging
import time
from tqdm import tqdm
import pandas as pd
import numpy as np

# Use the shared DatabaseManager from the root directory
from database import DatabaseManager
from .fetcher import FinancialsFetcher
# This now correctly imports the new config file we just created
from . import config

def _clean_value(value):
    """
    A robust helper to handle potential NaN or None values and convert all
    numpy numeric types to standard Python int or float.
    """
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    if isinstance(value, (np.floating, np.float64)):
        return float(value)
    return value


def run_financials_update():
    """
    The main orchestration function for fetching and storing historical
    quarterly financial statements for all stocks in the database.
    """
    logging.info("🚀 Starting financial statements update process...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        
        # For testing, let's just process a few tickers first
        tickers = db_manager.get_all_tickers()
        if not tickers:
            logging.warning("No tickers found in the database to update financials for.")
            return

        logging.info(f"Found {len(tickers)} tickers to process for financial statements.")
        
        for ticker in tqdm(tickers, desc="Updating Financials"):
            try:
                fetcher = FinancialsFetcher(ticker)

                # --- 1. Process Income Statements ---
                income_stmt_df = fetcher.get_quarterly_income_statement()
                if income_stmt_df is not None:
                    for report_date, data in income_stmt_df.items():
                        statement_data = {
                            'report_date': report_date.date(),
                            'total_revenue': _clean_value(data.get('Total Revenue')),
                            'cost_of_revenue': _clean_value(data.get('Cost Of Revenue')),
                            'gross_profit': _clean_value(data.get('Gross Profit')),
                            'operating_income': _clean_value(data.get('Operating Income')),
                            'operating_expense': _clean_value(data.get('Operating Expense')),
                            'net_income': _clean_value(data.get('Net Income')),
                            'ebit': _clean_value(data.get('EBIT')),
                            'ebitda': _clean_value(data.get('EBITDA')),
                            'basic_eps': _clean_value(data.get('Basic EPS'))
                        }
                        db_manager.upsert_income_statement(ticker, statement_data)

                # --- 2. Process Balance Sheets ---
                balance_sheet_df = fetcher.get_quarterly_balance_sheet()
                if balance_sheet_df is not None:
                    for report_date, data in balance_sheet_df.items():
                        statement_data = {
                            'report_date': report_date.date(),
                            'total_assets': _clean_value(data.get('Total Assets')),
                            'current_assets': _clean_value(data.get('Current Assets')),
                            'total_liabilities': _clean_value(data.get('Total Liabilities')),
                            'current_liabilities': _clean_value(data.get('Current Liabilities')),
                            'total_debt': _clean_value(data.get('Total Debt')),
                            'net_debt': _clean_value(data.get('Net Debt')),
                            'stockholders_equity': _clean_value(data.get('Stockholders Equity'))
                        }
                        db_manager.upsert_balance_sheet(ticker, statement_data)

                # --- 3. Process Cash Flow Statements ---
                cash_flow_df = fetcher.get_quarterly_cash_flow()
                if cash_flow_df is not None:
                    for report_date, data in cash_flow_df.items():
                        statement_data = {
                            'report_date': report_date.date(),
                            'operating_cash_flow': _clean_value(data.get('Operating Cash Flow')),
                            'investing_cash_flow': _clean_value(data.get('Investing Cash Flow')),
                            'financing_cash_flow': _clean_value(data.get('Financing Cash Flow')),
                            'free_cash_flow': _clean_value(data.get('Free Cash Flow'))
                        }
                        db_manager.upsert_cash_flow(ticker, statement_data)
                
                time.sleep(1) # Be respectful to the API provider

            except Exception as e:
                logging.error(f"An unexpected error occurred for ticker {ticker}: {e}")
    
    except Exception as e:
        logging.critical(f"A critical error stopped the financials update process: {e}")
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Financial statements update process finished.")


if __name__ == '__main__':
    # The import of config at the top of the file handles logging initialization.
    run_financials_update()
