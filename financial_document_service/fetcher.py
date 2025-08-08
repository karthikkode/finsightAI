import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Any
from urllib.parse import quote, urljoin
import os
import re
import pandas as pd
from datetime import datetime

# We'll create a simple config for this service
from . import config

class DocumentFetcher:
    """
    Handles scraping and downloading of financial documents like Annual Reports
    and Credit Ratings from Screener.in.
    """
    def __init__(self):
        """Initializes the fetcher and sets up the base download directory."""
        self.base_download_dir = config.DOWNLOAD_DIR
        os.makedirs(self.base_download_dir, exist_ok=True)
        logging.info(f"Base document download directory set to: {self.base_download_dir}")
        
        # --- FIX: Use the comprehensive, browser-like headers you provided ---
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        }

    def fetch_annual_reports(self, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Scrapes the links and metadata for Annual Reports for a given stock ticker.
        It then downloads the PDF to a dedicated subdirectory.
        """
        logging.info(f"Fetching annual reports for {ticker_symbol} from Screener.in...")
        
        base_ticker = ticker_symbol.replace('.NS', '')
        encoded_ticker = quote(base_ticker)
        screener_url = f"https://www.screener.in/company/{encoded_ticker}/"
        
        try:
            response = requests.get(screener_url, headers=self.headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            
            documents_section = soup.find('section', id='documents')
            if not documents_section:
                logging.warning(f"Main 'documents' section not found for {ticker_symbol}.")
                return []

            reports_div = documents_section.find('div', class_='annual-reports')
            if not reports_div:
                logging.warning(f"Annual reports sub-section not found for {ticker_symbol}.")
                return []
            
            link_list = reports_div.find('ul', class_='list-links')
            if not link_list:
                logging.warning(f"Annual reports link list not found for {ticker_symbol}.")
                return []

            documents = []
            links = link_list.find_all('a')
            
            for link_tag in links:
                report_url = urljoin(screener_url, link_tag.get('href', ''))
                
                if not report_url.lower().endswith('.pdf'):
                    logging.info(f"Skipping non-PDF link: {report_url}")
                    continue

                report_text = link_tag.text.strip()
                
                year_match = re.search(r'\b(20\d{2})\b', report_text)
                if not year_match:
                    logging.warning(f"Could not find a year in link text: '{report_text}'. Skipping.")
                    continue
                year_str = year_match.group(1)
                
                local_path = self._download_file(report_url, f"{base_ticker}_AR_{year_str}.pdf", "annual_reports", referer_url=screener_url)
                if not local_path:
                    continue

                document_info = {
                    'document_type': 'Annual Report',
                    'source_url': report_url,
                    'report_date': f"{year_str}-03-31", 
                    'local_path': local_path
                }
                documents.append(document_info)
            
            logging.info(f"Successfully downloaded {len(documents)} annual reports for {ticker_symbol}.")
            return documents

        except Exception as e:
            logging.error(f"Error fetching annual reports for {ticker_symbol}: {e}")
            return []

    def fetch_credit_ratings(self, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Scrapes the links and metadata for Credit Rating reports for a given stock ticker.
        """
        logging.info(f"Fetching credit ratings for {ticker_symbol} from Screener.in...")
        
        base_ticker = ticker_symbol.replace('.NS', '')
        encoded_ticker = quote(base_ticker)
        screener_url = f"https://www.screener.in/company/{encoded_ticker}/"
        
        try:
            response = requests.get(screener_url, headers=self.headers, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            
            documents_section = soup.find('section', id='documents')
            if not documents_section:
                return []

            ratings_div = documents_section.find('div', class_='credit-ratings')
            if not ratings_div:
                logging.warning(f"Credit ratings sub-section not found for {ticker_symbol}.")
                return []
            
            link_list = ratings_div.find('ul', class_='list-links')
            if not link_list:
                return []

            documents = []
            links = link_list.find_all('a')
            
            for link_tag in links:
                report_url = urljoin(screener_url, link_tag.get('href', ''))
                
                encoded_report_url = report_url.replace(' ', '%20')
                
                full_text = ' '.join(link_tag.text.split())
                
                match = re.search(r'(\d{1,2}\s\w{3}(\s\d{4})?)\sfrom\s(\w+)', full_text)
                if not match:
                    continue
                
                date_str = match.group(1)
                agency = match.group(3)
                
                # --- FIX: Handle dates with and without a year ---
                try:
                    # Try to parse with year first
                    parsed_date = pd.to_datetime(date_str)
                except ValueError:
                    # If it fails, add the current year and try again
                    current_year = datetime.now().year
                    date_str_with_year = f"{date_str} {current_year}"
                    parsed_date = pd.to_datetime(date_str_with_year, errors='coerce')

                logging.info(f"parsed_date: {parsed_date}")
                if pd.isna(parsed_date): continue
                
                # Determine the correct file extension based on the URL
                is_pdf = encoded_report_url.lower().endswith('.pdf')
                file_ext = ".pdf" if is_pdf else ".txt"
                filename = f"{base_ticker}_CR_{agency}_{parsed_date.strftime('%Y%m%d')}{file_ext}"
                
                # Use the dispatcher to handle either PDF or HTML
                local_path = self._process_document_url(encoded_report_url, filename, "credit_reports", referer_url=screener_url)
                if not local_path:
                    continue

                document_info = {
                    'document_type': 'Credit Rating',
                    'source_url': encoded_report_url,
                    'report_date': parsed_date.strftime('%Y-%m-%d'),
                    'agency': agency,
                    'local_path': local_path
                }
                documents.append(document_info)
            
            logging.info(f"Successfully processed {len(documents)} credit rating reports for {ticker_symbol}.")
            return documents

        except Exception as e:
            logging.error(f"Error fetching credit ratings for {ticker_symbol}: {e}")
            return []

    def _process_document_url(self, url: str, filename: str, subdirectory: str, referer_url: str) -> str | None:
        """
        Dispatcher function that checks if a URL is a PDF or HTML and calls the
        appropriate processing function.
        """
        if url.lower().endswith('.pdf'):
            return self._download_pdf(url, filename, subdirectory, referer_url)
        else:
            return self._extract_text_from_html(url, filename, subdirectory, referer_url)

    def _download_pdf(self, url: str, filename: str, subdirectory: str, referer_url: str) -> str | None:
        """Downloads a PDF from a URL to the local download directory."""
        target_dir = os.path.join(self.base_download_dir, subdirectory)
        os.makedirs(target_dir, exist_ok=True)
        local_filepath = os.path.join(target_dir, filename)
        
        try:
            logging.info(f"Downloading PDF from {url}...")
            download_headers = self.headers.copy()
            download_headers['Referer'] = referer_url
            
            with requests.get(url, stream=True, timeout=30, headers=download_headers) as r:
                r.raise_for_status()
                with open(local_filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info(f"Successfully saved PDF to {local_filepath}")
            return local_filepath
        except Exception as e:
            logging.error(f"Failed to download PDF from {url}: {e}")
            return None

    def _extract_text_from_html(self, url: str, filename: str, subdirectory: str, referer_url: str) -> str | None:
        """Fetches an HTML page, extracts its text, and saves it to a file."""
        target_dir = os.path.join(self.base_download_dir, subdirectory)
        os.makedirs(target_dir, exist_ok=True)
        local_filepath = os.path.join(target_dir, filename)

        try:
            logging.info(f"Extracting text from HTML page: {url}...")
            download_headers = self.headers.copy()
            download_headers['Referer'] = referer_url

            response = requests.get(url, timeout=30, headers=download_headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            text_content = soup.get_text(separator='\n', strip=True)
            
            with open(local_filepath, 'w', encoding='utf-8') as f:
                f.write(text_content)
            logging.info(f"Successfully extracted text and saved to {local_filepath}")
            return local_filepath
        except Exception as e:
            logging.error(f"Failed to extract text from {url}: {e}")
            return None

# --- Example of how to use this class for testing ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    class TestConfig:
        DOWNLOAD_DIR = "financial_reports"
        USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    
    config = TestConfig()

    sample_ticker = 'RELIANCE.NS'
    fetcher = DocumentFetcher()
    
    print("\n--- Fetching Credit Ratings ---")
    ratings = fetcher.fetch_credit_ratings(sample_ticker)
    if ratings:
        print("\n--- Processed Credit Ratings ---")
        for rating in ratings:
            print(rating)
