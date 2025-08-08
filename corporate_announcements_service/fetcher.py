import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Any
from urllib.parse import quote

# We'll need a simple config for this service
from . import config

class AnnouncementsFetcher:
    """
    Handles scraping of corporate announcements from Screener.in.
    """
    def fetch_announcements(self, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Scrapes the latest corporate announcements for a given stock ticker from Screener.in.
        """
        logging.info(f"Fetching announcements for {ticker_symbol} from Screener.in...")
        
        # Screener uses the ticker symbol without the '.NS' suffix in its URL
        base_ticker = ticker_symbol.replace('.NS', '')
        # URL encode the ticker to handle any special characters
        encoded_ticker = quote(base_ticker)
        
        url = f"https://www.screener.in/company/{encoded_ticker}/"
        
        try:
            # Use a timeout to prevent the script from hanging on a slow request
            response = requests.get(url, headers={'User-Agent': config.USER_AGENT}, timeout=20)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            
            # Find the "Announcements" heading and its corresponding table
            announcements_heading = soup.find('h2', class_='sub-heading', string=lambda t: 'Announcements' in t)
            if not announcements_heading:
                logging.warning(f"Announcements section not found for {ticker_symbol}.")
                return []
            
            table = announcements_heading.find_next_sibling('div', class_='responsive-holder').find('table')
            if not table:
                logging.warning(f"Announcements table not found for {ticker_symbol}.")
                return []

            announcements = []
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_str = cols[0].text.strip()
                    link_tag = cols[1].find('a')
                    
                    if not link_tag:
                        continue

                    title = link_tag.text.strip()
                    link = link_tag['href']
                    
                    # Handle cases where the date cannot be parsed
                    parsed_date = pd.to_datetime(date_str, format='%b %d, %Y', errors='coerce')
                    if pd.isna(parsed_date):
                        logging.warning(f"Could not parse date '{date_str}' for an announcement. Skipping.")
                        continue

                    announcement = {
                        'title': title,
                        'url': link, # Screener links are usually absolute URLs to exchange PDFs
                        'announcement_date': parsed_date,
                        'category': 'General' # Can be improved with NLP later
                    }
                    announcements.append(announcement)
            
            logging.info(f"Found {len(announcements)} announcements for {ticker_symbol}.")
            return announcements

        except Exception as e:
            logging.error(f"Error fetching announcements for {ticker_symbol}: {e}")
            return []
