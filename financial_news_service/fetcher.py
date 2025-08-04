import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Any

from . import config

class NewsFetcher:
    """
    Handles scraping and parsing of news articles from external sources.
    """
    def fetch_news_for_ticker(self, ticker_symbol: str) -> List[Dict[str, Any]]:
        """
        Scrapes recent news headlines and metadata for a stock from Google News RSS.
        """
        logging.info(f"Scraping news for {ticker_symbol}...")
        search_term = ticker_symbol.replace('.NS', '')
        url = f"https://news.google.com/rss/search?q={search_term}&hl=en-IN&gl=IN&ceid=IN:en"

        try:
            response = requests.get(url, headers={'User-Agent': config.USER_AGENT}, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml-xml')
            items = soup.find_all('item')
            
            articles = []
            # Process the top 5 articles for the MVP
            for item in items[:5]:
                article = {
                    'title': item.title.text,
                    'link': item.link.text,
                    'published_at': pd.to_datetime(item.pubDate.text, utc=True),
                    'source': item.source.text if item.source else 'Unknown'
                }
                articles.append(article)
            
            logging.info(f"Found {len(articles)} articles for {ticker_symbol}.")
            return articles

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching news for {ticker_symbol}: {e}")
            return []
