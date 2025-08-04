import requests
from bs4 import BeautifulSoup
import pandas as pd
import ollama

def generate_embedding(text, model_name="mxbai-embed-large"):
    """
    Generates a vector embedding for a given piece of text using a local Ollama model.
    
    Args:
        text (str): The text to embed.
        model_name (str): The name of the Ollama embedding model to use.
        
    Returns:
        A list of floats representing the vector embedding, or None if it fails.
    """
    try:
        # Generate the embedding using the specified Ollama model
        response = ollama.embeddings(model=model_name, prompt=text)
        return response['embedding']
    except Exception as e:
        print(f"❌ Error generating embedding: {e}")
        return None

def scrape_news_for_ticker(ticker_symbol):
    """
    Scrapes recent news headlines, links, and summaries for a given stock ticker
    from Google News.
    
    Args:
        ticker_symbol (str): The stock ticker (e.g., 'RELIANCE.NS').
    
    Returns:
        A list of dictionaries, where each dictionary represents a news article.
        Returns an empty list if scraping fails.
    """
    print(f"Scraping news for {ticker_symbol}...")
    
    # Remove the '.NS' suffix for a better Google News search query
    search_term = ticker_symbol.replace('.NS', '')
    
    # Construct the Google News RSS feed URL
    url = f"https://news.google.com/rss/search?q={search_term}&hl=en-IN&gl=IN&ceid=IN:en"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Use the 'lxml' parser for robustness and speed
        soup = BeautifulSoup(response.content, 'lxml-xml')
        items = soup.find_all('item')
        
        articles = []
        # We'll just take the top 5 for the MVP
        for item in items[:5]:
            article = {
                'title': item.title.text,
                'link': item.link.text,
                'pubDate': pd.to_datetime(item.pubDate.text),
                'source': item.source.text if item.source else 'N/A'
            }
            articles.append(article)
        
        print(f"✅ Found {len(articles)} articles for {ticker_symbol}.")
        return articles

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching news for {ticker_symbol}: {e}")
        return []

# --- Example of how to use this function ---
if __name__ == "__main__":
    # Let's test it with a sample ticker
    sample_ticker = 'RELIANCE.NS'
    news_list = scrape_news_for_ticker(sample_ticker)

    if news_list:
        print("\n--- Latest News ---")
        first_article_title = ""
        for article in news_list:
            if not first_article_title:
                first_article_title = article['title']
            print(f"Title: {article['title']}")
            print(f"Source: {article['source']}")
            print(f"Published: {article['pubDate']}")
            print(f"Link: {article['link']}\n")

        # --- Test the new embedding function ---
        print("\n--- Testing Embedding Generation ---")
        if first_article_title:
            print(f"Generating embedding for title: '{first_article_title}'")
            embedding = generate_embedding(first_article_title)
            if embedding:
                print(f"✅ Successfully generated a {len(embedding)}-dimensional vector.")
                print(f"   Preview of vector: {embedding[:5]}...")
