import requests
from bs4 import BeautifulSoup
import logging
import trafilatura
from googlesearch import search

def web_search(query: str, num_results: int = 5) -> list[dict]:
    """
    Performs a web search using the Google Search library and returns
    a list of URLs and titles.
    """
    logging.info(f"Performing web search for: '{query}'")
    try:
        # The googlesearch library handles making the request look legitimate
        search_results = search(query, num_results=num_results, stop=num_results, pause=2)
        
        results = []
        for url in search_results:
            # We don't get titles directly, so we'll fetch them
            try:
                response = requests.get(url, timeout=10)
                soup = BeautifulSoup(response.content, 'lxml')
                title = soup.title.string if soup.title else "No title found"
                results.append({"url": url, "title": title})
            except Exception:
                # If fetching a title fails, just append the URL
                results.append({"url": url, "title": "Title could not be fetched"})
        
        return results
    except Exception as e:
        logging.error(f"Web search failed for query '{query}': {e}")
        return []

def browse_website(url: str) -> str | None:
    """
    Visits a URL and extracts the main text content using trafilatura.
    """
    logging.info(f"Browsing website: {url}")
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            return trafilatura.extract(downloaded)
        return "Could not extract content from the page."
    except Exception as e:
        logging.error(f"Failed to browse website {url}: {e}")
        return f"Error browsing website: {e}"

# --- Example of how to use these tools ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("--- Testing Web Search ---")
    results = web_search("latest news for Reliance Industries")
    for res in results:
        print(res)
    
    if results:
        print("\n--- Testing Website Browsing ---")
        first_url = results[0]['url']
        content = browse_website(first_url)
        print(f"Content from {first_url}:\n{content[:500]}...")
