# news_fetcher.py
import logging
import re
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs, quote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
import trafilatura

try:
    import ollama  # optional
except Exception:
    ollama = None

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("NewsFetcher")

# ---------- HTTP Session with Retries ----------
from requests.adapters import HTTPAdapter, Retry
def _make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.5",
        "cache-control": "no-cache",
        "pragma": "no-cache",
    })
    return session

# ---------- Helpers ----------
def _parse_pubdate(pub: Optional[str]) -> Optional[pd.Timestamp]:
    if not pub:
        return None
    try:
        ts = pd.to_datetime(pub, utc=True, errors="coerce")
        return ts
    except Exception:
        return None

def _resolve_google_news_link(session: requests.Session, link: str) -> str:
    # Google News often uses outbound redirector links (news.google.com/rss/articles/…)
    try:
        # HEAD then follow redirect with GET if HEAD blocked
        r = session.head(link, allow_redirects=True, timeout=15)
        if r.status_code in (301, 302, 303, 307, 308):
            return r.headers.get("location", link)
        if r.url and "news.google" not in urlparse(r.url).netloc:
            return r.url
        # Some publishers reject HEAD; fallback to GET
        g = session.get(link, allow_redirects=True, timeout=20)
        if g.url and "news.google" not in urlparse(g.url).netloc:
            return g.url
        return link
    except Exception:
        return link

def _extract_main_text(html: str) -> Optional[str]:
    # First attempt: fast soup selection
    try:
        soup = BeautifulSoup(html, "lxml")
        # Prefer <article>, else common containers
        candidates = []
        art = soup.find("article")
        if art:
            candidates.append(art)
        candidates += soup.select("div[id*='content'],div[class*='content'],div[class*='article'],div[class*='story'],section[class*='content']")
        best = max(candidates, key=lambda c: len(c.get_text(" ", strip=True)), default=None)
        if best:
            text = " ".join(best.get_text(" ", strip=True).split())
            if len(text) > 500:
                return text
    except Exception:
        pass
    # Fallback: trafilatura
    try:
        txt = trafilatura.extract(html, include_comments=False, include_tables=False, no_fallback=True)
        if txt and len(txt) > 500:
            return " ".join(txt.split())
    except Exception:
        pass
    return None

# ---------- Core ----------
class NewsFetcher:
    """
    Hardened, LLM-optional financial news fetcher using Google News RSS discovery,
    publisher URL resolution, robust extraction, deduplication, and strict relevance filtering.
    """
    def __init__(self, use_llm: bool = False, reasoning_model: str = "llama3:70b"):
        self.session = _make_session()
        self.use_llm = use_llm and (ollama is not None)
        self.reasoning_model = reasoning_model if self.use_llm else None

    # ----- LLM-optional steps -----
    def _generate_search_terms(self, company_name: str, ticker_symbol: str) -> List[str]:
        base = [company_name, ticker_symbol]
        # Add naive expansions without LLM to avoid failures
        extra = []
        # Abbreviations from capitals (e.g., Reliance Industries -> RIL)
        parts = [p for p in re.split(r"\s+", company_name) if p and p[0].isalpha()]
        if len(parts) >= 2:
            abbr = "".join([p[0] for p in parts]).upper()
            if len(abbr) >= 2 and abbr not in base:
                extra.append(abbr)
        terms = list(dict.fromkeys([t for t in base + extra if t]))
        if not self.use_llm:
            return terms
        prompt = f"""
        Generate 3–4 optimal search terms to find financial news for the Indian company '{company_name}' (ticker: {ticker_symbol}).
        Include the full name, common abbreviation, and possibly the CEO.
        Return ONLY a JSON object where keys are the search terms. Example: {{"Reliance Industries": "", "RIL": "", "Mukesh Ambani": ""}}
        """
        try:
            resp = ollama.chat(model=self.reasoning_model, messages=[{"role": "user", "content": prompt}], format="json")
            data = json.loads(resp["message"]["content"])
            llm_terms = list(data.keys())
            out = list(dict.fromkeys(terms + llm_terms))
            log.info(f"Search terms: {out}")
            return out
        except Exception as e:
            log.warning(f"LLM term-gen failed: {e}")
            return terms

    def _is_article_relevant(self, title: str, content: str, company_name: str) -> bool:
        title_low = title.lower()
        comp_low = company_name.lower()
        # Fast-path: crude filter avoids LLM call
        if comp_low in title_low:
            return True
        if not self.use_llm:
            return comp_low in content.lower()
        prompt = f"""
        Determine if the article is specifically about the corporate entity '{company_name}'.
        Be strict: if it's mainly about a different company or a distinct subsidiary, answer NO.

        Title: {title}
        Content: {content[:1000]}

        Answer with only YES or NO.
        """
        try:
            resp = ollama.chat(model=self.reasoning_model, messages=[{"role": "user", "content": prompt}])
            ans = (resp["message"]["content"] or "").strip().upper()
            return ans == "YES"
        except Exception as e:
            log.warning(f"LLM relevance failed: {e}")
            return comp_low in content.lower()

    # ----- Discovery and extraction -----
    def _discover_from_google_news(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "lxml-xml")
            items = soup.find_all("item")[:limit]
            out = []
            for it in items:
                link = (it.link.text or "").strip()
                title = (it.title.text or "").strip()
                src = it.source.text.strip() if it.source else None
                pub = _parse_pubdate(it.pubDate.text.strip() if it.pubDate else None)
                if not link or not title:
                    continue
                resolved = _resolve_google_news_link(self.session, link)
                out.append({
                    "title": title,
                    "link": resolved,
                    "publisher": src,
                    "published_at": pub,
                })
            return out
        except Exception as e:
            log.error(f"Google News discovery failed for '{query}': {e}")
            return []

    def _fetch_html(self, url: str) -> Optional[str]:
        try:
            r = self.session.get(url, timeout=30)
            # Some sites serve AMP/Paywall or block HTML; try to follow meta-refresh
            if r.status_code == 200 and r.text:
                return r.text
            return None
        except Exception as e:
            log.debug(f"HTML fetch failed: {e}")
            return None

    def _scrape_full_text(self, url: str) -> Optional[str]:
        html = self._fetch_html(url)
        if not html:
            return None
        text = _extract_main_text(html)
        return text

    # ----- Public API -----
    def fetch_news_for_ticker(self, ticker_symbol: str, company_name: str, per_term: int = 8, final_max: int = 10) -> List[Dict[str, Any]]:
        terms = self._generate_search_terms(company_name, ticker_symbol)
        discovered: List[Dict[str, Any]] = []
        seen_links = set()

        for term in terms:
            batch = self._discover_from_google_news(term, limit=per_term)
            for item in batch:
                if item["link"] in seen_links:
                    continue
                seen_links.add(item["link"])
                discovered.append(item)
            time.sleep(0.5)  # be nice

        # Fetch and filter
        results: List[Dict[str, Any]] = []
        for art in discovered:
            content = self._scrape_full_text(art["link"])
            if not content or len(content) < 500:
                continue
            if not self._is_article_relevant(art["title"], content, company_name):
                continue
            results.append({
                "title": art["title"],
                "link": art["link"],
                "publisher": art.get("publisher"),
                "published_at": art.get("published_at"),
                "content": content
            })

        # Deduplicate by normalized title
        dedup: Dict[str, Dict[str, Any]] = {}
        for a in results:
            key = re.sub(r"\s+", " ", a["title"]).strip().lower()
            # keep most recent if duplicate titles
            existing = dedup.get(key)
            if existing is None:
                dedup[key] = a
            else:
                t_new = a["published_at"] or pd.Timestamp(0, tz="UTC")
                t_old = existing["published_at"] or pd.Timestamp(0, tz="UTC")
                if t_new > t_old:
                    dedup[key] = a

        out = list(dedup.values())
        # Sort newest first; default to very old if missing
        out.sort(key=lambda x: x["published_at"] or pd.Timestamp(0, tz="UTC"), reverse=True)
        return out[:final_max]

    def fetch_as_dataframe(self, ticker_symbol: str, company_name: str, **kwargs) -> pd.DataFrame:
        rows = self.fetch_news_for_ticker(ticker_symbol, company_name, **kwargs)
        if not rows:
            return pd.DataFrame(columns=["title", "publisher", "published_at", "link", "content"])
        df = pd.DataFrame(rows)
        # Convert to Asia/Kolkata for display
        try:
            df["published_at_local"] = df["published_at"].dt.tz_convert("Asia/Kolkata")
        except Exception:
            pass
        return df

# ---------- Example usage ----------
if __name__ == "__main__":
    nf = NewsFetcher(use_llm=False)  # set True only if ollama is installed and model is running
    df = nf.fetch_as_dataframe("RELIANCE", "Reliance Industries")
    # Print minimal view
    with pd.option_context("display.max_colwidth", 100):
        print(df[["published_at", "publisher", "title", "link"]].to_string(index=False))
