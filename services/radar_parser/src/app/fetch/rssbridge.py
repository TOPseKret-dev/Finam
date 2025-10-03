import requests

def fetch_rssbridge(url: str) -> str:
    """Загружает Atom/RSS ленту через rss-bridge"""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.text
