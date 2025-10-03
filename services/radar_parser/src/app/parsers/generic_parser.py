import feedparser
from datetime import datetime

def parse_atom(feed_xml: str, source: str):
    """Парсит Atom/RSS и возвращает список словарей"""
    parsed = feedparser.parse(feed_xml)
    items = []
    for entry in parsed.entries:
        items.append({
            "source": source,
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "summary": entry.get("summary", ""),
        })
    return items
