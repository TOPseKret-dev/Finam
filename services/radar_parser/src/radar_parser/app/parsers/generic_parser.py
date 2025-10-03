# src/app/parsers/generic_parser.py
from __future__ import annotations
import feedparser
from typing import List, Dict, Any

def parse_atom(feed_xml: str, source: str) -> List[Dict[str, Any]]:
    parsed = feedparser.parse(feed_xml)
    items: List[Dict[str, Any]] = []
    for e in parsed.entries:
        items.append({
            "source":   source,
            "title":    e.get("title"),
            "link":     e.get("link"),
            "published": e.get("published") or e.get("updated") or e.get("pubDate"),
            "summary":  e.get("summary") or e.get("description") or "",
        })
    return items
