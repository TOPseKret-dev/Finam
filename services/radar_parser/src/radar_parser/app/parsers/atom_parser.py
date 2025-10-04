# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import feedparser

from .generic_parser import parse_date_safe, to_iso_utc, first_nonempty

def parse_atom(feed_xml: str, source: str) -> List[Dict[str, Any]]:
    """
    Универсальный разбор RSS/Atom с нормализацией даты в ISO-8601 UTC.
    """
    parsed = feedparser.parse(feed_xml)
    items: List[Dict[str, Any]] = []

    for e in parsed.entries:
        # 1) дата
        dt_str = first_nonempty(
            e.get("published"), e.get("updated"), e.get("pubDate"), e.get("dc_date")
        )
        dt_obj: Optional[datetime] = parse_date_safe(dt_str)

        if not dt_obj:
            # Попытка из struct_time
            st = first_nonempty(e.get("published_parsed"), e.get("updated_parsed"))
            if st:
                try:
                    dt_obj = datetime(*st[:6], tzinfo=timezone.utc)
                except Exception:
                    dt_obj = None

        published_iso = to_iso_utc(dt_obj)

        # 2) ссылка
        link = e.get("link")
        if not link:
            links = e.get("links") or []
            if links and isinstance(links, list):
                href = links[0].get("href")
                if href:
                    link = href

        items.append({
            "source": source,
            "title": e.get("title"),
            "link": link,
            "published": published_iso,  # ISO-8601 UTC или None
            "summary": first_nonempty(e.get("summary"), e.get("description"), ""),
        })

    return items
