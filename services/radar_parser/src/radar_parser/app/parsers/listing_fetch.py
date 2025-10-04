# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..fetch.html_fetcher import fetch_html
from .site_parsers import parse_html as parse_article

LINK_RULES = {
    "www.rbc.ru": ["a.news-feed__item", "a.js-news-feed-item"],
    "rbc.ru": ["a.news-feed__item", "a.js-news-feed-item"],
    "www.kommersant.ru": ["article a[href*='/doc/']", "h2 a[href*='/doc/']", ".news-item a[href*='/doc/']"],
    "kommersant.ru": ["article a[href*='/doc/']", "h2 a[href*='/doc/']"],
    "tass.ru": ["article a[href*='/news/']", "a.card__link"],
    "www.tass.ru": ["article a[href*='/news/']", "a.card__link"],
    "vedomosti.ru": ["article a[href*='/news/']", "h2 a[href*='/news/']"],
    "www.vedomosti.ru": ["article a[href*='/news/']", "h2 a[href*='/news/']"],
}

GENERIC_RULES = ["article a[href]", "h2 a[href]", ".news a[href]", ".item a[href]"]

def _pick_rules(host: str) -> List[str]:
    return LINK_RULES.get(host, GENERIC_RULES)

def fetch_listing_and_articles(listing_url: str, source_name: str, limit: int = 20) -> List[Dict]:
    html = fetch_html(listing_url)
    soup = BeautifulSoup(html, "lxml")

    host = (urlparse(listing_url).hostname or "").lower()
    rules = _pick_rules(host)

    links: List[str] = []
    seen = set()
    for sel in rules:
        for a in soup.select(sel):
            href = a.get("href")
            if not href:
                continue
            link = urljoin(listing_url, href)
            if link in seen:
                continue
            if link.startswith(("mailto:", "tel:")):
                continue
            seen.add(link)
            links.append(link)
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    out: List[Dict] = []
    for link in links:
        try:
            art_html = fetch_html(link)
            out.append(parse_article(link, art_html, source_name))
        except Exception:
            pass
    return out
