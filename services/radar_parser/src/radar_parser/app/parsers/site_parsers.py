# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Callable
from bs4 import BeautifulSoup
from .generic_parser import first_nonempty

def _text_or_none(node):
    return (node.get_text(" ", strip=True) if node else None)

# --- TASS.ru ---
def parse_html_tass(url: str, html: str, source_name: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    title_node = soup.select_one("h1")
    title = _text_or_none(title_node) or _text_or_none(soup.title)
    if not title:
        og = soup.select_one('meta[property="og:title"]')
        title = og.get("content").strip() if og and og.get("content") else None

    pub = (soup.select_one('meta[property="article:published_time"]') or
           soup.select_one('time[datetime]') or
           soup.select_one('meta[name="pubdate"]'))
    published = (pub.get("content") if pub and pub.has_attr("content")
                 else (pub.get("datetime") if pub and pub.has_attr("datetime") else None))

    body = (soup.select_one('div[itemprop="articleBody"]') or
            soup.select_one("article .text-content") or
            soup.select_one("article"))
    text = _text_or_none(body)
    if not text:
        ps = soup.select("article p, .text-content p")
        text = " ".join(p.get_text(" ", strip=True) for p in ps[:80]) or None

    return {
        "source": source_name,
        "title": title,
        "link": url,
        "published": published,
        "summary": None,
        "raw_html": html,
        "text": text,
    }

# --- Vedomosti.ru ---
def parse_html_vedomosti(url: str, html: str, source_name: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    title_node = soup.select_one("h1") or soup.title
    title = _text_or_none(title_node)
    if not title:
        og = soup.select_one('meta[property="og:title"]')
        title = og.get("content").strip() if og and og.get("content") else None

    pub = (soup.select_one('meta[property="article:published_time"]') or
           soup.select_one('time[datetime]') or
           soup.select_one('meta[name="pubdate"]') or
           soup.select_one('meta[itemprop="datePublished"]'))
    published = (pub.get("content") if pub and pub.has_attr("content")
                 else (pub.get("datetime") if pub and pub.has_attr("datetime") else None))

    body = (soup.select_one(".article__content") or
            soup.select_one(".article-content") or
            soup.select_one("article"))
    text = _text_or_none(body)
    if not text:
        ps = soup.select("article p, .article__content p, .article-content p")
        text = " ".join(p.get_text(" ", strip=True) for p in ps[:80]) or None

    return {
        "source": source_name,
        "title": title,
        "link": url,
        "published": published,
        "summary": None,
        "raw_html": html,
        "text": text,
    }

# --- Реестр: домен → парсер ---
_PARSERS: Dict[str, Callable[[str, str, str], dict]] = {
    "tass.ru": parse_html_tass,
    "www.tass.ru": parse_html_tass,
    "vedomosti.ru": parse_html_vedomosti,
    "www.vedomosti.ru": parse_html_vedomosti,
}

def parse_html(url: str, html: str, source_name: str) -> dict:
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or "").lower()
    fn = None
    if host in _PARSERS:
        fn = _PARSERS[host]
    else:
        for k, v in _PARSERS.items():
            if host.endswith(k):
                fn = v
                break
    if fn is None:
        soup = BeautifulSoup(html, "lxml")
        title = first_nonempty(_text_or_none(soup.select_one("h1")), _text_or_none(soup.title))
        body = soup.select_one("article") or soup.select_one("main") or soup
        text = _text_or_none(body)
        return {
            "source": source_name,
            "title": title,
            "link": url,
            "published": None,
            "summary": None,
            "raw_html": html,
            "text": text,
        }
    return fn(url, html, source_name)
