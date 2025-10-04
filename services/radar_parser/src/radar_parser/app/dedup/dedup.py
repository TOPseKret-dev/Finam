from __future__ import annotations
from typing import List, Dict
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

STRIP_Q = ("utm_", "fbclid", "yclid", "gclid", "ref")

def _canonical_url(u: str) -> str:
    sp = urlsplit(u)
    q = [(k, v) for k, v in parse_qsl(sp.query, keep_blank_values=True) if not any(k.lower().startswith(p) for p in STRIP_Q)]
    scheme = "https" if sp.scheme in ("http", "https") else sp.scheme
    path = sp.path.rstrip("/") or "/"
    return urlunsplit((scheme, sp.netloc.lower(), path, urlencode(q, doseq=True), ""))

def dedup(items: List[Dict]) -> List[Dict]:
    seen = set()
    out: List[Dict] = []
    for it in items:
        url = _canonical_url(it.get("link") or it.get("url") or "")
        title = (it.get("title") or "").strip().lower()
        key = (url, title)
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out
