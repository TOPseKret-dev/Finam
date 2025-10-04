from __future__ import annotations
import os
import time
from collections import defaultdict
from urllib.parse import urlparse

import requests

UA = os.getenv("RADAR_UA", "RadarParser/1.1 (+https://example.org)")
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_LAST_HIT = defaultdict(float)


def _sleep_if_needed(url: str, min_gap: float = 0.7):
    host = urlparse(url).hostname or ""
    now = time.time()
    elapsed = now - _LAST_HIT[host]
    if elapsed < min_gap:
        time.sleep(min_gap - elapsed)
    _LAST_HIT[host] = time.time()


def fetch_html(url: str, timeout: int = 25, verify: bool | None = None) -> str:
    if verify is None:
        verify = os.getenv("RADAR_VERIFY_SSL", "true").lower() == "true"
    _sleep_if_needed(url)
    resp = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS, allow_redirects=True, verify=verify)
    resp.raise_for_status()
    return resp.text
