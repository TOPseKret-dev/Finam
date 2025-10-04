
from __future__ import annotations
import os
import requests

UA = os.getenv("RADAR_UA", "RadarParser/1.1 (+https://example.org)")
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
}

def fetch_rss(url: str, timeout: int = 25, verify: bool | None = None) -> str:
    if verify is None:
        verify = os.getenv("RADAR_VERIFY_SSL", "true").lower() == "true"
    resp = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS, allow_redirects=True, verify=verify)
    resp.raise_for_status()
    return resp.text
