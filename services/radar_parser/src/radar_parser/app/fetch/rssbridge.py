from __future__ import annotations
import requests

UA = "RadarParser/1.0 (+fetch)"


def fetch_rssbridge(url: str, timeout: int = 20) -> str:
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": UA})
    resp.raise_for_status()
    return resp.text
