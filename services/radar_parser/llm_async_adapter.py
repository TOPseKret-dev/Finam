from __future__ import annotations
import asyncio, re, html, hashlib
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from bs4 import BeautifulSoup  # pip install beautifulsoup4
from dateutil import parser as dtp, tz

# ===== настройки времени =====
TARGET_TZ = ZoneInfo("Europe/Moscow")
TZINFOS = {
    "UTC": tz.UTC, "GMT": tz.UTC,
    "EST": tz.gettz("America/New_York"), "EDT": tz.gettz("America/New_York"),
    "CST": tz.gettz("America/Chicago"), "CDT": tz.gettz("America/Chicago"),
    "PST": tz.gettz("America/Los_Angeles"), "PDT": tz.gettz("America/Los_Angeles"),
    "BST": tz.gettz("Europe/London"), "CEST": tz.gettz("Europe/Paris"), "CET": tz.gettz("Europe/Paris"),
    "MSK": tz.gettz("Europe/Moscow"),
}

# ===== утилиты =====
NOISE_PATTERNS = [
    r"Please open Telegram to view this post",
    r"VIEW IN TELEGRAM",
    r"Подписывайтесь.*$",
    r"Оставляйте.*$",
    r"Картина дня.*$",
    r"^Реклама\..*$",
    r"^erid:\s*\S+",
]
NOISE_RE = re.compile("|".join(f"(?:{p})" for p in NOISE_PATTERNS),
                      re.IGNORECASE | re.MULTILINE)


def _cleanup_noise(text: str) -> str:
    return re.sub(NOISE_RE, " ", text)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = dtp.parse(str(s), tzinfos=TZINFOS)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TARGET_TZ)
    except Exception:
        return None


def _extract_text_and_links(html_or_text: str) -> tuple[str, List[str]]:
    if not html_or_text:
        return "", []
    s = html.unescape(html_or_text)
    soup = BeautifulSoup(s, "html.parser")
    # ссылки
    hrefs: List[str] = []
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if href:
            hrefs.append(href)
    # текст
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    text = _cleanup_noise(text)
    # uniq order-preserving
    seen, uniq = set(), []
    for u in hrefs:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return text, uniq


def _norm_for_dupe(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\W+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _fingerprint(s: str) -> str:
    return hashlib.sha1(_norm_for_dupe(s).encode("utf-8")).hexdigest()


def _to_llm_schema(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует твои items -> список словарей вида:
    {
      "Время выхода": ISO8601 в Asia/Almaty или None,
      "источник": str,
      "текст статьи": str (очищенный),
      "список ссылок внутри текста": [str, ...],
      "ссылка на саму статью": str|None,
      "количество повторений": int
    }
    """
    tmp: List[Dict[str, Any]] = []
    freq: Dict[str, int] = {}

    # 1) сбор с очисткой и первичным fp
    for it in items:
        title = it.get("title") or ""
        summary = it.get("summary") or it.get("description") or ""
        raw = f"{title}. {summary}".strip(". ")
        body, inner_links = _extract_text_and_links(raw)
        if len(body) < 25:  # вдруг только заголовок информативен
            t_only, _ = _extract_text_and_links(title)
            if len(t_only) > len(body):
                body = t_only

        link = it.get("link") or None
        dt = _parse_dt(it.get("published") or it.get("updated") or it.get("pubDate"))
        when = dt.isoformat() if dt else None
        source = it.get("source") or it.get("feed") or "unknown"

        rec = {
            "Время выхода": when,
            "источник": source,
            "текст статьи": body,
            "список ссылок внутри текста": inner_links,
            "ссылка на саму статью": link,
            "количество повторений": 1,  # скорректируем позже
        }
        fp = _fingerprint(body or title)
        rec["_fp"] = fp
        tmp.append(rec)
        freq[fp] = freq.get(fp, 0) + 1

    # 2) проставляем повторы и убираем точные дубли по (link,text)
    out: List[Dict[str, Any]] = []
    seen_pairs = set()
    for r in tmp:
        r["количество повторений"] = freq.get(r["_fp"], 1)
        r.pop("_fp", None)
        key = (r.get("ссылка на саму статью"), r.get("текст статьи"))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        out.append(r)
    return out


# ===== Публичный async-API =====
async def build_llm_payload(hours: int = 48, config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Асинхронно дергает твой run_pipeline (в отдельном потоке, чтобы не блокировать event loop),
    и возвращает список словарей в требуемом формате.
    """

    def _runner():
        from pipeline import run_pipeline
        if config_path:
            return run_pipeline(config_path=config_path, hours=hours)
        return run_pipeline(hours=hours)

    res = await asyncio.to_thread(_runner)
    items = res.get("items", []) if isinstance(res, dict) else (res or [])
    return _to_llm_schema(items)

# Быстрый самотест:
# if __name__ == "__main__":
#     import json
#     async def _main():
#         payload = await build_llm_payload(hours=48)
#         print(json.dumps(payload[:3], ensure_ascii=False, indent=2))
#     asyncio.run(_main())
