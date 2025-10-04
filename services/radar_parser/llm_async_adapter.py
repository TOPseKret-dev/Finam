from __future__ import annotations
import asyncio, re, html, hashlib
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dateutil import parser as dtp, tz

TARGET_TZ = ZoneInfo("Europe/Moscow")
TZINFOS = {
    "UTC": tz.UTC, "GMT": tz.UTC,
    "EST": tz.gettz("America/New_York"), "EDT": tz.gettz("America/New_York"),
    "CST": tz.gettz("America/Chicago"), "CDT": tz.gettz("America/Chicago"),
    "PST": tz.gettz("America/Los_Angeles"), "PDT": tz.gettz("America/Los_Angeles"),
    "BST": tz.gettz("Europe/London"), "CEST": tz.gettz("Europe/Paris"), "CET": tz.gettz("Europe/Paris"),
    "MSK": tz.gettz("Europe/Moscow"),
}

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

ERROR_PHRASES = (
    "bridge returned error",
    "404 page not found",
    "navigation",
    "access denied",
    "forbidden",
)


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


def _http_code_in(obj: Any) -> Optional[int]:
    """
    Пытается найти HTTP-код в dict (включая вложенные 'response'/'meta'/'result').
    Возвращает int (100..599) или None, если ничего похожего не найдено.
    """
    if not isinstance(obj, dict):
        return None

    candidate_keys = ("status", "status_code", "http", "http_status", "code")
    for k in candidate_keys:
        v = obj.get(k)
        if isinstance(v, (int, str)) and str(v).isdigit():
            iv = int(v)
            if 100 <= iv <= 599:
                return iv

    for container in ("response", "meta", "result"):
        sub = obj.get(container)
        if isinstance(sub, dict):
            iv = _http_code_in(sub)
            if iv is not None:
                return iv
    return None


def _has_bad_http(obj: Dict[str, Any]) -> bool:
    """
    True если найден явный не-2xx HTTP-код, либо ok=False, либо присутствуют error/exception/traceback.
    """
    code = _http_code_in(obj)
    if code is not None:
        return not (200 <= code < 300)

    ok = obj.get("ok")
    if isinstance(ok, bool) and ok is False:
        return True

    if any(obj.get(k) for k in ("error", "exception", "traceback")):
        return True

    return False


def _is_garbage_text(body: str, title: str) -> bool:
    s = f"{title} {body}".lower()
    if any(p in s for p in ERROR_PHRASES):
        return True
    # слишком короткие куски — часто навигация/прокладки
    if len(_norm_for_dupe(body)) < 40:
        return True
    return False


def _is_bad_link(link: Optional[str]) -> bool:
    if not link:
        return False
    l = link.lower()
    return ("localhost:" in l) or ("rss-bridge" in l)


def _to_llm_schema(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует items -> список словарей вида:
    {
      "Время выхода": ISO8601 в Europe/Moscow или None,
      "источник": str,
      "текст статьи": str (очищенный),
      "список ссылок внутри текста": [str, ...],
      "ссылка на саму статью": str|None,
      "количество повторений": int
    }
    """
    tmp: List[Dict[str, Any]] = []
    freq: Dict[str, int] = {}

    for it in items:
        title = it.get("title") or ""
        summary = it.get("summary") or it.get("description") or ""
        raw = f"{title}. {summary}".strip(". ")

        body, inner_links = _extract_text_and_links(raw)
        if len(body) < 25:  # иногда только заголовок информативен
            t_only, _ = _extract_text_and_links(title)
            if len(t_only) > len(body):
                body = t_only

        link = it.get("link") or None
        dt = _parse_dt(it.get("published") or it.get("updated") or it.get("pubDate"))
        when = dt.isoformat() if dt else None
        source = it.get("source") or it.get("feed") or "unknown"

        if _is_garbage_text(body, title):
            continue
        if _is_bad_link(link):
            continue

        rec = {
            "Время выхода": when,
            "источник": source,
            "текст статьи": body,
            "список ссылок внутри текста": inner_links,
            "ссылка на саму статью": link,
            "количество повторений": 1,
        }
        fp = _fingerprint(body or title)
        rec["_fp"] = fp
        tmp.append(rec)
        freq[fp] = freq.get(fp, 0) + 1

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


async def build_llm_payload(hours: int = 48, config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Асинхронно дергает run_pipeline (в отдельном потоке),
    и возвращает список словарей в требуемом формате.

    Дополнительно: отбрасывает items, у которых HTTP-статус явно не 2xx
    (ищется в ключах status/status_code/http/http_status/code, в т.ч. во вложенных 'response'/'meta'/'result'),
    либо присутствует ok=False / error / exception / traceback.
    """

    def _runner():
        from services.radar_parser.pipeline import run_pipeline
        if config_path:
            return run_pipeline(config_path=config_path, hours=hours)
        return run_pipeline(hours=hours)

    res = await asyncio.to_thread(_runner)
    items = res.get("items", []) if isinstance(res, dict) else (res or [])

    items = [it for it in items if not _has_bad_http(it)]

    return _to_llm_schema(items)
