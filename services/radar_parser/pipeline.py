from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os, sys
from typing import Optional, Dict, Any, List

from radar_parser.app.config import load_sources
from radar_parser.app.fetch.rss_direct import fetch_rss
from radar_parser.app.fetch.html_fetcher import fetch_html
from radar_parser.app.parsers.atom_parser import parse_atom
from radar_parser.app.parsers.site_parsers import parse_html as parse_html_doc
from radar_parser.app.parsers.listing_fetch import fetch_listing_and_articles

SOURCE_TYPES = {"rss", "bridge", "html", "html_listing"}


def _handle_rss(src) -> List[Dict[str, Any]]:
    verify = (str(src.get("verify_ssl")).lower() != "false")
    try:
        xml = fetch_rss(src["url"], verify=verify)
        return parse_atom(xml, src["name"])
    except Exception as e:
        msg = str(e)
        if any(code in msg for code in ("401", "403", "404", "429")) and src.get("fallback_url"):
            xml = fetch_rss(src["fallback_url"], verify=verify)
            return parse_atom(xml, src["name"])
        raise


def _handle_bridge(src) -> List[Dict[str, Any]]:
    return _handle_rss(src)


def _handle_html(src) -> List[Dict[str, Any]]:
    verify = (str(src.get("verify_ssl")).lower() != "false")
    html = fetch_html(src["url"], verify=verify)
    return [parse_html_doc(src["url"], html, src["name"])]


def _handle_html_listing(src) -> List[Dict[str, Any]]:
    items = fetch_listing_and_articles(src["url"], src["name"], limit=int(src.get("limit") or 20))
    return items


HANDLERS = {
    "rss": _handle_rss,
    "bridge": _handle_bridge,
    "html": _handle_html,
    "html_listing": _handle_html_listing,
}


def run_pipeline(config_path: Optional[str] = None, hours: int = 48, max_workers: int = 8) -> Dict[str, Any]:
    cfg = config_path or os.getenv("RADAR_SOURCES")

    if not cfg:
        here = Path(__file__).resolve()
        default_local = here.parent / "config" / "sources.csv"
        if default_local.exists():
            cfg = str(default_local)

    if not cfg:
        base = Path(sys.path[0]).resolve()
        candidates = [
            base / "services" / "radar_parser" / "config" / "sources.csv",
            base / "services" / "radar_parser" / "config" / "sources_local.csv",
            base / "config" / "sources.csv",
        ]
        for p in candidates:
            if p.exists():
                cfg = str(p)
                break

    if not cfg or not Path(cfg).exists():
        raise FileNotFoundError(
            "Не найден sources.csv.\n"
            "Передай config_path явно или установи RADAR_SOURCES.\n"
            "Ожидалось, например: services/radar_parser/config/sources.csv"
        )

    sources = load_sources(cfg)
    used_sources = [s for s in sources if (s.get("type") or "").strip().lower() in SOURCE_TYPES]

    per_source: Dict[str, Dict[str, Any]] = {s["name"]: {"type": (s.get("type") or "").strip().lower(),
                                                         "url": s.get("url"),
                                                         "ok": 0, "errors": []}
                                             for s in used_sources}

    all_parsed: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(HANDLERS[(s.get("type") or '').strip().lower()], s): s for s in used_sources}
        for fut, s in list(future_map.items()):
            try:
                items = fut.result()
                if items:
                    all_parsed.extend(items)
                    per_source[s["name"]]["ok"] += len(items)
            except Exception as e:
                msg = str(e)
                errors.append(
                    {"source": s["name"], "type": per_source[s["name"]]["type"], "url": s.get("url"), "error": msg})
                per_source[s["name"]]["errors"].append(msg)

    max_per_source = int(os.getenv("RADAR_MAX_ITEMS_PER_SOURCE", "50"))
    if max_per_source > 0:
        trimmed: List[Dict[str, Any]] = []
        cap: Dict[str, int] = {}
        for it in all_parsed:
            sname = it.get("source") or "unknown"
            n = cap.get(sname, 0)
            if n < max_per_source:
                cap[sname] = n + 1
                trimmed.append(it)
        all_parsed = trimmed

    by_source: Dict[str, int] = {}
    for it in all_parsed:
        sname = it.get("source") or "unknown"
        by_source[sname] = by_source.get(sname, 0) + 1

    res: Dict[str, Any] = {
        "collected_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "source_count": len(used_sources),
        "total_items_raw": sum(p["ok"] for p in per_source.values()),
        "total_items_after_filter": len(all_parsed),
        "items": all_parsed,
        "errors": errors,
        "per_source": per_source,
        "by_source": by_source,
    }
    return res
