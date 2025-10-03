from typing import List, Dict, Any, Optional
from datetime import datetime
from src.radar_parser.app.config import load_sources, resolve_config_path
from src.radar_parser.app.fetch.rssbridge import fetch_rssbridge
from src.radar_parser.app.parsers.generic_parser import parse_atom
from src.radar_parser.app.filter.time_filter import filter_last_hours


def run_pipeline(
        config_path: Optional[str] = "config/sources.csv",
        hours: int = 48,
        source_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if source_types is None:
        source_types = ["bridge"]

    # resolve_config_path сам корректно понимает абсолютные/относительные пути
    cfg_abs = resolve_config_path(config_path, project_root=None)
    sources = load_sources(cfg_abs)
    collected_at = datetime.utcnow().isoformat() + "Z"

    all_parsed = []
    source_count = 0
    for src in sources:
        typ = (src.get("type") or "").strip().lower()
        if typ not in source_types:
            continue
        source_count += 1
        name = src.get("name") or "unknown"
        url = src.get("url") or ""
        try:
            xml = fetch_rssbridge(url)
            parsed = parse_atom(xml, name)
            if parsed:
                all_parsed.extend(parsed)
        except Exception as e:
            print(f"[run_pipeline] warn: {name}: {e}")

    total_raw = len(all_parsed)
    items_after = filter_last_hours(all_parsed, hours=hours)
    total_after = len(items_after)

    return {
        "collected_at": collected_at,
        "config_used": cfg_abs,
        "source_count": source_count,
        "total_items_raw": total_raw,
        "total_items_after_filter": total_after,
        "items": items_after,
    }
