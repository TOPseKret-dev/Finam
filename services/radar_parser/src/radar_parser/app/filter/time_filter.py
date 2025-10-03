# src/app/filter/time_filter.py
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import dateutil.parser
from typing import List, Dict, Any

def _to_utc(dt):
    if dt.tzinfo is None:
        # трактуем как UTC, чтобы не попадать в локальный tz
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def filter_last_hours(items: List[Dict[str, Any]], hours: int = 48) -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out: List[Dict[str, Any]] = []
    for it in items:
        ds = it.get("published")
        if not ds:
            continue
        try:
            dt = dateutil.parser.parse(ds)
            dt = _to_utc(dt)
            if dt >= cutoff:
                out.append(it)
        except Exception:
            continue
    return out
