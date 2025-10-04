from datetime import datetime, timezone

def _hours_since(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts)
    except Exception:
        return 9999.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.001, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)

def compute_hotness(event: dict) -> float:
    pass
