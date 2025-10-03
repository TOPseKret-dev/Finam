from datetime import datetime, timezone
import math

def _hours_since(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts)
    except Exception:
        return 9999.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.001, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)

def compute_hotness(event: dict) -> float:
    age_h = _hours_since(event.get("first_seen", "1970-01-01T00:00:00+00:00"))
    unexpected = 1.0 / (1.0 + math.log1p(age_h))
    confirmations = min(1.0, event.get("confirmations", 1) / 5.0)
    spread = min(1.0, len(event.get("sources", [])) / 3.0)
    w_unexp, w_conf, w_spread = 0.5, 0.3, 0.2
    score = w_unexp*unexpected + w_conf*confirmations + w_spread*spread
    return max(0.0, min(1.0, score))
