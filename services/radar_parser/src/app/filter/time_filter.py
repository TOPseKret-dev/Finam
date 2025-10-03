from datetime import datetime, timedelta
import dateutil.parser

def filter_last_hours(items, hours=48):
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    filtered = []
    for item in items:
        try:
            pub = dateutil.parser.parse(item["published"])
            if pub >= cutoff:
                filtered.append(item)
        except Exception:
            continue
    return filtered
