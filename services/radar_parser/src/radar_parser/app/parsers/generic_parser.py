# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from dateutil import parser as dtp, tz

# Нормализация таймзон (убирает варнинги вида UnknownTimezoneWarning: EST ...)
TZINFOS = {
    "UTC": tz.UTC,
    "GMT": tz.UTC,
    "EST": tz.gettz("America/New_York"),
    "EDT": tz.gettz("America/New_York"),
    "CST": tz.gettz("America/Chicago"),
    "CDT": tz.gettz("America/Chicago"),
    "PST": tz.gettz("America/Los_Angeles"),
    "PDT": tz.gettz("America/Los_Angeles"),
    "BST": tz.gettz("Europe/London"),
    "CEST": tz.gettz("Europe/Paris"),
    "CET": tz.gettz("Europe/Paris"),
    "MSK": tz.gettz("Europe/Moscow"),
}

def parse_date_safe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return dtp.parse(s, tzinfos=TZINFOS)
    except Exception:
        return None

def to_iso_utc(dt_obj: Optional[datetime]) -> Optional[str]:
    if not dt_obj:
        return None
    if not dt_obj.tzinfo:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc).isoformat()

def first_nonempty(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None
