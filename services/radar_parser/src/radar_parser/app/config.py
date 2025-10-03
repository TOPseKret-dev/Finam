from pathlib import Path
import csv, os
from typing import List, Dict, Optional

ENV_CONFIG = "RADAR_SOURCES_CSV"

def _project_root() -> Path:
    # src/radar_parser/app/config.py -> project root = 3 уровня вверх
    return Path(__file__).resolve().parents[3]

def resolve_config_path(path_like: Optional[str], project_root: Optional[str|Path]) -> str:
    pr = Path(project_root).resolve() if project_root else _project_root()
    if path_like:
        p = Path(path_like)
        return str(p if p.is_absolute() else (pr / p).resolve())
    env = os.getenv(ENV_CONFIG)
    if env:
        p = Path(env)
        return str(p if p.is_absolute() else (pr / p).resolve())
    return str((pr / "config" / "sources.csv").resolve())

def load_sources(abs_path: str) -> List[Dict[str, str]]:
    p = Path(abs_path)
    if not p.exists():
        raise FileNotFoundError(f"Sources CSV not found: {p}")
    rows = []
    with p.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append({k: (v or "").strip() for k,v in r.items()})
    return rows
