
from __future__ import annotations
import os, csv
from pathlib import Path
from typing import List, Dict, Any

_DEFAULT_REL = "services/radar_parser/config/sources.csv"

def resolve_config_path(config_path: str | None, project_root: str | None = None) -> str:
    tried: list[str] = []

    def _exists(p: str) -> str | None:
        tried.append(p)
        return p if Path(p).exists() else None

    # 1) явный параметр
    if config_path:
        p = _exists(config_path)
        if p: return p

    # 2) env
    env_p = os.getenv("RADAR_SOURCES")
    if env_p:
        p = _exists(env_p)
        if p: return p

    # 3) путь внутри репозитория
    repo_root = Path(project_root or Path(__file__).resolve().parents[5])  # .../services/radar_parser/src/radar_parser/app
    repo_p = repo_root / _DEFAULT_REL
    p = _exists(str(repo_p))
    if p: return p

    # 4) пакетный ресурс рядом с модулем (например, при установке как wheel)
    pkg_p = Path(__file__).resolve().parents[3] / "config" / "sources.csv"
    p = _exists(str(pkg_p))
    if p: return p

    # 5) ничего не нашли — объясняем, где искали
    raise FileNotFoundError("Sources CSV not found. Tried:\n" + "\n".join(tried))

def load_sources(p: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(p, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # нормализуем ключи
            rows.append({k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
    return rows
