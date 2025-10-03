# src/radar_parser/__main__.py
import sys, json
from .pipeline import run_pipeline


def _cli(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    cfg = argv[0] if len(argv) >= 1 else "config/sources.csv"
    hours = int(argv[1]) if len(argv) >= 2 else 48
    out = argv[2] if len(argv) >= 3 else "output.json"

    res = run_pipeline(config_path=cfg, hours=hours)
    print(f"Collected: {res['total_items_after_filter']} / raw {res['total_items_raw']}")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    print("Wrote", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
