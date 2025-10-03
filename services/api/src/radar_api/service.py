from typing import Dict
from radar_parser.collector import collect_events
from .scoring import compute_hotness

async def get_top_k(window: int = 24, k: int = 5) -> Dict:
    events = await collect_events(window_hours=window)

    enriched = []
    for e in events:
        score = compute_hotness(e)
        headline = e["title"]
        why = f"{e['confirmations']} публикаций; первое упоминание — {e['first_seen']}"
        draft = {
            "title": headline,
            "lead": e["text"][:300],
            "bullets": [
                f"Когда: {e['first_seen']}",
                f"Подтверждения: {e['confirmations']}",
                f"Источников: {len(e.get('sources', []))}"
            ],
            "note": "Проверьте первоисточники"
        }
        enriched.append({
            "id": e["id"],
            "headline": headline,
            "hotness": score,
            "why_now": why,
            "entities": [],  # слот для NER
            "sources": e.get("sources", []),
            "timeline": e.get("timeline", []),
            "draft": draft
        })

    topk = sorted(enriched, key=lambda x: x["hotness"], reverse=True)[:k]
    return {"window": f"{window}h", "k": k, "results": topk}
