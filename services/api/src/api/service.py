# radar_topk_minimal.py
import asyncio
from typing import Dict, Any, List
from datetime import datetime, timezone
import math
import aiohttp
from urllib.parse import urlparse

# --- Параметры (регулируй) ---
TIME_DECAY_HALF_LIFE_HOURS = 6.0
VELOCITY_SCALE = 3.0   # для tanh нормализации velocity
MAX_CONFIRMATIONS_NORM = 3.0  # нормировка числа подтверждений (links)
WEIGHTS = {
    "recency": 0.25,
    "velocity": 0.30,
    "confirmations": 0.18,
    "source_rep": 0.17,
    "entities": 0.10
}
# LLM endpoints (опционально)
OPENROUTER_API_KEY = None  # <-- если хочешь использовать openrouter, положи ключ
OPENROUTER_CHAT_URL = "https://api.openrouter.ai/chat/completions"
# --------------------------------

# Простая репутация источника (расширяй)
SOURCE_REPUTATION = {
    "reuters.com": 0.96,
    "bloomberg.com": 0.95,
    "ft.com": 0.93,
    "wsj.com": 0.92,
    "tass.ru": 0.6,
    "ria.ru": 0.55,
    # default ниже
}


async def testParse(windowed):
    return []


def time_decay_score(dt: datetime, now: datetime = None, half_life_hours: float = TIME_DECAY_HALF_LIFE_HOURS) -> float:
    now = now or datetime.now(timezone.utc)
    hours = max(0.0, (now - dt).total_seconds() / 3600.0)
    return 2 ** (-hours / half_life_hours)  # 1.0 at now, 0.5 at half_life

def get_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def get_source_reputation(url_or_source: str) -> float:
    d = get_domain(url_or_source) or (url_or_source or "")
    for known, rep in SOURCE_REPUTATION.items():
        if known in d:
            return rep
    # heuristics
    if d.endswith(".gov") or ".gov." in d:
        return 0.9
    return 0.4  # default low-medium

def normalize_velocity(repeats: float, age_hours: float) -> float:
    # velocity = repeats per hour (if very fresh age_hours might be <1)
    rate = repeats / max(age_hours, 1/24.0)  # avoid divide by zero; minute-old -> large rate
    return math.tanh(rate / VELOCITY_SCALE)  # maps to (0,1)

def normalize_confirmations(num_links: int, repeat_count: int) -> float:
    # combine unique links (как proxy независимых источников) и повторов
    base = min(1.0, num_links / MAX_CONFIRMATIONS_NORM)
    extra = min(1.0, math.log1p(repeat_count) / math.log1p(10))  # repeat_count up to ~10 gives full
    return min(1.0, 0.6 * base + 0.4 * extra)

def normalize_entities_count(n: int) -> float:
    # тысячи тикеров не бывает — несколько тикеров => больше охват
    return min(1.0, n / 3.0)

def compute_hotness(features: Dict[str, float], weights: Dict[str, float] = WEIGHTS) -> float:
    """
    features: {
      recency: [0,1],
      velocity: [0,1],
      confirmations: [0,1],
      source_rep: [0,1],
      entities: [0,1]
    }
    returns hotness in [0,1]
    """
    # safety: ensure keys present
    r = features.get("recency", 0.0)
    v = features.get("velocity", 0.0)
    c = features.get("confirmations", 0.0)
    s = features.get("source_rep", 0.0)
    e = features.get("entities", 0.0)

    raw = (
        weights["recency"] * r +
        weights["velocity"] * v +
        weights["confirmations"] * c +
        weights["source_rep"] * s +
        weights["entities"] * e
    )
    # final clipping (already in [0,1] because weights sum 1), but be robust:
    return max(0.0, min(1.0, raw))

def make_why_now(features: Dict[str, Any]) -> str:
    reasons = []
    if features["recency"] > 0.6:
        reasons.append("свежая публикация")
    if features["velocity"] > 0.6:
        reasons.append("высокая скорость репостов/повторов")
    if features["confirmations"] > 0.5:
        reasons.append("несколько подтверждающих источников")
    if features["source_rep"] > 0.85:
        reasons.append("источник с высокой репутацией")
    if features["entities"] > 0.6:
        reasons.append("широкий охват активов")
    if not reasons:
        return "Накопление повторов и быстрый рост упоминаний."
    return ", ".join(reasons[:2]) + "."

# Optional: minimal OpenRouter draft generator (если у тебя есть ключ)
async def generate_draft_openrouter(item: Dict[str, Any]) -> str:
    if not OPENROUTER_API_KEY:
        return ""
    prompt = (
        "Сформируй короткий черновик финансовой заметки (headline, 1-2 предложения лид, 3 буллета, 1 строчка с ссылками).\n"
        "Дай факты только из переданных данных.\n\n"
        f"TEXT: {item.get('text','')[:2000]}\n"
        f"LINKS: {', '.join(item.get('links',[])[:5])}\n"
        f"ENTITIES: {item.get('entities',{})}\n"
    )
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
    payload = {
        "model": "google/gemini-2.5-flash",
        "messages": [{"role":"user","content":prompt}],
        "max_tokens": 400
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(OPENROUTER_CHAT_URL, json=payload, headers=headers) as resp:
            j = await resp.json()
            # адаптируй под фактическую схему ответа
            return j["choices"][0]["message"]["content"]

# --- Основная функция ---
async def get_top_k(window: int = 24, k: int = 5) -> Dict[str, Any]:
    """
    window: hours (передается тестПарсу, но мы просто проксируем)
    Возвращает dict: {"items": [ {headline, hotness, why_now, entities, sources, timeline, draft, dedup_group} ]}
    """
    data = await testParse(window)   # <- твоя затычка, возвращает список словарей
    now = datetime.now(timezone.utc)
    items_out = []

    for it in data:
        # нормализуем время
        t = it.get("Время выхода") or it.get("time") or it.get("time_published")
        if isinstance(t, str):
            try:
                t_dt = datetime.fromisoformat(t)
            except Exception:
                t_dt = now
        elif isinstance(t, datetime):
            t_dt = t
        else:
            t_dt = now

        age_hours = max(0.0, (now - t_dt).total_seconds() / 3600.0)
        recency = time_decay_score(t_dt, now)

        repeats = float(it.get("количество повторяшек", it.get("repeat_count", 1)))
        num_links = len(it.get("список ссылок внутри текста", it.get("links", [])))
        url = it.get("ссылка на саму статью", it.get("url", it.get("source")))
        source_rep = get_source_reputation(url)

        entities = it.get("entities") or it.get("список сущностей") or {}
        tickers = entities.get("tickers", []) if isinstance(entities, dict) else []
        num_entities = len(tickers)

        velocity = normalize_velocity(repeats, age_hours)
        confirmations = normalize_confirmations(num_links, int(repeats))
        entities_norm = normalize_entities_count(num_entities)

        features = {
            "recency": recency,
            "velocity": velocity,
            "confirmations": confirmations,
            "source_rep": source_rep,
            "entities": entities_norm
        }

        hotness = compute_hotness(features)

        why_now = make_why_now(features)

        # draft через LLM — асинхронно (опционально). Я собираю пустую строку, если ключа нет.
        draft = ""
        # не вызываю LLM автоматически, но если ключ указан — сделаю вызов
        if OPENROUTER_API_KEY:
            try:
                draft = await generate_draft_openrouter({
                    "text": it.get("text") or it.get("текст"),
                    "links": it.get("links") or it.get("список ссылок внутри текста") or [],
                    "entities": entities
                })
            except Exception as e:
                draft = f"LLM error: {e}"

        items_out.append({
            "headline": None,  # можно парсить из draft или формировать шаблонно
            "hotness": round(hotness, 4),
            "why_now": why_now,
            "entities": entities,
            "sources": (it.get("links") or it.get("список ссылок внутри текста") or [])[:5],
            "timeline": [{"time": t_dt.isoformat(), "url": url}],
            "draft": draft,
            "dedup_group": it.get("dedup_group") or f"article:{url or (it.get('id') or '')}",
            "raw_item": it,
            "features": features
        })

    # сортируем по hotness
    items_out.sort(key=lambda x: x["hotness"], reverse=True)
    return {"items": items_out[:k], "generated_at": now.isoformat(), "k": k}
