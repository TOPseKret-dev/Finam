import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timezone
import math
import aiohttp
from urllib.parse import urlparse
import os
import json
import re
import ssl
from services.radar_parser.llm_async_adapter import build_llm_payload

TIME_DECAY_HALF_LIFE_HOURS = 6.0
VELOCITY_SCALE = 3.0
MAX_CONFIRMATIONS_NORM = 3.0
STRICT_MODE = True

WEIGHTS = {
    "financial": 0.45,
    "recency": 0.20,
    "velocity": 0.15,
    "confirmations": 0.08,
    "source_rep": 0.07,
    "entities": 0.05,
}

FIN_ALLOWED_SECTIONS = (
    "business", "econom", "market", "finance", "company",
    "money", "realty", "tech", "commodit", "energy", "oil",
)
FIN_BLOCK_SECTIONS = (
    "sport", "sports", "politic", "photo", "style",
    "auto", "society", "culture", "lifestyle", "travel",
)

FIN_KEYWORDS = (
    "акци", "облигац", "дивиден", "buyback", "байбэк",
    "выручк", "прибыл", "убыт", "ebitda", "guidance", "прогноз",
    "ipo", "spo", "расписка", "gdr", "adr",
    "ставк", "ключев", "фрс", "цб", "инфляц", "ввп", "pmi",
    "офз", "купон", "доходност", "индекс", "рубл", "курс", "usd", "eur",
    "нефть", "brent", "wti", "газ", "угол", "уголь", "золот", "металл",
    "санкц", "ндс", "демпфер", "тариф", "квот", "пошлин", "бюджет", "рвп",
)

_TICKER_RE = re.compile(
    r"\b(?:MCX:|TQBR:)?[A-Z]{2,5}(?:\.[A-Z])?\b|USD/RUB|EUR/RUB|BRENT|WTI",
    re.IGNORECASE
)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = (
    '''Ты — RADAR.AI, автономный агент для анализа новостных потоков.
Твоя задача — из входных данных выделять самые горячие события и формировать компактный, верифицируемый JSON-вывод по схеме, показанной ниже.
Работай строго с существующими новостями, не придумывай фактов.
Выводи только JSON — без описаний вне него.
Формат входных данных:
[
  {
    "time": "2025-10-04T10:05:00Z",
    "source": "РИА Новости",
    "text": "ЦБ повысил ключевую ставку до 18% годовых, чтобы сдержать инфляцию.",
    "links_in_text": ["https://cbr.ru/press/"],
    "article_url": "https://ria.ru/economy/article123",
    "duplicates_count": 2
  }
]
Алгоритм работы: Сначала задай скор метрикам от 0 до 1
1. materiality -  оцени насколько событие влияет на экономику, компании, бюджет или благосостояние.
2.  unexpectedness - оцени насколько новость неожиданна для разных рынков или общества в целом.
3. spread_speed - Скорость распространения (оцени количество дубликатов, ссылок, посмотри как часто новость встречается в разных источниках).
4  breadth - оцени масштаб — сколько стран, отраслей или рынков затронуто.
5  credibility - оцени достоверность источников.
6  confirmations - количество независимых подтверждений (например, регуляторы, политики, рыночные аналитики).
7  recency - свежесть публикации, чем старее новость, тем меньше скор.
8  public_reaction - эмулируй эмоциональный и общественный отклик, опирайся на реакцию на похожие кейсы в прошлом.
9  russia_weight - оцени степень влияния на Россию, рубль или российских граждан.

1.  Для каждого текста оцени hotness ∈ [0,1] по девяти метрикам:
materiality, unexpectedness, spread_speed, breadth, credibility, confirmations, recency, public_reaction, russia_weight.
2.  Рассчитай base_hotness по формуле:
base_hotness = 0.25*materiality
             + 0.2*unexpectedness
             + 0.1*spread_speed
             + 0.1*breadth
             + 0.1*credibility
             + 0.05*confirmations
             + 0.05*recency
             + 0.1*public_reaction
             + 0.05*russia_weight
•  Применяй корректировки:
•  если russia_weight ≥ 0.7 → ×1.15 (cap 1.0)
•  если public_reaction ≥ 0.8 и credibility ≥ 0.7 → ×1.10 (cap 1.0)
•  Создай короткий черновик поста (draft), объясни почему новость важна именно сейчас (why_now), выдели сущности (entities) и источники (sources). Выведи 3-5 буллит поинтов по примеру ниже:
•  Выведи результат строго по шаблону ниже.
{
  "events": [
    {
      "headline": "ЦБ повысил ключевую ставку до 18% годовых",
      "hotness": 0.92,
      "why_now": "Решение неожиданно и напрямую влияет на ипотеку, кредиты и курс рубля. Вызывает бурное обсуждение в СМИ и у населения.",
      "entities": ["ЦБ РФ", "рубль", "инфляция", "ставка"],
"timeline": [
            {"time": "2025-10-04T10:05:00Z", "event": "первое сообщение"} 
          ]
"bullets": [
          "Решение принято на внеплановом заседании ЦБ РФ.",
          "Инфляционные ожидания превысили прогноз Минэкономразвития.",
          "Банки начали пересматривать условия по ипотеке и кредитам."
        ],
        "citation": "Источник: РИА Новости, 2025-10-04"
🔹 Правила поведения системы
    В JSON ТЕКСТ СТАТЬИ ДОЛЖЕН БЫТЬ ПЕРЕВЕДЕН НА РУССКИЙ ЯЗЫК, ЭТО ОЧЕНЬ ВАЖНО
•  Не добавляй выдуманных событий или ссылок.
•  Не пиши комментариев вне JSON.
•  Сохраняй сжатость и смысловую плотность.
•  Если данных мало, снижай hotness.
•  При равных hotness отдавай приоритет тем, что влияют на благосостояние населения или экономику РФ.
•  Все формулировки на русском языке.
•  RADAR.AI должен возвращать только JSON в описанном выше формате,
где каждая запись отражает реальное событие и имеет структуру:
•  headline → hotness → why_now → entities → sources → timeline → draft
    '''
)

SUMMARY_SCHEMA_DESC = """Верни JSON строго в формате:
{
  "impact_level": "нет" | "низкое" | "среднее" | "высокое",
  "summary": "2–4 предложения или 3–6 буллитов про влияние на RU рынок",
  "watchlist": ["тикер/сектор", "..."],
  "rationale": "коротко почему выбран такой уровень"
}
Если влияние практически отсутствует, impact_level="нет" и явно напиши, что существенного влияния на RU рынок не ожидается.
Не придумывай фактов и новые события. Опираться только на входные items.
"""

SOURCE_REPUTATION: Dict[str, float] = {
    "reuters.com": 0.96,
    "bloomberg.com": 0.95,
    "ft.com": 0.93,
    "wsj.com": 0.92,
    "cnbc.com": 0.90,
    "economist.com": 0.90,
    "tass.ru": 0.60,
    "ria.ru": 0.55,
}


def _ssl_context() -> ssl.SSLContext:
    """Создаёт SSL-контекст с доверенными корневыми сертификатами; учитывает SSL_CERT_FILE/SSL_CERT_DIR и certifi (если установлен)."""
    ctx = ssl.create_default_context()
    cafile = os.getenv("SSL_CERT_FILE")
    capath = os.getenv("SSL_CERT_DIR")
    if cafile or capath:
        try:
            ctx.load_verify_locations(cafile=cafile, capath=capath)
        except Exception:
            pass
    try:
        import certifi
        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    return ctx


def time_decay_score(dt: datetime, now: Optional[datetime] = None,
                     half_life_hours: float = TIME_DECAY_HALF_LIFE_HOURS) -> float:
    """Экспоненциальный скор свежести в [0,1] по возрасту события и полу-периоду."""
    now = now or datetime.now(timezone.utc)
    hours = max(0.0, (now - dt).total_seconds() / 3600.0)
    return 2 ** (-hours / half_life_hours)


def get_domain(url: str) -> str:
    """Возвращает домен из URL или пустую строку при ошибке."""
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def get_source_reputation(url_or_source: str) -> float:
    """Оценивает репутацию источника по известным доменам; .gov получает повышенную оценку."""
    d = get_domain(url_or_source) or (url_or_source or "").lower()
    for known, rep in SOURCE_REPUTATION.items():
        if known in d:
            return rep
    if d.endswith(".gov") or ".gov." in d:
        return 0.9
    return 0.4


def _path_flags(url: str) -> Tuple[bool, bool]:
    """Возвращает (allow, block) на основе рубрик в пути URL."""
    u = (url or "").lower()
    allow = any(s in u for s in FIN_ALLOWED_SECTIONS)
    block = any(s in u for s in FIN_BLOCK_SECTIONS)
    return allow, block


def adjust_rep_by_path(rep: float, url: str) -> float:
    """Корректирует репутацию с учётом рубрики URL (финансовые/нефинансовые разделы)."""
    allow, block = _path_flags(url or "")
    if allow:
        rep = min(1.0, rep + 0.10)
    if block:
        rep *= 0.5
    return rep


def normalize_velocity(repeats: float, age_hours: float, has_time: bool) -> float:
    """Нормализует скорость распространения через tanh(rate / VELOCITY_SCALE)."""
    if not has_time or repeats <= 1 or age_hours <= 0:
        return 0.0
    rate = repeats / max(age_hours, 1.0)
    return math.tanh(rate / VELOCITY_SCALE)


def normalize_confirmations(num_links: int, repeat_count: int) -> float:
    """Нормализует подтверждения, ограничивая MAX_CONFIRMATIONS_NORM."""
    return min(1.0, num_links / MAX_CONFIRMATIONS_NORM)


def normalize_entities_count(n: int) -> float:
    """Нормализует количество сущностей/тикеров до [0,1] с мягким капом на 3."""
    return min(1.0, n / 3.0)


def compute_financial_score(text: str, url: str, entities: Optional[Dict[str, Any]]) -> float:
    """Возвращает скор финансовой релевантности по ключам, тикерам и рубрике URL."""
    t = (text or "").lower()
    allow, block = _path_flags(url)
    kw_hit = any(k in t for k in FIN_KEYWORDS)
    ticker_hit = bool(_TICKER_RE.search(t)) or bool((entities or {}).get("tickers"))
    score = 0.0
    if allow:
        score += 0.40
    if kw_hit:
        score += 0.35
    if ticker_hit:
        score += 0.35
    if block and not (kw_hit or ticker_hit):
        score *= 0.25
    return min(1.0, score)


def compute_hotness(features: Dict[str, float], weights: Dict[str, float] = WEIGHTS) -> float:
    """Считает общий hotness как взвешенную сумму фич с ограничением [0,1]."""
    acc = 0.0
    for k, w in weights.items():
        acc += w * float(features.get(k, 0.0))
    return max(0.0, min(1.0, acc))


def make_why_now(features: Dict[str, Any]) -> str:
    """Формирует краткое объяснение «почему сейчас» по порогам фич."""
    reasons = []
    if features.get("financial", 0.0) > 0.6:
        reasons.append("финансовая релевантность (сигнал)")
    if features.get("recency", 0.0) > 0.6:
        reasons.append("свежая публикация")
    if features.get("velocity", 0.0) > 0.6:
        reasons.append("высокая скорость репостов/повторов")
    if features.get("confirmations", 0.0) > 0.5:
        reasons.append("несколько подтверждающих источников")
    if features.get("source_rep", 0.0) > 0.85:
        reasons.append("источник с высокой репутацией")
    if features.get("entities", 0.0) > 0.6:
        reasons.append("широкий охват активов")
    if not reasons:
        return "Накопление повторов и умеренная актуальность."
    return ", ".join(reasons[:2]) + "."


def _coerce_items(data: Union[List[Dict[str, Any]], Dict[str, Any], None]) -> List[Dict[str, Any]]:
    """Приводит вход к списку элементов (list[dict])."""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    return []


def _safe_parse_time(t: Any, fallback: datetime) -> Tuple[datetime, bool]:
    """Парсит ISO-время (поддерживает суффикс 'Z'); возвращает (dt_utc, has_time)."""
    if isinstance(t, datetime):
        return (t if t.tzinfo else t.replace(tzinfo=timezone.utc)), True
    if isinstance(t, str):
        s = t.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt, True
        except Exception:
            return fallback, False
    return fallback, False


def _openrouter_headers() -> Dict[str, str]:
    """Собирает HTTP-заголовки для OpenRouter с необязательной атрибуцией приложения."""
    if not OPENROUTER_API_KEY:
        return {"Content-Type": "application/json"}
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    referer = os.getenv("OPENROUTER_SITE_URL", "https://finam-radar.local")
    title = os.getenv("OPENROUTER_APP_TITLE", "Finam Radar")
    headers["HTTP-Referer"] = referer
    headers["X-Title"] = title
    return headers


def _make_connector() -> aiohttp.TCPConnector:
    """Создаёт aiohttp-коннектор с проверкой SSL (можно отключить через OPENROUTER_INSECURE_SSL=1)."""
    if os.getenv("OPENROUTER_INSECURE_SSL") == "1":
        return aiohttp.TCPConnector(ssl=False)
    return aiohttp.TCPConnector(ssl=_ssl_context())


async def _post_openrouter(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Отправляет POST на OpenRouter и возвращает распарсенный JSON либо подробную ошибку."""
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is not set")
    async with aiohttp.ClientSession(connector=_make_connector()) as s:
        async with s.post(OPENROUTER_CHAT_URL, json=payload, headers=_openrouter_headers(), timeout=60) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"OpenRouter HTTP {resp.status}: {text[:300]}")
            try:
                return json.loads(text)
            except Exception as e:
                raise RuntimeError(f"OpenRouter JSON parse error: {e}; payload: {text[:300]}")


async def generate_draft_openrouter(item: Dict[str, Any], system_prompt: str = SYSTEM_PROMPT) -> str:
    """Генерирует драфт по одному событию через OpenRouter; возвращает текст ассистента."""
    if not OPENROUTER_API_KEY:
        return ""
    text = (item.get("text") or "")[:3000]
    links = item.get("links") or []
    entities = item.get("entities") or {}
    user_prompt = f"TEXT:\n{text}\n\nLINKS:\n{', '.join(links[:5])}\n\nENTITIES:\n{entities}\n"
    payload = {
        "model": "google/gemini-2.5-flash",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 800,
        "temperature": 0.3,
    }
    j = await _post_openrouter(payload)
    try:
        content = j["choices"][0]["message"]["content"]
        return (content or "").strip()
    except Exception as e:
        raise RuntimeError(f"OpenRouter schema mismatch: {e}; keys={list(j.keys())}")


async def generate_overall_summary_openrouter(items_out: List[Dict[str, Any]],
                                              system_prompt: str = SYSTEM_PROMPT) -> Dict[str, Any]:
    """Строит сводное резюме по рынку из топ-событий (LLM) или локальный фолбэк по hotness."""
    if not OPENROUTER_API_KEY:
        if not items_out:
            return {
                "impact_level": "нет",
                "summary": "Релевантных событий не выявлено; влияния на российский рынок нет.",
                "watchlist": [],
                "rationale": "Пустой список событий."
            }
        avg_hot = sum(i.get("hotness", 0.0) for i in items_out) / max(1, len(items_out))
        if avg_hot < 0.2:
            level, msg = "нет", "Существенного влияния на российский рынок не ожидается."
        elif avg_hot < 0.4:
            level, msg = "низкое", "Влияние ограниченное; существенных драйверов для RU рынка не просматривается."
        elif avg_hot < 0.7:
            level, msg = "среднее", "Есть умеренные факторы влияния; реакция может быть точечной по секторам."
        else:
            level, msg = "высокое", "Высокая значимость событий; возможно влияние на индекс и рубль."
        return {"impact_level": level, "summary": msg, "watchlist": [],
                "rationale": f"Локальный фолбэк по среднему hotness={avg_hot:.2f}."}

    compact_items = []
    for x in items_out[:10]:
        compact_items.append({
            "headline": x.get("headline"),
            "hotness": x.get("hotness"),
            "why_now": x.get("why_now"),
            "time": (x.get("timeline") or [{}])[0].get("time"),
            "financial": round((x.get("features") or {}).get("financial", 0.0), 3),
            "source_rep": round((x.get("features") or {}).get("source_rep", 0.0), 3),
            "recency": round((x.get("features") or {}).get("recency", 0.0), 3),
            "velocity": round((x.get("features") or {}).get("velocity", 0.0), 3),
            "confirmations": round((x.get("features") or {}).get("confirmations", 0.0), 3),
            "entities_count": len((x.get("entities") or {}).get("tickers", [])) if isinstance(x.get("entities"),
                                                                                              dict) else 0
        })

    user_prompt = (
        "Ниже итоговый список событий (items) с оценками hotness и причинами why_now.\n"
        "Сформируй консолидированное резюме для российского рынка: индексы, рубль, ОФЗ, сектора/тикеры.\n"
        "Если влияние минимально — так и скажи.\n\n"
        f"items = {json.dumps(compact_items, ensure_ascii=False)}\n\n"
        f"{SUMMARY_SCHEMA_DESC}"
    )
    payload = {
        "model": "google/gemini-2.5-pro",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 600,
        "temperature": 0.2,
    }
    j = await _post_openrouter(payload)
    content = j["choices"][0]["message"]["content"].strip()
    try:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("not a dict")
        for k in ("impact_level", "summary", "watchlist", "rationale"):
            parsed.setdefault(k, "" if k != "watchlist" else [])
        return parsed
    except Exception:
        return {
            "impact_level": "низкое" if len(items_out) and (
                        sum(i.get("hotness", 0.0) for i in items_out) / len(items_out) < 0.4) else "среднее",
            "summary": content[:1200],
            "watchlist": [],
            "rationale": "Модель вернула не-JSON; контент сохранён как summary."
        }


async def get_top_k(window: int = 24, k: int = 5,
                    items: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Возвращает топ-k горячих событий и сводку влияния на рынок РФ."""
    data = _coerce_items(items)
    if not data:
        data = await build_llm_payload(window)
        try:
            open('data.json', 'w').write(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception:
            pass

    now = datetime.now(timezone.utc)
    items_out: List[Dict[str, Any]] = []

    for it in data:
        raw_time = it.get("Время выхода") or it.get("time") or it.get("time_published")
        t_dt, has_time = _safe_parse_time(raw_time, now)
        age_hours = max(0.0, (now - t_dt).total_seconds() / 3600.0)
        recency = time_decay_score(t_dt, now) if has_time else 0.0

        repeats = float(it.get("количество повторений", it.get("количество повторяшек", it.get("repeat_count", 1))))
        inner_links = (
                it.get("список ссылок внутри текста")
                or it.get("links_in_text")
                or it.get("links", [])
                or []
        )
        num_links = len(inner_links)

        url = (
                it.get("ссылка на саму статью")
                or it.get("article_url")
                or it.get("url")
                or (it.get("source") if (it.get("source") and str(it.get("source")).startswith("http")) else "")
        )
        source_field = it.get("источник") or it.get("source") or ""
        source_rep = get_source_reputation(url or source_field)
        source_rep = adjust_rep_by_path(source_rep, url or "")

        entities = it.get("entities") or it.get("список сущностей") or {}
        entities_norm = normalize_entities_count(
            len(entities.get("tickers", [])) if isinstance(entities, dict) else 0
        )

        text_for_score = it.get("текст статьи") or it.get("text") or ""
        fin_score = compute_financial_score(text_for_score, url or "", entities)
        if fin_score < 0.30:
            continue

        if STRICT_MODE:
            low = text_for_score.lower()
            strict_hit = bool(_TICKER_RE.search(text_for_score)) or any(
                key in low for key in
                ("ставк", "инфляц", "индекс", "brent", "usd/rub", "eur/rub", "офз", "купон", "доходност")
            )
            if not strict_hit:
                continue

        velocity = normalize_velocity(repeats, age_hours, has_time)
        confirmations = normalize_confirmations(num_links, int(repeats))

        features = {
            "financial": fin_score,
            "recency": recency,
            "velocity": velocity,
            "confirmations": confirmations,
            "source_rep": source_rep,
            "entities": entities_norm
        }

        hotness = compute_hotness(features)
        why_now = make_why_now(features)

        draft = ""
        if OPENROUTER_API_KEY:
            try:
                draft = await generate_draft_openrouter({
                    "text": text_for_score,
                    "links": inner_links,
                    "entities": entities
                })
            except Exception as e:
                draft = f"LLM error: {e}"

        headline = None
        if draft:
            first_line = draft.splitlines()[0].strip()
            if first_line:
                headline = first_line[:120]
        if not headline:
            txt = (text_for_score or "").strip()
            headline = (txt.split(".")[0] if "." in txt else txt)[:120] or None

        items_out.append({
            "headline": headline,
            "hotness": round(hotness, 4),
            "why_now": why_now,
            "entities": entities,
            "sources": (inner_links[:5]) if inner_links else ([url] if url else []),
            "timeline": [{"time": t_dt.isoformat(), "url": url}],
            "draft": draft,
            "dedup_group": it.get("dedup_group") or f"article:{url or (it.get('id') or '')}",
            "raw_item": it,
            "features": features
        })

    items_out.sort(key=lambda x: x["hotness"], reverse=True)
    top_items = items_out[:k]
    overall_summary = await generate_overall_summary_openrouter(top_items)

    return {
        "items": top_items,
        "generated_at": now.isoformat(),
        "k": k,
        "overall_summary": overall_summary
    }
