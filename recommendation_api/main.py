"""
Leftover to Makeover — recommendation REST API + static UI.

Reads credentials from .env (or environment):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_PRIVATE_KEY_PATH
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

try:
    from confluent_kafka import Consumer
except ImportError:
    Consumer = None

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .engine import parse_user_ingredients, recommend
from .query_snowflake import fetch_candidates, validate_snowflake_env, fetch_recent

from dotenv import load_dotenv
import asyncio
import json

load_dotenv()

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
LINK_CHECK_TIMEOUT_S = float(os.environ.get("LINK_CHECK_TIMEOUT_S", "3"))
LINK_CHECK_CACHE_TTL_S = int(os.environ.get("LINK_CHECK_CACHE_TTL_S", "3600"))
_LINK_OK_CACHE: dict[str, tuple[float, bool]] = {}
ENABLE_LIVE_LINK_CHECK = os.environ.get("ENABLE_LIVE_LINK_CHECK", "false").lower() == "true"
QUERY_CACHE_TTL_S = int(os.environ.get("QUERY_CACHE_TTL_S", "120"))
QUERY_CACHE_MAX_RESULTS = int(os.environ.get("QUERY_CACHE_MAX_RESULTS", "500"))
_QUERY_RESULTS_CACHE: dict[tuple[tuple[str, ...], float], tuple[float, list[dict]]] = {}

connected_clients: set = set()
ENABLE_KAFKA = os.environ.get("ENABLE_KAFKA", "false").lower() == "true"

app = FastAPI(title="Leftover to Makeover API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

consumer = None
if ENABLE_KAFKA and Consumer is not None:
    # Listen to kafka topic for updates in real time.
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "recipe-ui",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["recipes_processed"])
else:
    logger.info("Realtime Kafka updates disabled.")

def kafka_listener(loop: asyncio.AbstractEventLoop):
    if consumer is None:
        return
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        event = json.loads(msg.value().decode("utf-8"))
        asyncio.run_coroutine_threadsafe(
            broadcast_new_recipes([event]),
            loop
        )


@app.on_event("startup")
async def on_startup():
    validate_snowflake_env()
    if consumer is not None:
        loop = asyncio.get_event_loop()  # get loop here in async context
        asyncio.create_task(asyncio.to_thread(kafka_listener, loop))

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class RecommendBody(BaseModel):
    ingredients: str = Field(..., description="Comma-separated ingredients you have")
    limit: int = Field(10, ge=1, le=100)
    offset: int = Field(0, ge=0)
    min_score: float = Field(0.0, ge=0.0, le=1.0)


def _is_url_reachable(url: str) -> bool:
    now = time.time()
    cached = _LINK_OK_CACHE.get(url)
    if cached and (now - cached[0] <= LINK_CHECK_CACHE_TTL_S):
        return cached[1]

    ok = False
    headers = {"User-Agent": "Mozilla/5.0 LeftoverToMakeover/1.0"}
    try:
        req = Request(url, method="HEAD", headers=headers)
        with urlopen(req, timeout=LINK_CHECK_TIMEOUT_S) as resp:
            ok = 200 <= getattr(resp, "status", 200) < 400
    except HTTPError as exc:
        # Some sites block HEAD; retry with GET when method is not allowed.
        if exc.code == 405:
            try:
                req = Request(url, method="GET", headers=headers)
                with urlopen(req, timeout=LINK_CHECK_TIMEOUT_S) as resp:
                    ok = 200 <= getattr(resp, "status", 200) < 400
            except (HTTPError, URLError, TimeoutError, ValueError):
                ok = False
        else:
            ok = False
    except (URLError, TimeoutError, ValueError):
        ok = False

    _LINK_OK_CACHE[url] = (now, ok)
    return ok


def _scored_to_dict_or_none(s):
    r = s.recipe
    title = str(r.get("title") or "recipe")
    search_link = f"https://www.google.com/search?q={quote_plus(title + ' recipe')}"
    raw_link = str(r.get("link") or "").strip()
    if raw_link and not raw_link.lower().startswith(("http://", "https://")):
        raw_link = f"https://{raw_link}"
    # By default, skip live URL probing for performance; it can be enabled via env.
    if not raw_link:
        return None
    if ENABLE_LIVE_LINK_CHECK and not _is_url_reachable(raw_link):
        return None
    return {
        "recipe_id":            r.get("recipe_id"),
        "title":                title,
        "rating":               r.get("rating"),
        "link":                 raw_link,
        "source":               r.get("source"),
        "search_link":          search_link,
        "match_score":          s.match_score,
        "matched_ingredients":  s.matched_ingredient_names,
        "missing_ingredients":  s.missing_ingredient_names,
        "ingredient_count":     len(r.get("ingredients") or []),
    }


def _paginate_scored_results(scored: list, *, offset: int, limit: int) -> tuple[list[dict], bool]:
    results: list[dict] = []
    seen_valid = 0
    has_more = False
    for s in scored:
        item = _scored_to_dict_or_none(s)
        if item is None:
            continue
        if seen_valid < offset:
            seen_valid += 1
            continue
        if len(results) < limit:
            results.append(item)
            seen_valid += 1
            continue
        has_more = True
        break
    return results, has_more


def _cache_key(phrases: list[str], min_score: float) -> tuple[tuple[str, ...], float]:
    return (tuple(phrases), round(float(min_score), 4))


def _get_cached_results(phrases: list[str], min_score: float) -> list[dict] | None:
    key = _cache_key(phrases, min_score)
    cached = _QUERY_RESULTS_CACHE.get(key)
    if not cached:
        return None
    ts, results = cached
    if time.time() - ts > QUERY_CACHE_TTL_S:
        _QUERY_RESULTS_CACHE.pop(key, None)
        return None
    return results


def _set_cached_results(phrases: list[str], min_score: float, results: list[dict]) -> None:
    key = _cache_key(phrases, min_score)
    _QUERY_RESULTS_CACHE[key] = (time.time(), results[:QUERY_CACHE_MAX_RESULTS])


def _filter_search_phrases(phrases: list[str]) -> list[str]:
    # Ignore very short/noisy terms to avoid broad accidental matches.
    return [p for p in phrases if len((p or "").strip()) >= 3]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/api/recent-recipes")
def recent_recipes():
    try:
        return {"results": fetch_recent()}
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/recommend")
def recommend_get(
    q: str = Query(..., description="Ingredients, comma-separated"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    min_score: float = Query(0.0, ge=0.0, le=1.0),
):
    phrases = parse_user_ingredients(q)
    if not phrases:
        raise HTTPException(400, "Provide at least one ingredient in `q`")
    search_phrases = _filter_search_phrases(phrases)
    if not search_phrases:
        return {"query_parsed": phrases, "results": [], "has_more": False}

    cached = _get_cached_results(search_phrases, min_score)
    if cached is not None:
        page = cached[offset: offset + limit]
        has_more = (offset + len(page)) < len(cached)
        return {"query_parsed": phrases, "results": page, "has_more": has_more}

    try:
        candidates = fetch_candidates(search_phrases)
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc

    # Build and cache a reusable result pool for fast paging.
    scanned_limit = QUERY_CACHE_MAX_RESULTS
    scored = recommend(candidates, search_phrases, limit=scanned_limit, min_score=min_score)
    all_results, _ = _paginate_scored_results(scored, offset=0, limit=QUERY_CACHE_MAX_RESULTS)
    _set_cached_results(search_phrases, min_score, all_results)
    page = all_results[offset: offset + limit]
    has_more = (offset + len(page)) < len(all_results)
    return {"query_parsed": phrases, "results": page, "has_more": has_more}


@app.post("/api/recommend")
def recommend_post(body: RecommendBody):
    phrases = parse_user_ingredients(body.ingredients)
    if not phrases:
        raise HTTPException(400, "Provide at least one ingredient")
    search_phrases = _filter_search_phrases(phrases)
    if not search_phrases:
        return {"query_parsed": phrases, "results": [], "has_more": False}

    cached = _get_cached_results(search_phrases, body.min_score)
    if cached is not None:
        page = cached[body.offset: body.offset + body.limit]
        has_more = (body.offset + len(page)) < len(cached)
        return {"query_parsed": phrases, "results": page, "has_more": has_more}

    try:
        candidates = fetch_candidates(search_phrases)
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc

    scored = recommend(candidates, search_phrases, limit=QUERY_CACHE_MAX_RESULTS, min_score=body.min_score)
    all_results, _ = _paginate_scored_results(scored, offset=0, limit=QUERY_CACHE_MAX_RESULTS)
    _set_cached_results(search_phrases, body.min_score, all_results)
    page = all_results[body.offset: body.offset + body.limit]
    has_more = (body.offset + len(page)) < len(all_results)
    return {"query_parsed": phrases, "results": page, "has_more": has_more}

@app.websocket("/ws/new-recipes")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # blocks until client sends or disconnects
    except Exception:
        connected_clients.discard(websocket)

# this is called evertime a new recipe is detected in snowflake
async def broadcast_new_recipes(recipes: list[dict]):
    if not recipes or not connected_clients:
        return

    msg = json.dumps({
        "type": "new_recipes",
        "recipes": recipes
    })

    for ws in connected_clients.copy():
        try:
            await ws.send_text(msg)
        except Exception:
            connected_clients.discard(ws)

@app.get("/")
def serve_ui():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(404, "UI not built — missing static/index.html")
    return FileResponse(index)


app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")