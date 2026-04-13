"""
Leftover to Makeover — recommendation REST API + static UI.
Run: uvicorn recommendation_api.main:app --reload --app-dir ..
Or from repo root: uvicorn recommendation_api.main:app --reload
"""

from __future__ import annotations

import os
from pathlib import Path

from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .engine import load_recipes_json, parse_user_ingredients, recommend

ROOT = Path(__file__).resolve().parent
DATA_PATH = Path(os.environ.get("RECIPES_DATA_PATH", str(ROOT / "data" / "recipes_demo.json")))
STATIC_DIR = ROOT / "static"

_RECIPES_CACHE: Optional[list] = None


def get_recipes() -> list:
    global _RECIPES_CACHE
    if _RECIPES_CACHE is None:
        if not DATA_PATH.exists():
            raise FileNotFoundError(f"Recipe data not found: {DATA_PATH}")
        _RECIPES_CACHE = load_recipes_json(DATA_PATH)
    return _RECIPES_CACHE


app = FastAPI(title="Leftover to Makeover API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RecommendBody(BaseModel):
    ingredients: str = Field(..., description="Comma-separated ingredients you have")
    cuisine: Optional[str] = None
    meal_type: Optional[str] = None
    limit: int = Field(25, ge=1, le=100)
    min_score: float = Field(0.0, ge=0.0, le=1.0)


def _scored_to_dict(s):
    r = s.recipe
    return {
        "recipe_id": r.get("recipe_id"),
        "title": r.get("title"),
        "cuisine": r.get("cuisine"),
        "meal_type": r.get("meal_type"),
        "rating": r.get("rating"),
        "link": r.get("link"),
        "match_score": s.match_score,
        "matched_ingredients": s.matched_ingredient_names,
        "missing_ingredients": s.missing_ingredient_names,
        "ingredient_count": len(r.get("ingredients") or []),
    }


@app.get("/api/health")
def health():
    return {"status": "ok", "data_path": str(DATA_PATH), "recipe_count": len(get_recipes())}


@app.get("/api/filters")
def filters():
    recipes = get_recipes()
    cuisines = sorted({str(r.get("cuisine") or "").strip() for r in recipes if r.get("cuisine")})
    meals = sorted({str(r.get("meal_type") or "").strip() for r in recipes if r.get("meal_type")})
    return {"cuisines": [c for c in cuisines if c], "meal_types": [m for m in meals if m]}


@app.get("/api/recommend")
def recommend_get(
    q: str = Query(..., description="Ingredients, comma-separated"),
    cuisine: Optional[str] = None,
    meal_type: Optional[str] = None,
    limit: int = Query(25, ge=1, le=100),
    min_score: float = Query(0.0, ge=0.0, le=1.0),
):
    phrases = parse_user_ingredients(q)
    if not phrases:
        raise HTTPException(400, "Provide at least one ingredient in `q`")
    try:
        recipes = get_recipes()
    except FileNotFoundError as e:
        raise HTTPException(500, str(e)) from e
    scored = recommend(
        recipes,
        phrases,
        cuisine=cuisine or None,
        meal_type=meal_type or None,
        limit=limit,
        min_score=min_score,
    )
    return {
        "query_parsed": phrases,
        "results": [_scored_to_dict(s) for s in scored],
    }


@app.post("/api/recommend")
def recommend_post(body: RecommendBody):
    phrases = parse_user_ingredients(body.ingredients)
    if not phrases:
        raise HTTPException(400, "Provide at least one ingredient")
    try:
        recipes = get_recipes()
    except FileNotFoundError as e:
        raise HTTPException(500, str(e)) from e
    scored = recommend(
        recipes,
        phrases,
        cuisine=body.cuisine,
        meal_type=body.meal_type,
        limit=body.limit,
        min_score=body.min_score,
    )
    return {
        "query_parsed": phrases,
        "results": [_scored_to_dict(s) for s in scored],
    }


@app.get("/")
def serve_ui():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(404, "UI not built — missing static/index.html")
    return FileResponse(index)


app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")
