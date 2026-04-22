"""
Ingredient overlap scoring and filters for recipe recommendations.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Optional


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def parse_user_ingredients(text: str) -> list[str]:
    """
    Prefer **one ingredient per line** (clearest for users; avoids commas inside
    quantities like "1/2 cup milk, room temperature" being split wrong).

    If the user types only **one line**, commas/semicolons still split multiple
    ingredients so paragraph-style paste still works.
    """
    if not (text or "").strip():
        return []

    lines = [_norm(line) for line in text.splitlines()]
    non_empty = [ln for ln in lines if ln]
    if not non_empty:
        return []

    if len(non_empty) > 1:
        return non_empty

    only = non_empty[0]
    if re.search(r"[,;]", only):
        return [p for p in (_norm(x) for x in re.split(r"[,;]+", only)) if p]
    return [only]


def _tokens(s: str) -> set[str]:
    return {t for t in re.split(r"\W+", s) if len(t) > 1}


def ingredient_matches_user(recipe_ing_name: str, user_phrases: list[str]) -> bool:
    r = _norm(recipe_ing_name)
    if not r:
        return False
    r_words = _tokens(r)

    for u in user_phrases:
        if not u:
            continue
        u_norm = _norm(u)

        # Avoid noisy one-letter artifacts from source data (e.g. "i").
        if len(r) < 2 or len(u_norm) < 2:
            continue
        if u_norm == r:
            return True

        # Substring matching is useful for "olive oil" vs "extra virgin olive oil",
        # but too permissive for very short tokens.
        if len(r) >= 3 and len(u_norm) >= 3 and (u_norm in r or r in u_norm):
            return True

        # word boundary match — prevents "water" matching "watermelon"
        if re.search(rf'\b{re.escape(u_norm)}\b', r):
            return True

        # all user tokens must appear in recipe ingredient
        uw = _tokens(u_norm)
        if uw and uw.issubset(r_words):
            return True

        # fuzzy match
        if SequenceMatcher(None, u_norm, r).ratio() >= 0.82:
            return True

    return False


@dataclass
class ScoredRecipe:
    recipe: dict[str, Any]
    match_score: float
    matched_ingredient_names: list[str]
    missing_ingredient_names: list[str]


def score_recipe(recipe: dict[str, Any], user_phrases: list[str]) -> ScoredRecipe:
    ings: list[dict[str, Any]] = recipe.get("ingredients") or []
    names = [_norm(i.get("name", "")) for i in ings if _norm(i.get("name", ""))]
    matched: list[str] = []
    missing: list[str] = []
    for n in names:
        if ingredient_matches_user(n, user_phrases):
            matched.append(n)
        else:
            missing.append(n)
    total = max(len(names), 1)
    overlap_ratio = len(matched) / total

    # Also score how much of the user's requested list this recipe covers.
    # This prevents many tiny recipes from clustering at 100% overlap.
    covered_phrases = 0
    for phrase in user_phrases:
        if any(ingredient_matches_user(n, [phrase]) for n in names):
            covered_phrases += 1
    phrase_coverage = covered_phrases / max(len(user_phrases), 1)
    combined_score = (0.6 * overlap_ratio) + (0.4 * phrase_coverage)

    return ScoredRecipe(
        recipe=recipe,
        match_score=round(combined_score, 4),
        matched_ingredient_names=matched,
        missing_ingredient_names=missing,
    )


def filter_by_cuisine_meal(
    recipes: Iterable[dict[str, Any]],
    cuisine: Optional[str],
    meal_type: Optional[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    c = _norm(cuisine) if cuisine else ""
    m = _norm(meal_type) if meal_type else ""
    for r in recipes:
        if c and c not in _norm(str(r.get("cuisine") or "")):
            continue
        if m and m not in _norm(str(r.get("meal_type") or "")):
            continue
        out.append(r)
    return out


def recommend(
    recipes: list[dict[str, Any]],
    user_phrases: list[str],
    *,
    cuisine: Optional[str] = None,
    meal_type: Optional[str] = None,
    limit: int = 30,
    min_score: float = 0.0,
) -> list[ScoredRecipe]:
    pool = filter_by_cuisine_meal(recipes, cuisine, meal_type)
    scored = [score_recipe(r, user_phrases) for r in pool]
    scored = [s for s in scored if s.match_score >= min_score]
    scored.sort(
        key=lambda s: (-s.match_score, -(s.recipe.get("rating") or 0), s.recipe.get("title", "")),
    )
    return scored[:limit]


def load_recipes_json(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    recipes = data.get("recipes")
    if not isinstance(recipes, list):
        raise ValueError("JSON must contain a 'recipes' array")
    return recipes
