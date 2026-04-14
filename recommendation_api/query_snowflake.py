"""
snowflake_loader.py — per-request ingredient-filtered queries against Snowflake.

Instead of loading all recipes at startup, each recommendation request sends
the user's ingredients to Snowflake and retrieves only the recipes that contain
at least one match. Snowflake does the JOIN and filtering; Python only scores
and ranks the smaller result set.

Required environment variables (matches .env in repo root):
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_PRIVATE_KEY_PATH     absolute path to your RSA private key (.p8)
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  passphrase if key is encrypted (optional)
"""

from __future__ import annotations

import os
from pathlib import Path
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)
from cryptography.hazmat.backends import default_backend

import snowflake.connector
from snowflake.connector import DictCursor


def _load_private_key() -> bytes:
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase_raw = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    passphrase = passphrase_raw.encode() if passphrase_raw else None

    pem_data = Path(key_path).read_bytes()
    private_key = load_pem_private_key(pem_data, password=passphrase, backend=default_backend())

    return private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def _get_connection() -> snowflake.connector.SnowflakeConnection:
    connect_kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        connect_kwargs["role"] = role
    return snowflake.connector.connect(**connect_kwargs)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_candidates(
    user_phrases: list[str],
    cuisine: str | None = None,
    meal_type: str | None = None,
) -> list[dict]:
    """
    Query Snowflake for recipes that contain at least one ingredient matching
    any of the user's phrases, then fetch all ingredients for those recipes so
    engine.py can compute the full match score.

    Returns a list of recipe dicts shaped identically to what load_recipes_json()
    previously produced, so engine.py::recommend() requires no changes.
    """
    conn = _get_connection()
    try:
        return _fetch_candidates(conn, user_phrases, cuisine, meal_type)
    finally:
        conn.close()


def fetch_filters() -> dict:
    """Return distinct cuisine and meal_type values for the filter dropdowns."""
    conn = _get_connection()
    try:
        cur = conn.cursor(DictCursor)
        cur.execute("""
            SELECT
                ARRAY_AGG(DISTINCT CUISINE)   AS cuisines,
                ARRAY_AGG(DISTINCT MEAL_TYPE) AS meal_types
            FROM RECIPES
            WHERE CUISINE IS NOT NULL
            AND MEAL_TYPE IS NOT NULL
        """)
        row = cur.fetchone()
        return {
            "cuisines":   sorted(row["CUISINES"]   or []),
            "meal_types": sorted(row["MEAL_TYPES"] or []),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _build_ilike_clause(phrases: list[str]) -> tuple[str, list[str]]:
    """
    Build a parameterised OR chain of ILIKE conditions against INGREDIENT_ID.
    Returns the SQL fragment and the corresponding parameter list.

    e.g. phrases = ["garlic", "butter"]
    =>  "ri.INGREDIENT_ID ILIKE %s OR ri.INGREDIENT_ID ILIKE %s"
        ["%garlic%", "%butter%"]

    INGREDIENT_ID values follow the pattern "ing_<name>" (e.g. "ing_garlic"),
    so a substring ILIKE match is correct here.
    """
    conditions = " OR ".join("ri.INGREDIENT_ID ILIKE %s" for _ in phrases)
    params = [f"%{p}%" for p in phrases]
    return conditions, params


def _fetch_candidates(
    conn: snowflake.connector.SnowflakeConnection,
    user_phrases: list[str],
    cuisine: str | None,
    meal_type: str | None,
) -> list[dict]:
    ilike_clause, params = _build_ilike_clause(user_phrases)

    # Optional cuisine / meal_type filters appended as parameterised clauses
    cuisine_clause   = "AND LOWER(r.CUISINE)   = LOWER(%s)" if cuisine   else ""
    meal_type_clause = "AND LOWER(r.MEAL_TYPE) = LOWER(%s)" if meal_type else ""
    if cuisine:
        params.append(cuisine)
    if meal_type:
        params.append(meal_type)

    # --- Step 1: find recipe_ids that have at least one matching ingredient ---
    # Narrows the candidate set in Snowflake before any data crosses the network.
    candidate_sql = f"""
        SELECT DISTINCT ri.RECIPE_ID
        FROM   RECIPE_INGREDIENTS ri
        JOIN   RECIPES r ON r.RECIPE_ID = ri.RECIPE_ID
        WHERE  ({ilike_clause})
        {cuisine_clause}
        {meal_type_clause}
    """

    cur = conn.cursor()
    cur.execute(candidate_sql, params)
    candidate_ids = [row[0] for row in cur.fetchall()]

    if not candidate_ids:
        return []

    # --- Step 2: fetch full recipe metadata for candidates only ---
    id_placeholders = ", ".join("%s" for _ in candidate_ids)
    cur.execute(f"""
        SELECT RECIPE_ID, TITLE, CUISINE, MEAL_TYPE, RATING, LINK
        FROM   RECIPES
        WHERE  RECIPE_ID IN ({id_placeholders})
    """, candidate_ids)

    recipes: dict[str, dict] = {
        row[0]: {
            "recipe_id":   row[0],
            "title":       row[1],
            "cuisine":     row[2],
            "meal_type":   row[3],
            "rating":      float(row[4]) if row[4] is not None else None,
            "link":        row[5],
            "ingredients": [],
        }
        for row in cur.fetchall()
    }

    # --- Step 3: fetch ALL ingredients for candidate recipes ---
    # engine.py needs the full ingredient list to compute match score
    # (matched vs missing), not just the ones that matched the search phrase.
    cur.execute(f"""
        SELECT RECIPE_ID, INGREDIENT_ID, QUANTITY_GRAMS, RAW_TEXT
        FROM   RECIPE_INGREDIENTS
        WHERE  RECIPE_ID IN ({id_placeholders})
    """, candidate_ids)

    for recipe_id, ingredient_id, quantity_grams, raw_text in cur:
        recipes[recipe_id]["ingredients"].append({
            "ingredient_id":  ingredient_id,
            "name":           ingredient_id.replace("ing_", "").replace("_", " "),
            "quantity_grams": float(quantity_grams) if quantity_grams is not None else None,
            "raw_text":       raw_text,
        })

    cur.close()
    return list(recipes.values())