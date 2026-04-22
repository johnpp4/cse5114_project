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
    SNOWFLAKE_PRIVATE_KEY_PATH     path to your RSA private key (.p8), absolute or relative to project root
    or SNOWFLAKE_PRIVATE_KEY       PEM key content directly
    or SNOWFLAKE_PRIVATE_KEY_B64   Base64-encoded PEM key content
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  passphrase if key is encrypted (optional)
"""

from __future__ import annotations

import base64
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

import time

# for most recent recipes inserted into snowflake
_recent_cache: list[dict] = []
_recent_cache_time: float = 0
_CACHE_TTL = 30  # seconds

_REQUIRED_SNOWFLAKE_ENV = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)


def validate_snowflake_env() -> None:
    missing = [name for name in _REQUIRED_SNOWFLAKE_ENV if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing)
        )

    has_key_source = any(
        os.environ.get(name)
        for name in (
            "SNOWFLAKE_PRIVATE_KEY_B64",
            "SNOWFLAKE_PRIVATE_KEY",
            "SNOWFLAKE_PRIVATE_KEY_PATH",
        )
    )
    if not has_key_source:
        raise RuntimeError(
            "Missing Snowflake private key configuration. Set one of "
            "SNOWFLAKE_PRIVATE_KEY_B64, SNOWFLAKE_PRIVATE_KEY, or "
            "SNOWFLAKE_PRIVATE_KEY_PATH."
        )


def _load_private_key() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    key_pem_inline = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    key_pem_b64 = os.environ.get("SNOWFLAKE_PRIVATE_KEY_B64")
    passphrase_raw = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    passphrase = passphrase_raw.encode() if passphrase_raw else None

    if key_pem_b64:
        pem_data = base64.b64decode(key_pem_b64)
    elif key_pem_inline:
        pem_data = key_pem_inline.encode()
    elif key_path:
        candidate = Path(key_path).expanduser()
        if not candidate.is_absolute():
            project_root = Path(__file__).resolve().parent.parent
            candidate = project_root / candidate
        pem_data = candidate.read_bytes()
    else:
        raise KeyError(
            "Set one of SNOWFLAKE_PRIVATE_KEY_B64, SNOWFLAKE_PRIVATE_KEY, or "
            "SNOWFLAKE_PRIVATE_KEY_PATH."
        )
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
    conn = _get_connection()
    try:
        return _fetch_candidates(conn, user_phrases, cuisine, meal_type)
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _build_ilike_clause(phrases: list[str]) -> tuple[str, list[str]]:
    conditions = " OR ".join("ri.INGREDIENT_ID ILIKE %s" for _ in phrases)
    params = []
    for p in phrases:
        # replace spaces with underscores to match ingredient_id format
        slug = p.strip().lower().replace(" ", "_")
        params.append(f"%{slug}%")
    return conditions, params


def _fetch_candidates(
    conn: snowflake.connector.SnowflakeConnection,
    user_phrases: list[str],
    cuisine: str | None,
    meal_type: str | None,
) -> list[dict]:
    ilike_clause, params = _build_ilike_clause(user_phrases)
    total_phrases = len(user_phrases)

    scored_sql = f"""
        WITH matched AS (
            SELECT
                ri.RECIPE_ID,
                COUNT(DISTINCT ri.INGREDIENT_ID) as match_count
            FROM RECIPE_INGREDIENTS ri
            WHERE ({ilike_clause})
            GROUP BY ri.RECIPE_ID
        ),
        totals AS (
            SELECT
                RECIPE_ID,
                COUNT(DISTINCT INGREDIENT_ID) as total_count
            FROM RECIPE_INGREDIENTS
            GROUP BY RECIPE_ID
        )
        SELECT
            m.RECIPE_ID,
            m.match_count,
            t.total_count,
            ROUND(
                2.0 * (m.match_count / t.total_count) * (m.match_count / {total_phrases})
                / ((m.match_count / t.total_count) + (m.match_count / {total_phrases})),
                4
            ) as f1_score
        FROM matched m
        JOIN totals t ON m.RECIPE_ID = t.RECIPE_ID
        WHERE m.match_count >= LEAST(2, {total_phrases})
        ORDER BY f1_score DESC, m.match_count DESC
        LIMIT 50
    """

    cur = conn.cursor()
    cur.execute(scored_sql, params)
    rows = cur.fetchall()

    if not rows:
        return []

    candidate_ids = [row[0] for row in rows]
    id_placeholders = ", ".join("%s" for _ in candidate_ids)

    # fetch recipe metadata
    cur.execute(f"""
        SELECT RECIPE_ID, TITLE, LINK, SOURCE, PUBLISHED_AT
        FROM RECIPES
        WHERE RECIPE_ID IN ({id_placeholders})
    """, candidate_ids)

    recipes: dict[str, dict] = {
        row[0]: {
            "recipe_id":    row[0],
            "title":        row[1],
            "link":         row[2],
            "source":       row[3],
            "published_at": row[4],
            "ingredients":  [],
        }
        for row in cur.fetchall()
    }

    # fetch all ingredients for these recipes
    cur.execute(f"""
        SELECT RECIPE_ID, INGREDIENT_ID, RAW_TEXT
        FROM RECIPE_INGREDIENTS
        WHERE RECIPE_ID IN ({id_placeholders})
    """, candidate_ids)

    for recipe_id, ingredient_id, raw_text in cur:
        if recipe_id in recipes:
            recipes[recipe_id]["ingredients"].append({
                "ingredient_id": ingredient_id,
                "name":          ingredient_id.replace("ing_", "").replace("_", " "),
                "raw_text":      raw_text,
            })

    cur.close()
    return list(recipes.values())


def fetch_recent() -> list[dict]:
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT RECIPE_ID, TITLE, LINK, SOURCE, CREATED_AT
            FROM RECIPES
            ORDER BY CREATED_AT DESC
            LIMIT 20
        """)
        recipes = {
            row[0]: {
                "recipe_id":  row[0],
                "title":      row[1],
                "link":       row[2],
                "source":     row[3],
                "created_at": str(row[4]) if row[4] else None,
                "ingredients": [],
            }
            for row in cur.fetchall()
        }

        if recipes:
            id_placeholders = ", ".join("%s" for _ in recipes)
            cur.execute(f"""
                SELECT RECIPE_ID, INGREDIENT_ID
                FROM RECIPE_INGREDIENTS
                WHERE RECIPE_ID IN ({id_placeholders})
            """, list(recipes.keys()))

            for recipe_id, ingredient_id in cur:
                if recipe_id in recipes:
                    recipes[recipe_id]["ingredients"].append(
                        ingredient_id.replace("ing_", "").replace("_", " ")
                    )

        return list(recipes.values())
    finally:
        conn.close()