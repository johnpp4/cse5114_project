"""
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
_SCHEMA_CACHE: dict[str, set[str]] = {}
_MAX_CANDIDATES = int(os.environ.get("RECOMMEND_MAX_CANDIDATES", "10000"))


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


# public API

def fetch_candidates(user_phrases: list[str]) -> list[dict]:
    conn = _get_connection()
    try:
        return _fetch_candidates(conn, user_phrases)
    finally:
        conn.close()

# internal

def _build_ilike_clause(phrases: list[str]) -> tuple[str, list[str]]:
    conditions = " OR ".join("ri.INGREDIENT_ID ILIKE %s" for _ in phrases)
    # INGREDIENT_ID usually uses underscores, so normalize user spaces
    params = [f"%{p.replace(' ', '_')}%" for p in phrases]
    return conditions, params


def _get_table_columns(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
) -> set[str]:
    key = table_name.upper()
    cached = _SCHEMA_CACHE.get(key)
    if cached is not None:
        return cached
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS IN TABLE {key}")
        cols = {row[2].upper() for row in cur.fetchall()}
        _SCHEMA_CACHE[key] = cols
        return cols
    finally:
        cur.close()


def _fetch_candidates(
    conn: snowflake.connector.SnowflakeConnection,
    user_phrases: list[str],
) -> list[dict]:
    def choose_column(available: set[str], candidates: tuple[str, ...]) -> str | None:
        for name in candidates:
            if name in available:
                return name
        return None

    ilike_clause, params = _build_ilike_clause(user_phrases)
    total_phrases = len(user_phrases)

    # Use a subquery for candidate recipe IDs instead of a huge IN (...) list.
    # This avoids Snowflake's expression limit when many recipes match.
    candidate_subquery = f"""
        SELECT ri.RECIPE_ID
        FROM RECIPE_INGREDIENTS ri
        WHERE ({ilike_clause})
        GROUP BY ri.RECIPE_ID
        ORDER BY COUNT(*) DESC
        LIMIT {_MAX_CANDIDATES}
    """

    cur = conn.cursor()

    # fetch full recipe metadata for candidates only
    recipe_cols = _get_table_columns(conn, "RECIPES")
    title_col = choose_column(recipe_cols, ("TITLE", "RECIPE_NAME", "NAME"))
    rating_col = choose_column(recipe_cols, ("RATING", "AVG_RATING", "STARS"))
    link_col = choose_column(recipe_cols, ("LINK", "URL", "SOURCE_URL", "RECIPE_URL", "HREF"))
    source_col = choose_column(recipe_cols, ("SOURCE", "FOOD_JOURNAL", "PUBLISHER", "SITE_NAME"))

    title_expr = f'"{title_col}"' if title_col else "TO_VARCHAR(RECIPE_ID)"
    rating_expr = f'"{rating_col}"' if rating_col else "NULL"
    link_expr = f'"{link_col}"' if link_col else "NULL"
    source_expr = f'"{source_col}"' if source_col else "NULL"

    cur.execute(f"""
        SELECT RECIPE_ID, {title_expr} AS TITLE, {rating_expr} AS RATING, {link_expr} AS LINK, {source_expr} AS SOURCE
        FROM   RECIPES
        WHERE  RECIPE_ID IN ({candidate_subquery})
    """, params)

    recipes: dict[str, dict] = {
        row[0]: {
            "recipe_id":   row[0],
            "title":       row[1],
            "rating":      float(row[2]) if row[2] is not None else None,
            "link":        row[3],
            "source":      row[4],
            "ingredients": [],
        }
        for row in cur.fetchall()
    }

    if not recipes:
        cur.close()
        return []

    # fetch ALL ingredients for candidate recipes
    # engine.py needs the full ingredient list to compute match score
    # (matched vs missing), not just the ones that matched the search phrase.
    ingredient_cols = _get_table_columns(conn, "RECIPE_INGREDIENTS")
    quantity_col = choose_column(
        ingredient_cols,
        ("QUANTITY_GRAMS", "QUANTITY", "AMOUNT_GRAMS", "AMOUNT"),
    )
    raw_text_col = choose_column(
        ingredient_cols,
        ("RAW_TEXT", "INGREDIENT_TEXT", "RAW_INGREDIENT"),
    )
    quantity_expr = f'"{quantity_col}"' if quantity_col else "NULL"
    raw_text_expr = f'"{raw_text_col}"' if raw_text_col else "NULL"

    cur.execute(f"""
        SELECT RECIPE_ID, INGREDIENT_ID, {quantity_expr} AS QUANTITY_GRAMS, {raw_text_expr} AS RAW_TEXT
        FROM   RECIPE_INGREDIENTS
        WHERE  RECIPE_ID IN ({candidate_subquery})
    """, params)

    for recipe_id, ingredient_id, quantity_grams, raw_text in cur:
        if recipe_id not in recipes:
            continue
        recipes[recipe_id]["ingredients"].append({
            "ingredient_id":  ingredient_id,
            "name":           ingredient_id.replace("ing_", "").replace("_", " "),
            "quantity_grams": float(quantity_grams) if quantity_grams is not None else None,
            "raw_text":       raw_text,
        })

    cur.close()
    return list(recipes.values())


def fetch_recent() -> list[dict]:
    conn = _get_connection()
    try:
        cur = conn.cursor()
        recipe_cols = _get_table_columns(conn, "RECIPES")
        required_cols = {"TITLE", "LINK", "SOURCE", "CREATED_AT"}
        missing = sorted(required_cols - recipe_cols)
        if missing:
            raise RuntimeError(
                "RECIPES table missing required columns for recent feed: "
                + ", ".join(missing)
            )

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