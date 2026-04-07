import pandas as pd
import hashlib
import ast
import re
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tqdm import tqdm

# configuration
CSV_PATH = "/Users/cassiephan/Downloads/archive/RecipeNLG_dataset.csv"
FILTER_SOURCE = "Gathered"

OUTPUT_RECIPES          = "out_recipes.csv"
OUTPUT_INGREDIENTS      = "out_ingredients.csv"
OUTPUT_RECIPE_INGREDS   = "out_recipe_ingredients.csv"

# snowflake connection
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            recipe_id   VARCHAR PRIMARY KEY,
            title       VARCHAR,
            rating      FLOAT,
            link        VARCHAR,
            created_at  VARCHAR
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id VARCHAR PRIMARY KEY,
            name          VARCHAR
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_id       VARCHAR,
            ingredient_id   VARCHAR,
            quantity_grams  FLOAT,
            raw_text        VARCHAR,
            PRIMARY KEY (recipe_id, ingredient_id)
        )
    """)
    cursor.close()
    print("Tables created (or already exist).")

def upload_to_snowflake(conn, df, table_name):
    # write_pandas requires uppercase column names
    df.columns = [c.upper() for c in df.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
    if success:
        print(f"  Uploaded {nrows:,} rows to {table_name.upper()}")
    else:
        print(f"  Failed to upload {table_name}")


# helpers
def make_recipe_id(title: str, link: str) -> str:
    base = (title + link).encode("utf-8")
    return "rec_" + hashlib.md5(base).hexdigest()[:10]

def make_ingredient_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return "ing_" + slug

def parse_list_field(val) -> list:
    if isinstance(val, list):
        return val
    try:
        return ast.literal_eval(val)
    except Exception:
        return []

def canonicalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())

UNIT_TO_GRAMS = {
    "c": 240, "cup": 240, "cups": 240,
    "tbsp": 15, "tablespoon": 15, "tablespoons": 15,
    "tsp": 5,  "teaspoon": 5,   "teaspoons": 5,
    "oz": 28,  "ounce": 28,     "ounces": 28,
    "lb": 454, "pound": 454,    "pounds": 454,
    "g": 1,    "gram": 1,       "grams": 1,
    "kg": 1000,
    "l": 1000, "liter": 1000,
    "ml": 1,   "milliliter": 1,
}

FRACTION_MAP = {"½": 0.5, "¼": 0.25, "¾": 0.75, "⅓": 0.333, "⅔": 0.667}

def parse_quantity_grams(raw_text: str):
    text = raw_text.lower().strip()
    for sym, val in FRACTION_MAP.items():
        text = text.replace(sym, str(val))
    m = re.match(r"(\d+(?:\.\d+)?)\s*(?:/\s*(\d+))?\s*([a-z\.]+)?", text)
    if not m:
        return None
    whole = float(m.group(1))
    if m.group(2):
        whole = whole / float(m.group(2))
    unit = (m.group(3) or "").rstrip(".")
    multiplier = UNIT_TO_GRAMS.get(unit)
    if multiplier is None:
        return None
    return round(whole * multiplier, 2)


# load and clean data
print("Loading CSV...")
df = pd.read_csv(CSV_PATH)
print(f"  Raw rows: {len(df):,}")

df = df[df["source"] == FILTER_SOURCE].copy()
print(f"  After source filter: {len(df):,}")

df = df.dropna(subset=["title", "link", "ingredients"])
df["ingredients_list"] = df["ingredients"].apply(parse_list_field)
df["ner_list"]         = df["NER"].apply(parse_list_field)
df = df[df["ingredients_list"].map(len) > 0]

df["recipe_id"] = df.apply(lambda r: make_recipe_id(r["title"], r["link"]), axis=1)
df = df.drop_duplicates(subset="recipe_id")
print(f"  After dedup: {len(df):,}")


# transform data

recipes_rows        = []
ingredients_map     = {}
recipe_ingreds_rows = []

print("Transforming...")
for _, row in tqdm(df.iterrows(), total=len(df)):
    rid       = row["recipe_id"]
    raw_ings  = row["ingredients_list"]
    ner_names = row["ner_list"]
    paired    = list(zip(raw_ings, ner_names))
    if not paired:
        continue

    recipes_rows.append({
        "recipe_id": rid,
        "title":     row["title"].strip(),
        "cuisine":   "unknown",
        "meal_type": "unknown",
        "rating":    None,
        "link":      row["link"].strip(),
        "created_at": pd.Timestamp.utcnow().isoformat(),
    })

    seen_in_recipe = set()
    for raw_text, ner_name in paired:
        canonical = canonicalize(ner_name)
        if not canonical:
            continue
        iid = make_ingredient_id(canonical)

        if iid not in ingredients_map:
            ingredients_map[iid] = {"ingredient_id": iid, "name": canonical}

        if iid not in seen_in_recipe:
            seen_in_recipe.add(iid)
            recipe_ingreds_rows.append({
                "recipe_id":      rid,
                "ingredient_id":  iid,
                "quantity_grams": parse_quantity_grams(raw_text),
                "raw_text":       raw_text.strip(),
            })


# write to csvs
print("Writing CSVs...")

recipes_df    = pd.DataFrame(recipes_rows)
ingredients_df = pd.DataFrame(list(ingredients_map.values()))
recipe_ingreds_df = pd.DataFrame(recipe_ingreds_rows)

recipes_df.to_csv(OUTPUT_RECIPES, index=False)
print(f"  {OUTPUT_RECIPES}: {len(recipes_rows):,} rows")

ingredients_df.to_csv(OUTPUT_INGREDIENTS, index=False)
print(f"  {OUTPUT_INGREDIENTS}: {len(ingredients_map):,} rows")

recipe_ingreds_df.to_csv(OUTPUT_RECIPE_INGREDS, index=False)
print(f"  {OUTPUT_RECIPE_INGREDS}: {len(recipe_ingreds_rows):,} rows")


# upload to snowflake
print("\nConnecting to Snowflake...")
conn = get_snowflake_conn()

create_tables(conn)

print("Uploading tables...")
upload_to_snowflake(conn, recipes_df.copy(), "recipes")
upload_to_snowflake(conn, ingredients_df.copy(), "ingredients")
upload_to_snowflake(conn, recipe_ingreds_df.copy(), "recipe_ingredients")

conn.close()
print("\nDone.")