"""
RSS Ingestion Script:
polls multiple RSS feeds, validates each entry is a real recipe by scraping the page with recipe-scrapers, 
then pushes structured events to the Kafka topic `recipes_raw`.

Dependencies:
    pip install feedparser kafka-python recipe-scrapers ingredient-parser-nlp
"""

import json
import hashlib
import time
import logging
import re
from datetime import datetime, timezone

import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError
from recipe_scrapers import scrape_me
from recipe_scrapers._exceptions import SchemaOrgException, WebsiteNotImplementedError
from ingredient_parser import parse_ingredient as nlp_parse

# configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "recipes_raw"
POLL_INTERVAL_SECONDS = 15 # polls feeds every 15 seconds

RSS_FEEDS = [
    {"source": "RecipeTin Eats",     "url": "https://www.recipetineats.com/feed"},
    {"source": "Budget Bytes",       "url": "https://www.budgetbytes.com/feed"},
    {"source": "Minimalist Baker",   "url": "https://minimalistbaker.com/feed"},
    {"source": "Cookie and Kate",    "url": "https://cookieandkate.com/feed"},
    {"source": "Downshiftology",     "url": "https://downshiftology.com/feed"},
    {"source": "Well Plated",        "url": "https://www.wellplated.com/feed"},
    {"source": "Spend with Pennies", "url": "https://www.spendwithpennies.com/feed"},
    {"source": "Smitten Kitchen",    "url": "https://feeds.feedburner.com/smittenkitchen"},
    {"source": "Food Network",       "url": "https://www.foodnetwork.com/feeds/rss/content-feed.rss"},
    {"source": "BBC Good Food",      "url": "https://www.bbcgoodfood.com/recipes/feed"},
    {"source": "Bon Appetit",        "url": "https://www.bonappetit.com/feed/rss"},
    {"source": "Taste of Home",      "url": "https://www.tasteofhome.com/feed"},
    {"source": "Allrecipes",         "url": "https://www.allrecipes.com/feeds/allrecipesrss.aspx"},
    {"source": "Epicurious",         "url": "https://www.epicurious.com/services/rss/feeds/allrecipes.xml"},
    {"source": "Simply Recipes",     "url": "https://www.simplyrecipes.com/feed"},
    {"source": "Serious Eats",       "url": "https://www.seriouseats.com/feeds/all"},
]

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rss_ingestor")

# in-memory de-duplication
seen_ids: set[str] = set()

# kafka producer
producer = None

def get_producer() -> KafkaProducer:
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
        )
        logger.info("Kafka producer connected to %s", KAFKA_BROKER)
    return producer


# ID + normalization helpers (matches ETL script)
def normalize_link(link: str) -> str:
    return re.sub(r"^https?://", "", str(link)).strip("/")

def make_recipe_id(title: str, link: str) -> str:
    base = (title + normalize_link(link)).encode("utf-8")
    return "rec_" + hashlib.md5(base).hexdigest()[:10]

def make_ingredient_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return "ing_" + slug

def canonicalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())

# quantity parsing (matches ETL script)
UNIT_TO_GRAMS = {
    "c": 240, "cup": 240, "cups": 240,
    "tbsp": 15, "tablespoon": 15, "tablespoons": 15,
    "tsp": 5,  "teaspoon": 5,   "teaspoons": 5,
    "oz": 28,  "ounce": 28,     "ounces": 28,
    "lb": 454, "pound": 454,    "pounds": 454,
    "g": 1,    "gram": 1,       "grams": 1,
    "kg": 1000, "kilogram": 1000, "kilograms": 1000,
    "l": 1000, "liter": 1000, "liters": 1000,
    "ml": 1,   "milliliter": 1,  "milliliters": 1
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
    return round(whole * multiplier, 2) if multiplier else None

def parse_rating(raw) -> float | None:
    """Normalize rating to plain float regardless of source format."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return round(float(raw), 2)
    if isinstance(raw, str):
        m = re.search(r"(\d+(?:\.\d+)?)", raw)
        return round(float(m.group(1)), 2) if m else None
    return None

# ingredient normalization
def normalize_ingredients(raw_list: list[str]) -> list[dict]:
    """
    Converts raw scraped ingredient strings into the normalized format
    the ETL script produces — ingredient_id, name, quantity_grams, raw_text.
    """
    result = []
    for raw in raw_list:
        try:
            parsed = nlp_parse(raw)
            name = canonicalize(parsed.name.text) if parsed.name else ""
        except Exception:
            name = ""
        if not name:
            name = raw.strip().lower()
        iid = make_ingredient_id(name)
        result.append({
            "ingredient_id":  iid,
            "name":           name,
            "quantity_grams": parse_quantity_grams(raw),
            "raw_text":       raw.strip(),
        })
    return result

# scraping
def scrape_recipe(url: str) -> dict | None:
    """
    Attempt to scrape structured recipe data from the article URL.
    Returns None if the page has no recipe schema (i.e. it's an article).
    """
    try:
        scraper = scrape_me(url)
        ingredients = scraper.ingredients()
        if not ingredients or len(ingredients) < 2:
            return None
        return {
            "ingredients": ingredients,
            "rating":      scraper.ratings(),
        }
    except (SchemaOrgException, WebsiteNotImplementedError):
        return None
    except Exception as e:
        logger.debug("Scrape failed for %s: %s", url, e)
        return None

# build event
def get_rss_tags(entry) -> list[str]:
    return [t.get("term", "") for t in entry.get("tags", []) if t.get("term")]

def build_event(entry, feed_meta: dict, scraped: dict) -> dict:
    title = entry.get("title", "unknown").strip()
    link  = entry.get("link", "")

    published = None
    if entry.get("published_parsed"):
        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()

    return {
        "event_type": "new_recipe",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"),  # matches ETL format
        "recipe": {
            "recipe_id":   make_recipe_id(title, link),          # hashes normalized link
            "title":       title,
            "link":        normalize_link(link),                  # bare link, matches ETL
            "source":      feed_meta["source"],
            "published_at": published,
            "tags":        get_rss_tags(entry),
            "rating":      parse_rating(scraped.get("rating")),   # normalized to float
            "ingredients": normalize_ingredients(                  # fully normalized
                               scraped.get("ingredients", [])
                           ),
        }
    }

# poll a single feed
def poll_feed(feed_meta: dict):
    source = feed_meta["source"]
    url    = feed_meta["url"]
    logger.info("Polling %s ...", source)

    try:
        feed = feedparser.parse(url)
    except Exception as e:
        logger.error("Failed to fetch feed %s: %s", source, e)
        return 0, 0

    prod    = get_producer()
    sent    = 0
    skipped = 0

    for entry in feed.entries:  # parallelize in future maybe
        title     = entry.get("title", "").strip()
        link      = entry.get("link", "")
        recipe_id = make_recipe_id(title, link)

        if recipe_id in seen_ids:
            continue

        scraped = scrape_recipe(link)
        if scraped is None:
            logger.debug("  ✗ Skipped (not a recipe): %s", title)
            skipped += 1
            seen_ids.add(recipe_id)
            continue

        event = build_event(entry, feed_meta, scraped)

        try:
            prod.send(TOPIC, value=event).get(timeout=10)
            seen_ids.add(recipe_id)
            sent += 1
            logger.info("  ✓ Sent: %s", title)
        except KafkaError as e:
            logger.error("  ✗ Kafka error for '%s': %s", title, e)

    prod.flush()
    logger.info("%s — %d sent, %d skipped", source, sent, skipped)
    return sent, skipped

# poll all feeds
def poll_all():
    total_sent    = 0
    total_skipped = 0
    for feed_meta in RSS_FEEDS:     # parallelize maybe
        sent, skipped = poll_feed(feed_meta)
        total_sent    += sent
        total_skipped += skipped
    logger.info("Cycle complete — %d recipes sent, %d articles skipped\n",
                total_sent, total_skipped)

# --------------------------------------
def main():
    logger.info("Starting Leftover-to-Makeover RSS Ingestor")
    logger.info("Feeds: %d | Topic: %s | Poll interval: %ds",
                len(RSS_FEEDS), TOPIC, POLL_INTERVAL_SECONDS)
    poll_all()
    try:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)
            poll_all()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    main()
