"""
RSS Ingestion Script:
polls multiple RSS feeds, validates each entry is a real recipe by scraping the page with recipe-scrapers, 
then pushes structured events to the Kafka topic `recipes_raw`.

Dependencies:
#   pip install feedparser kafka-python recipe-scrapers ingredient-parser-nlp
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
POLL_INTERVAL_SECONDS = 10 # polls feeds every 10 seconds


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

# ingredient parsing constants
SERVING_PHRASES = frozenset({
    "for serving", "for garnish", "for topping", "for dipping",
    "for drizzling", "to serve"
})

TRAILING_PREP = re.compile(
    r"\b(finely|roughly|coarsely|thinly|thickly|side|halved|sliced|"
    r"chopped|diced|minced|grated|shredded|crushed|divided|optional|"
    r"softened|melted|beaten|torn|peeled|deveined|butterflied)\b.*$",
    re.IGNORECASE
)

LEADING_UNITS = re.compile(
    r"^(cup|cups|tbsp|tsp|tablespoon|teaspoon|oz|lb|gram|kg|ml|liter|pinch|bunch|clove|cloves|"
    r"slice|slices|can|cans|package|stick|handful|sprig|sheet|fillet|block)\s+",
    re.IGNORECASE
)

QUANTITY_WORDS = re.compile(r"^(half|quarter|third|a|an|\d+)\s+", re.IGNORECASE)

UNIT_WORDS = frozenset({
    "cup", "cups", "tbsp", "tsp", "tablespoon", "teaspoon", "oz", "lb",
    "gram", "kg", "ml", "liter", "pinch", "bunch", "clove", "cloves",
    "slice", "slices", "can", "cans", "package", "stick", "handful",
    "sprig", "sheet", "fillet", "block", "medium", "large", "small"
})

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


# ID + normalization helpers
def normalize_link(link: str) -> str:
    return re.sub(r"^https?://", "", str(link)).strip("/")

def make_recipe_id(title: str, link: str) -> str:
    base = (title + normalize_link(link)).encode("utf-8")
    return "rec_" + hashlib.md5(base).hexdigest()[:10]

def make_ingredient_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return "ing_" + slug

def canonicalize(name: str) -> str:
    name = re.sub(r"[^\w\s-]", "", name)
    return re.sub(r"\s+", " ", name.strip().lower())

# ingredient normalization
def strip_brand_names(name: str) -> str:
    words = name.split()
    if len(words) <= 1:
        return name
    # keep first word even if capitalized, strip subsequent capitalized words
    filtered = [words[0]] + [w for w in words[1:] if not w[0].isupper()]
    return " ".join(filtered) if filtered else name

def strip_trailing_prep(name: str) -> str:
    return TRAILING_PREP.sub("", name).strip()

def strip_leading_units(name: str) -> str:
    return LEADING_UNITS.sub("", name).strip()

def is_serving_line(raw: str) -> bool:
    lower = raw.lower()
    return any(phrase in lower for phrase in SERVING_PHRASES)

def preprocess_ingredient(raw: str) -> str:
    # strip parentheticals
    raw = re.sub(r"\(.*?\)", "", raw).strip()

    # strip "room temperature"
    raw = re.sub(r"\bat\s+room\s+temperature\b", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\broom\s+temperature\b", "", raw, flags=re.IGNORECASE).strip()

    # handle "a X of Y" vague quantifiers
    raw = re.sub(r"^a\s+(handful|pinch|splash|drizzle|dash|bit|few|couple)\s+of\s+",
                 "", raw, flags=re.IGNORECASE).strip()

    # handle bare "X of Y" vague quantifiers
    raw = re.sub(r"^(handful|pinch|splash|drizzle|dash|bit|few|couple)\s+of\s+",
                 "", raw, flags=re.IGNORECASE).strip()

    # handle "or" alternatives
    if " or " in raw.lower():
        raw = raw.split(" or ")[0].strip()

    # handle "of" constructions — skip "cream of X" patterns
    if " of " in raw.lower() and not re.search(r"cream of|out of|instead of", raw.lower()):
        parts = raw.split(" of ", 1)
        thing = re.sub(r"[\d¼½¾⅓⅔]+", "", parts[-1]).strip().rstrip("s")
        while QUANTITY_WORDS.match(thing):
            thing = QUANTITY_WORDS.sub("", thing).strip()
        descriptor = parts[0].strip()
        raw = f"{thing} {descriptor}".strip()

    return raw

def normalize_ingredients(raw_list: list[str]) -> list[dict]:
    result = []
    for raw in raw_list:
        if raw.count(",") >= 2 or is_serving_line(raw):
            logger.debug("Skipping serving/multi-item line: %s", raw)
            continue

        name = ""
        try:
            preprocessed = preprocess_ingredient(raw)
            parsed = nlp_parse(preprocessed)
            name = canonicalize(strip_trailing_prep(strip_leading_units(strip_brand_names(parsed.get("name", "")))))
        except Exception as e:
            logger.debug("NLP parse failed for '%s': %s", raw, e)

        if len(name.split()) > 5:
            name = canonicalize(strip_trailing_prep(name))

        if not name or len(name.split()) > 5:
            words = [
                w for w in preprocess_ingredient(raw).split()
                if not w[0].isdigit()
                and w.lower() not in {"a", "an", "the", "of", "and", "side", "skin", "on"}
                and w.lower() not in UNIT_WORDS  # add this
            ]
            name = canonicalize(strip_trailing_prep(" ".join(words[:2]))) if words else raw.strip().lower()

        result.append({
            "ingredient_id": make_ingredient_id(name),
            "name":          name,
            "raw_text":      raw.strip(),
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
        }
    except (SchemaOrgException, WebsiteNotImplementedError):
        return None
    except Exception as e:
        logger.debug("Scrape failed for %s: %s", url, e)
        return None

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

    for entry in feed.entries:
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

def main():
    logger.info("Starting Leftover-to-Makeover RSS Ingestor")
    logger.info("Feeds: %d | Topic: %s | Poll interval: %ds",
                len(RSS_FEEDS), TOPIC, POLL_INTERVAL_SECONDS)
    try:
        while True:
            poll_all()
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    main()
