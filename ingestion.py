"""
RSS Ingestion Script:
polls multiple RSS feeds, validates each entry is a real recipe by scraping the page with recipe-scrapers, 
then pushes structured events to the Kafka topic `recipes_raw`.

Dependencies:
#     pip install feedparser kafka-python recipe-scrapers spacy
#     python -m spacy download en_core_web_sm
"""

import json
import hashlib
import time
import logging
import re
import spacy
from datetime import datetime, timezone

import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError
from recipe_scrapers import scrape_me
from recipe_scrapers._exceptions import SchemaOrgException, WebsiteNotImplementedError

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


nlp_model = spacy.load("en_core_web_sm")

MEASUREMENT_UNITS = {
    "cup", "cups", "tablespoon", "tablespoons", "tbsp", "teaspoon", "teaspoons",
    "tsp", "pound", "pounds", "lb", "lbs", "ounce", "ounces", "oz", "gram",
    "grams", "g", "kg", "ml", "liter", "liters", "pinch", "clove", "cloves",
    "slice", "slices", "package", "packages", "can", "cans", "bunch", "head",
    "quart", "quarts", "pint", "pints", "gallon", "gallons", "stick", "sticks",
    "dash", "handful", "sprig", "sprigs", "piece", "pieces", "bag", "bags", "spoonful", 
    "spoonfuls", "knob", "drop", "drops", "sheet", "sheets", "jar", "jars", "box", "boxes", 
    "strip", "strips", "fillet", "fillets", "rack", "racks", "bulb", "bulbs", "sprinkle", "sprinkles"
}

PREP_WORDS = {
    "chopped", "diced", "minced", "sliced", "grated", "shredded", "crushed",
    "frozen", "fresh", "dried", "cooked", "raw", "peeled", "halved", "cubed",
    "softened", "melted", "divided", "optional", "thawed", "rinsed", "drained",
    "trimmed", "pitted", "seeded", "deveined", "butterflied", "scored",
    "toasted", "roasted", "sauteed", "blanched", "beaten", "sifted", "packed",
    "heaping", "level", "rounded", "large", "medium", "small", "finely",
    "roughly", "coarsely", "thinly", "thick", "thin", "skinless", "boneless", "deboned", "deseeded", "quartered", "julienned",
    "crumbled", "torn", "separated", "room", "temperature", "firmly", "lightly", "well", "about", "approximately", "plus", "extra",
    "lean", "fat", "free", "reduced", "low", "sodium", "unsalted", "salted",
    "uncooked", "leftover", "homemade", "store", "bought", "such", "preferably",
}

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
    name = re.sub(r"[^\w\s-]", "", name)
    return re.sub(r"\s+", " ", name.strip().lower())


def extract_name_spacy(raw_text: str) -> str:
    cleaned = re.sub(r"\(.*?\)", "", raw_text).strip()

    # drop prep instructions that follow a comma --> ok do we need to do this?
    cleaned = cleaned.split(",")[0].strip()

    doc = nlp_model(cleaned)
    for token in doc:
        # ROOT is the syntactic head of the sentence; nsubj is the nominal
        # subject — both reliably point to the ingredient noun
        if token.dep_ in ("ROOT", "nsubj") and token.pos_ == "NOUN":
            parts = [token.text]

            for child in token.children:
                is_measurement = child.text.lower() in MEASUREMENT_UNITS
                is_prep_adj = (
                    child.dep_ == "amod"
                    and child.text.lower() in PREP_WORDS
                )

                if child.dep_ in ("compound", "amod") and not is_measurement and not is_prep_adj:
                    parts.insert(0, child.text)

            return canonicalize(" ".join(parts))

    return ""

# ingredient normalization
def normalize_ingredients(raw_list: list[str]) -> list[dict]:
    result = []
    for raw in raw_list:
        name = extract_name_spacy(raw)
        if not name:
            name = raw.strip().lower()
        iid = make_ingredient_id(name)
        result.append({
            "ingredient_id": iid,
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

# --------------------------------------
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
