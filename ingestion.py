"""
rss_ingestor.py
---------------
Leftover to Makeover — RSS Ingestion Pipeline
Polls multiple RSS feeds, validates each entry is a real recipe
by scraping the page with recipe-scrapers, then pushes structured
events to the Kafka topic `recipes_raw`.

Dependencies:
    pip install feedparser kafka-python schedule recipe-scrapers
"""

import json
import hashlib
import time
import logging
from datetime import datetime, timezone

import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError
from recipe_scrapers import scrape_me
from recipe_scrapers._exceptions import SchemaOrgException, WebsiteNotImplementedError

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC = "recipes_raw"
POLL_INTERVAL_SECONDS = 15  # poll every 15

# All feeds — scraper will automatically filter out non-recipe articles
RSS_FEEDS = [
    # ── Dedicated recipe blogs (clean, reliable) ──────────────────
    {"source": "RecipeTin Eats",     "url": "https://www.recipetineats.com/feed",               "cuisine": None},
    {"source": "Budget Bytes",       "url": "https://www.budgetbytes.com/feed",                 "cuisine": None},
    {"source": "Minimalist Baker",   "url": "https://minimalistbaker.com/feed",                 "cuisine": None},
    {"source": "Cookie and Kate",    "url": "https://cookieandkate.com/feed",                   "cuisine": None},
    {"source": "Downshiftology",     "url": "https://downshiftology.com/feed",                  "cuisine": None},
    {"source": "Well Plated",        "url": "https://www.wellplated.com/feed",                  "cuisine": None},
    {"source": "Spend with Pennies", "url": "https://www.spendwithpennies.com/feed",            "cuisine": None},
    {"source": "Smitten Kitchen",    "url": "https://feeds.feedburner.com/smittenkitchen",      "cuisine": None},

    # ── Major food media (mixed content — scraper filters non-recipes) ──
    {"source": "Food Network",       "url": "https://www.foodnetwork.com/feeds/rss/content-feed.rss", "cuisine": None},
    {"source": "BBC Good Food",      "url": "https://www.bbcgoodfood.com/recipes/feed",         "cuisine": "British"},
    {"source": "Bon Appetit",        "url": "https://www.bonappetit.com/feed/rss",              "cuisine": None},
    {"source": "Taste of Home",      "url": "https://www.tasteofhome.com/feed",                 "cuisine": "American"},
    {"source": "Allrecipes",         "url": "https://www.allrecipes.com/feeds/allrecipesrss.aspx", "cuisine": None},
    {"source": "Epicurious",         "url": "https://www.epicurious.com/services/rss/feeds/allrecipes.xml", "cuisine": None},
    {"source": "Simply Recipes",     "url": "https://www.simplyrecipes.com/feed",               "cuisine": None},
    {"source": "Serious Eats",       "url": "https://www.seriouseats.com/feeds/all",            "cuisine": None},
]

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rss_ingestor")

# ──────────────────────────────────────────────
# Deduplication (in-memory)
# ──────────────────────────────────────────────

seen_ids: set[str] = set()

# ──────────────────────────────────────────────
# Kafka producer
# ──────────────────────────────────────────────

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

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def generate_recipe_id(title: str, link: str) -> str:
    base = (title + link).encode("utf-8")
    return "rec_" + hashlib.md5(base).hexdigest()[:10]


def get_rss_tags(entry) -> list[str]:
    return [t.get("term", "") for t in entry.get("tags", []) if t.get("term")]


def scrape_recipe(url: str) -> dict | None:
    """
    Attempt to scrape structured recipe data from the article URL.
    Returns a dict of recipe fields if it's a valid recipe page,
    or None if the page has no recipe schema (i.e. it's an article).
    """
    try:
        scraper = scrape_me(url)
        ingredients = scraper.ingredients()

        # Must have at least 2 ingredients to count as a real recipe
        if not ingredients or len(ingredients) < 2:
            return None

        return {
            "ingredients": ingredients,
            "instructions": scraper.instructions(),
            "total_time": scraper.total_time(),
            "yields": scraper.yields(),
            "cuisine": scraper.cuisine(),
            "category": scraper.category(),
            "nutrients": scraper.nutrients(),
            "image_url": scraper.image(),
            "rating": scraper.ratings(),
        }

    except (SchemaOrgException, WebsiteNotImplementedError):
        return None  # not a recipe page — skip silently
    except Exception as e:
        logger.debug("Scrape failed for %s: %s", url, e)
        return None


def build_event(entry, feed_meta: dict, scraped: dict) -> dict:
    title = entry.get("title", "unknown").strip()
    link = entry.get("link", "")

    published = None
    if entry.get("published_parsed"):
        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()

    # Prefer scraped cuisine over feed-level hint
    cuisine = scraped.get("cuisine") or feed_meta.get("cuisine") or "unknown"

    return {
        "event_type": "new_recipe",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recipe": {
            "recipe_id":    generate_recipe_id(title, link),
            "title":        title,
            "link":         link,
            "source":       feed_meta["source"],
            "published_at": published,
            "tags":         get_rss_tags(entry),
            "cuisine":      cuisine,
            "meal_type":    scraped.get("category") or "unknown",
            "ingredients":  scraped.get("ingredients", []),
            "instructions": scraped.get("instructions", ""),
            "total_time":   scraped.get("total_time"),
            "yields":       scraped.get("yields"),
            "nutrients":    scraped.get("nutrients", {}),
            "image_url":    scraped.get("image_url"),
            "rating":       scraped.get("rating"),
        }
    }

# ──────────────────────────────────────────────
# Poll a single feed
# ──────────────────────────────────────────────

def poll_feed(feed_meta: dict):
    source = feed_meta["source"]
    url = feed_meta["url"]
    logger.info("Polling %s ...", source)

    try:
        feed = feedparser.parse(url)
    except Exception as e:
        logger.error("Failed to fetch feed %s: %s", source, e)
        return 0, 0

    prod = get_producer()
    sent = 0
    skipped = 0

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "")
        recipe_id = generate_recipe_id(title, link)

        if recipe_id in seen_ids:
            continue

        # Validate: try to scrape structured recipe data from the page
        scraped = scrape_recipe(link)
        if scraped is None:
            logger.debug("  ✗ Skipped (not a recipe): %s", title)
            skipped += 1
            seen_ids.add(recipe_id)  # don't retry this URL
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
    logger.info("%s — %d sent, %d skipped (not recipes)", source, sent, skipped)
    return sent, skipped

# ──────────────────────────────────────────────
# Poll all feeds
# ──────────────────────────────────────────────

def poll_all():
    total_sent = 0
    total_skipped = 0
    for feed_meta in RSS_FEEDS:
        sent, skipped = poll_feed(feed_meta)
        total_sent += sent
        total_skipped += skipped
    logger.info("Cycle complete — %d recipes sent, %d articles skipped\n", total_sent, total_skipped)

# ──────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────

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