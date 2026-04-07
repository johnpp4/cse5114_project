"""
spark_consumer.py
-----------------
Leftover to Makeover — Spark Structured Streaming Consumer
Reads recipe events from Kafka topic `recipes_raw`,
parses + explodes them, then upserts into three Snowflake tables:
    • RECIPES
    • INGREDIENTS
    • RECIPE_INGREDIENTS

Dependencies:
    pip install pyspark snowflake-connector-python

Run with:
    python spark_consumer.py

Note: Spark 4.x uses Scala 2.13. The Kafka package will be downloaded
automatically on first run (~30 seconds).
"""

import os
import json
import logging
import snowflake.connector

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, ArrayType,
)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "recipes_raw"

SNOWFLAKE_CONN = {
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database":  os.environ["SNOWFLAKE_DATABASE"],
    "schema":    os.environ["SNOWFLAKE_SCHEMA"],
}

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/recipes")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("spark_consumer")

# ──────────────────────────────────────────────
# Spark session
# Spark 4.x = Scala 2.13 → use _2.13 artifact
# ──────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("LeftoverToMakeover-Streaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
    )
    .config("spark.sql.shuffle.partitions", "4")   # keep it light on a local Mac
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session started — version %s", spark.version)

# ──────────────────────────────────────────────
# Schema  (mirrors build_event() in ingestion.py)
# ──────────────────────────────────────────────

INGREDIENT_SCHEMA = StructType([
    StructField("ingredient_id",  StringType(), True),
    StructField("name",           StringType(), True),
    StructField("quantity_grams", FloatType(),  True),
    StructField("raw_text",       StringType(), True),
])

RECIPE_SCHEMA = StructType([
    StructField("recipe_id",    StringType(), True),
    StructField("title",        StringType(), True),
    StructField("link",         StringType(), True),
    StructField("source",       StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("tags",         ArrayType(StringType()), True),
    StructField("rating",       FloatType(),  True),
    StructField("ingredients",  ArrayType(INGREDIENT_SCHEMA), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("recipe",     RECIPE_SCHEMA, True),
])

# ──────────────────────────────────────────────
# Read stream from Kafka
# ──────────────────────────────────────────────

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON value bytes → structured columns
parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("evt"))
    .filter(F.col("evt.event_type") == "new_recipe")
    .select("evt.recipe.*", F.col("evt.timestamp").alias("ingested_at"))
)

# ──────────────────────────────────────────────
# Snowflake helpers
# ──────────────────────────────────────────────

def get_snowflake_conn():
    return snowflake.connector.connect(**SNOWFLAKE_CONN)


def run_merge(staging_table: str, target_table: str, merge_keys: list[str], columns: list[str]):
    """Execute a MERGE from staging into target table via snowflake-connector-python."""
    merge_condition = " AND ".join(
        f"t.{k} = s.{k}" for k in merge_keys
    )
    update_cols = [c for c in columns if c not in merge_keys]
    update_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)

    sql = f"""
        MERGE INTO {target_table} AS t
        USING {staging_table} AS s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    conn = get_snowflake_conn()
    try:
        conn.cursor().execute(sql)
        logger.info("MERGE into %s complete", target_table)
    finally:
        conn.close()


def drop_staging(staging_table: str):
    conn = get_snowflake_conn()
    try:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {staging_table}")
    finally:
        conn.close()


def upsert_to_snowflake(batch_df: DataFrame, target_table: str, merge_keys: list[str]):
    """
    Write a micro-batch DataFrame to a Snowflake staging table,
    then MERGE it into the target table, then drop staging.
    Uses snowflake-connector-python (works with Spark 4.x).
    """
    if batch_df.isEmpty():
        return

    staging_table = f"{target_table}_STAGING_{os.getpid()}"
    columns = batch_df.columns

    # Write batch to Snowflake as a staging table via JDBC
    jdbc_url = (
        f"jdbc:snowflake://{SNOWFLAKE_CONN['account']}.snowflakecomputing.com"
        f"/?db={SNOWFLAKE_CONN['database']}"
        f"&schema={SNOWFLAKE_CONN['schema']}"
        f"&warehouse={SNOWFLAKE_CONN['warehouse']}"
    )

    try:
        (
            batch_df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", staging_table)
            .option("user", SNOWFLAKE_CONN["user"])
            .option("password", SNOWFLAKE_CONN["password"])
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver")
            .mode("overwrite")
            .save()
        )
        run_merge(staging_table, target_table, merge_keys, columns)
    finally:
        drop_staging(staging_table)

# ──────────────────────────────────────────────
# Micro-batch processor
# ──────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        logger.info("Batch %d — empty, skipping", batch_id)
        return

    count = batch_df.count()
    logger.info("Batch %d — %d recipe(s) received", batch_id, count)

    # ── 1. RECIPES ──────────────────────────────
    recipes_batch = batch_df.select(
        F.col("recipe_id"),
        F.col("title"),
        F.lit("unknown").alias("cuisine"),
        F.lit("unknown").alias("meal_type"),
        F.col("rating"),
        F.col("link"),
        F.col("published_at").alias("created_at"),
        F.col("source"),
        F.col("published_at"),
        F.array_join(F.col("tags"), ",").alias("tags"),
    ).dropDuplicates(["recipe_id"])

    # ── 2. INGREDIENTS ──────────────────────────
    ingredients_batch = (
        batch_df
        .select(F.explode("ingredients").alias("ing"))
        .select(
            F.col("ing.ingredient_id"),
            F.col("ing.name"),
        )
        .dropDuplicates(["ingredient_id"])
    )

    # ── 3. RECIPE_INGREDIENTS ───────────────────
    recipe_ingredients_batch = (
        batch_df
        .select("recipe_id", F.explode("ingredients").alias("ing"))
        .select(
            F.col("recipe_id"),
            F.col("ing.ingredient_id"),
            F.col("ing.quantity_grams"),
            F.col("ing.raw_text"),
        )
        .dropDuplicates(["recipe_id", "ingredient_id"])
    )

    # ── Write to Snowflake (ingredients first — FK dependency) ──
    upsert_to_snowflake(ingredients_batch,        "INGREDIENTS",        ["ingredient_id"])
    upsert_to_snowflake(recipes_batch,            "RECIPES",            ["recipe_id"])
    upsert_to_snowflake(recipe_ingredients_batch, "RECIPE_INGREDIENTS", ["recipe_id", "ingredient_id"])

    logger.info("Batch %d — committed to Snowflake", batch_id)

# ──────────────────────────────────────────────
# Launch streaming query
# ──────────────────────────────────────────────

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="30 seconds")
    .start()
)

logger.info("Streaming query started — listening on topic '%s'", KAFKA_TOPIC)
query.awaitTermination()