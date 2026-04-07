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

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from snowflake.connector.pandas_tools import write_pandas


# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "recipes_raw"

def get_snowflake_conn():
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        private_key=private_key_bytes,
    )

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
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED")
    .config("spark.executor.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED")
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
    if batch_df.rdd.isEmpty():
        return

    staging_table = f"{target_table}_STAGING_{os.getpid()}"
    columns = batch_df.columns

    # Convert Spark DataFrame → pandas → write directly via Python connector
    pandas_df = batch_df.toPandas()
    pandas_df.columns = [c.upper() for c in pandas_df.columns]

    conn = get_snowflake_conn()
    try:
        # Write staging table
        write_pandas(
            conn,
            pandas_df,
            staging_table.upper(),
            auto_create_table=True,
            overwrite=True,
        )
        # MERGE into target
        run_merge(staging_table.upper(), target_table.upper(), 
                  [k.upper() for k in merge_keys], 
                  list(pandas_df.columns))
    finally:
        try:
            conn.cursor().execute(f"DROP TABLE IF EXISTS {staging_table.upper()}")
        except Exception:
            pass
        conn.close()

# ──────────────────────────────────────────────
# Micro-batch processor
# ──────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d — empty, skipping", batch_id)
        return

    count = batch_df.count()
    logger.info("Batch %d — %d recipe(s) received", batch_id, count)

    # ── 1. RECIPES ──────────────────────────────
    recipes_batch = batch_df.select(
        F.col("recipe_id"),
        F.col("title"),
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