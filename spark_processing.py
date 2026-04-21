"""
spark_processing.py
-----------------
reads recipe events from Kafka topic `recipes_raw`, parses + explodes them, 
then upserts into two Snowflake tables:
    • RECIPES
    • RECIPE_INGREDIENTS

Dependencies:
    pip install pyspark snowflake-connector-python python-dotenv

Spark packages (set via --packages or spark.jars.packages):
    org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0
    net.snowflake:spark-snowflake_2.13:3.1.0
    net.snowflake:snowflake-jdbc:3.16.1

Run with:
    python spark_processing.py
"""

import os
import base64
import logging
import snowflake.connector
 
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType,
)

from dotenv import load_dotenv
load_dotenv()

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("spark_processing")

# configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "recipes_raw"
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/recipes")

def _load_private_key_b64() -> str:
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as fh:
        private_key = serialization.load_pem_private_key(
            fh.read(),
            password=None,
            backend=default_backend(),
        )
    der_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.b64encode(der_bytes).decode("utf-8")

SNOWFLAKE_OPTIONS: dict[str, str] = {
    "sfURL":      f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser":     os.environ["SNOWFLAKE_USER"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema":   os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "pem_private_key": _load_private_key_b64(),
}

SF_FORMAT = "net.snowflake.spark.snowflake"

# spark session
spark = (
    SparkSession.builder
    .appName("LeftoverToMakeover-Streaming")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
            "net.snowflake:spark-snowflake_2.13:3.1.0",
            "net.snowflake:snowflake-jdbc:3.16.1",
        ]),
    )
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.metricsEnabled", "false")        # moved here
    .config("spark.kafka.consumer.metrics.enabled", "false")      # add this
    .config(
        "spark.driver.extraJavaOptions",
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
    .config(
        "spark.executor.extraJavaOptions",
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session started — version %s", spark.version)

# schema
INGREDIENT_SCHEMA = StructType([
    StructField("ingredient_id",  StringType(), True),
    StructField("raw_text",       StringType(), True),
])

RECIPE_SCHEMA = StructType([
    StructField("recipe_id",    StringType(), True),
    StructField("title",        StringType(), True),
    StructField("link",         StringType(), True),
    StructField("source",       StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("ingredients",  ArrayType(INGREDIENT_SCHEMA), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp",  StringType(), True),
    StructField("recipe",     RECIPE_SCHEMA, True),
])

# read stream from kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# parse JSON value bytes → structured columns
parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("evt"))
    .filter(F.col("evt.event_type") == "new_recipe")
    .select("evt.recipe.*", F.col("evt.timestamp").alias("ingested_at"))
)

# Snowflake helpers
def _get_snowflake_conn() -> snowflake.connector.SnowflakeConnection:
    pkb = base64.b64decode(SNOWFLAKE_OPTIONS["pem_private_key"])
    account = SNOWFLAKE_OPTIONS["sfURL"].replace(".snowflakecomputing.com", "")
    return snowflake.connector.connect(
        user=SNOWFLAKE_OPTIONS["sfUser"],
        account=account,
        private_key=pkb,
        database=SNOWFLAKE_OPTIONS["sfDatabase"],
        schema=SNOWFLAKE_OPTIONS["sfSchema"],
        warehouse=SNOWFLAKE_OPTIONS["sfWarehouse"],
    )

def fetch_existing_links() -> set[str]:
    try:
        conn = _get_snowflake_conn()
        cur  = conn.cursor()
        cur.execute("SELECT LINK FROM RECIPES WHERE LINK IS NOT NULL")
        links = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        logger.info("Fetched %d existing links from RECIPES", len(links))
        return links
    except Exception as e:
        logger.warning("Could not fetch existing links (table may not exist yet): %s", e)
        return set()

def sf_write(df: DataFrame, table: str) -> None:
    (
        df.write
        .format(SF_FORMAT)
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table)
        .option("columnMapping", "name")
        .mode("overwrite")
        .save()
    )

def sf_run_sql(sql: str) -> None:
    Utils = spark._jvm.net.snowflake.spark.snowflake.Utils
    # convert Python dict to Java HashMap
    java_map = spark._jvm.java.util.HashMap()
    for k, v in SNOWFLAKE_OPTIONS.items():
        java_map.put(k, v)
    Utils.runQuery(java_map, sql)

def merge_sql(staging: str, target: str, merge_keys: list[str], all_columns: list[str]) -> str:
    on_clause = " AND ".join(f't.{k} = s.{k}' for k in merge_keys)
    update_cols = [c for c in all_columns if c not in merge_keys]
    update_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join(f"s.{c}" for c in all_columns)
    return f"""
        MERGE INTO {target} AS t
        USING {staging} AS s
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """

def upsert_to_snowflake(df: DataFrame, target_table: str, merge_keys: list[str]) -> None:
    if df.rdd.isEmpty():
        logger.info("  %s — nothing to upsert, skipping", target_table)
        return
 
    # uppercase everything
    df_upper = df.toDF(*[c.upper() for c in df.columns])
    all_cols  = df_upper.columns
    keys_upper = [k.upper() for k in merge_keys]
 
    staging_table = f"{target_table}_STAGING"
 
    # write staging
    sf_write(df_upper, staging_table)
    logger.info("Staged %s → %s", target_table, staging_table)
 
    # merge from staging into target using merge keys
    sql = merge_sql(staging_table, target_table, keys_upper, list(all_cols))
    sf_run_sql(sql)
    logger.info("MERGE into %s complete", target_table)
 
    # drop staging
    sf_run_sql(f"DROP TABLE IF EXISTS {staging_table}")


# micro-batch processor
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d — empty, skipping", batch_id)
        return

    count = batch_df.count()
    logger.info("Batch %d — %d recipe(s) received", batch_id, count)

    batch_df.cache()
    cached_df = batch_df  # hold reference to the cached version

    batch_df = batch_df.withColumn(
            "ingredients",
            F.coalesce(F.col("ingredients"), F.array()))
    try:
        # drop recipes whose link already exists in Snowflake ──
        existing_links = fetch_existing_links()
 
        if existing_links:
            existing_broadcast = spark.sparkContext.broadcast(existing_links)
 
            @F.udf(returnType="boolean")
            def is_new_link(link: str) -> bool:
                return link is not None and link not in existing_broadcast.value
 
            batch_df = batch_df.filter(is_new_link(F.col("link")))
 
        if batch_df.rdd.isEmpty():
            logger.info("Batch %d — all recipes already exist in Snowflake, skipping writes", batch_id)
            return
 
        new_count = batch_df.count()
        logger.info("Batch %d — %d new recipe(s) after idempotency filter", batch_id, new_count)

        # recipes
        recipes_df = batch_df.select(
            F.col("recipe_id"),
            F.col("title"),
            F.col("link"),
            F.col("ingested_at").alias("created_at"),
            F.col("source"),
            F.col("published_at"),
        ).dropDuplicates(["recipe_id"])
 
        # recipe ingredients
        recipe_ingredients_df = (
            batch_df
            .select("recipe_id", F.explode_outer("ingredients").alias("ing"))
            .select(
                F.col("recipe_id"),
                F.col("ing.ingredient_id"),
                F.col("ing.raw_text"),
            )
            .dropDuplicates(["recipe_id", "ingredient_id"])
        )

        # write to snowflake
        upsert_to_snowflake(recipes_df, "RECIPES", ["recipe_id"])
        upsert_to_snowflake(recipe_ingredients_df, "RECIPE_INGREDIENTS", ["recipe_id", "ingredient_id"])

    finally:
        cached_df.unpersist()

    logger.info("Batch %d — committed to Snowflake", batch_id)

# launch streaming query
query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="30 seconds")
    .start()
)

logger.info("Streaming query started — listening on topic '%s'", KAFKA_TOPIC)
query.awaitTermination()
