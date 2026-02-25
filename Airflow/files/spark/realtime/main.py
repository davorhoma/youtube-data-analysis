from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from config import (
    BATCH_STATS_PATH,
    MONGO_URI,
    MONGO_DB,
    TOPIC_NAME,
    KAFKA_BOOTSTRAP_SERVERS,
)
from schemas import video_schema, duration_to_seconds
from stream_processors import (
    duration_distribution,
    channel_regional_reach,
    trending_freshness,
    historical_overlap,
    current_vs_historical_views,
)

spark = (
    SparkSession.builder.appName("YouTubeTrendingStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_kafka.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .withColumn("data", F.from_json(F.col("json_str"), video_schema))
    .select("data.*", "kafka_ts")
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("collected_at", "yyyy-MM-dd HH:mm:ss"), F.col("kafka_ts")
        ),
    )
    .withColumn("duration_sec", duration_to_seconds(F.col("duration")))
    .withColumn(
        "engagement_score",
        F.round(
            (F.col("like_count") + F.col("comment_count"))
            / F.greatest(F.col("view_count"), F.lit(1)),
            6,
        ),
    )
    .withColumn(
        "like_ratio",
        F.round(
            F.col("like_count")
            / F.greatest(F.col("like_count") + F.col("dislike_count"), F.lit(1)),
            4,
        ),
    )
    .withWatermark("event_time", "30 seconds")
)

try:
    batch_stats_raw = spark.read.parquet(BATCH_STATS_PATH)
    batch_stats = batch_stats_raw.groupBy("region_code").agg(
        F.avg("view_count").alias("hist_avg_views"),
        F.stddev("view_count").alias("hist_stddev_views"),
    )
except Exception as e:
    print(f"Batch statistike nisu dostupne ({e}). P5 radi bez istorijskih podataka.")
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    batch_stats_schema = StructType(
        [
            StructField("region_code", StringType()),
            StructField("hist_avg_views", DoubleType()),
            StructField("hist_stddev_views", DoubleType()),
        ]
    )
    batch_stats = spark.createDataFrame([], batch_stats_schema)

from pyspark.sql import functions as F

top10_batch_static = (
    spark.read.format("mongodb")
    .option("connection.uri", MONGO_URI)
    .option("database", "youtube")
    .option("collection", "top_categories")
    .load()
    .select("category_id", "category_name", "number_of_videos")
)
total_historical = top10_batch_static.agg(F.sum("number_of_videos")).first()[0]
top10_batch_static = top10_batch_static.withColumn(
    "historical_pct", F.col("number_of_videos") / F.lit(total_historical) * 100
)

try:
    batch_channel_stats = (
        batch_stats_raw
        .groupBy("channel_id", "channel_title")
        .agg(
            F.approx_count_distinct("region_code").alias("hist_avg_reach"),
        )
    )
except Exception as e:
    print(f"[WARN] Channel batch statistike nisu dostupne ({e}). P8 radi bez istorijskih podataka.")
    batch_channel_stats = spark.createDataFrame(
        [],
        StructType([
            StructField("channel_id",      StringType()),
            StructField("channel_title",   StringType()),
            StructField("hist_avg_reach",  DoubleType()),
        ]),
    )

hist_channel_ids = (
    batch_stats_raw
    .select("channel_id")
    .distinct()
)

q6 = duration_distribution(parsed)
q8 = channel_regional_reach(parsed)
q11 = trending_freshness(parsed)
q12 = historical_overlap(parsed, hist_channel_ids)
q13 = current_vs_historical_views(parsed, batch_stats_raw)

print("✅ Stream procesori pokrenuti.")
print(f"   P1 — duration_distribution → {MONGO_DB}.duration_distribution")
print(f"   P2 — channel_regional_reach → {MONGO_DB}.channel_regional_reach")
print(f"   P3 — trending_freshness → {MONGO_DB}.trending_freshness")
print(f"   P4 — historical_overlap → {MONGO_DB}.historical_overlap")
print(f"   P5 — current_vs_historical_views → {MONGO_DB}.current_vs_historical_views")

spark.streams.awaitAnyTermination()
