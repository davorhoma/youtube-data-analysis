from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame
from config import (
    BATCH_STATS_PATH,
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

hist_channel_ids = batch_stats_raw.select("channel_id").distinct()

q1 = duration_distribution(parsed)
q2 = channel_regional_reach(parsed)
q3 = trending_freshness(parsed)
q4 = historical_overlap(parsed, hist_channel_ids)
q5 = current_vs_historical_views(parsed, batch_stats_raw)

print("✅ Stream procesori pokrenuti.")
print(f"   P1 — duration_distribution → {MONGO_DB}.duration_distribution")
print(f"   P2 — channel_regional_reach → {MONGO_DB}.channel_regional_reach")
print(f"   P3 — trending_freshness → {MONGO_DB}.trending_freshness")
print(f"   P4 — historical_overlap → {MONGO_DB}.historical_overlap")
print(f"   P5 — current_vs_historical_views → {MONGO_DB}.current_vs_historical_views")

spark.streams.awaitAnyTermination()
