from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("fastest_growth_videos").getOrCreate()

    # Učitavanje parquet fajla
    df = spark.read.parquet(input_file_path)

    # Definisanje prozora po video_id, sortirano po trending_date
    window_spec = Window.partitionBy("video_id").orderBy("trending_date")

    # Dodavanje kolone sa prethodnim brojem pregleda
    df_with_lag = df.withColumn("prev_views", F.lag("view_count").over(window_spec))

    # Računanje dnevne promene pregleda
    df_with_delta = df_with_lag.withColumn(
        "daily_growth",
        F.when(F.col("prev_views").isNotNull(), F.col("view_count") - F.col("prev_views")).otherwise(0)
    )

    # Grupisanje po video_id i title, uz maksimalni rast
    top_growth_videos = (
        df_with_delta.groupBy("video_id", "title", "channel_id", "channel_title")
        .agg(F.max("daily_growth").alias("max_daily_growth"))
        .orderBy(F.desc("max_daily_growth"))
        .limit(20)
    )

    # Prikaz rezultata
    top_growth_videos.show(truncate=False)

    # Snimanje rezultata u MongoDB
    (
        top_growth_videos.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()