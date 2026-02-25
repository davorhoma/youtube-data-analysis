from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("fastest_growth_videos").getOrCreate()

    df = spark.read.parquet(input_file_path)

    window_spec = Window.partitionBy("video_id", "region_code").orderBy("trending_date")

    df_with_lag = df.withColumn(
        "prev_views", F.lag("view_count").over(window_spec)
    ).withColumn("prev_date", F.lag("trending_date").over(window_spec))

    # daily_views_growth >= 0 && daily_views_growth <= 200_000_000 da bi se izbegle anomalije u podacima
    # Postojao je slučaj sa video klipom koji je imao dnevni rast od preko 700 miliona pregleda, što je kasnije ispravljeno od strane YouTube-a, ali je i dalje prisutno u datasetu.
    df_with_delta = (
        df_with_lag.withColumn("date_diff", F.datediff("trending_date", "prev_date"))
        .withColumn("daily_views_growth", F.col("view_count") - F.col("prev_views"))
        .filter(F.col("prev_views").isNotNull())
        .filter(F.col("date_diff") == 1)
        .filter(F.col("daily_views_growth") >= 0)
        .filter(F.col("daily_views_growth") <= 200_000_000)
    )

    top_growth_videos = (
        df_with_delta.groupBy("video_id", "title")
        .agg(F.max("daily_views_growth").alias("max_daily_views_growth"))
        .orderBy(F.desc("max_daily_views_growth"))
        .limit(20)
    )

    top_growth_videos.show(truncate=False)

    (
        top_growth_videos.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
