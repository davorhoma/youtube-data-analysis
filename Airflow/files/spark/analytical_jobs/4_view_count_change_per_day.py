from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window
import sys


if __name__ == "__main__":

    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder \
        .appName("views_over_time") \
        .getOrCreate()

    df = spark.read.parquet(input_file_path)

    df = df.withColumn("date", F.to_date(col("trending_date")))

    window_spec = Window.partitionBy("video_id").orderBy("date")

    df_us = df.filter(F.col("region_code") == "US")

    views_over_time = (
        df_us.select("video_id", "title", "date", "view_count")
        .withColumn("previous_day_views", lag("view_count").over(window_spec))
        .withColumn(
            "views_difference",
            col("view_count") - col("previous_day_views")
        )
        .orderBy("video_id", "date")
        .limit(100)
    )

    views_over_time.show(truncate=False)

    (
        views_over_time.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()