from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("popularity_over_time").getOrCreate()

    df = spark.read.parquet(input_file_path)

    popularity_over_time = (
        df.groupBy("trending_date")
        .agg(
            F.avg("view_count").alias("average_views"),
            F.sum("view_count").alias("total_views"),
            F.count("video_id").alias("video_count")
        )
        .select("trending_date", "average_views", "total_views", "video_count")
        .orderBy("trending_date")
    )

    # Window za moving average (7 dana)
    window_spec = (
        Window
        .orderBy("trending_date")
        .rowsBetween(-6, 0)
    )

    popularity_with_moving_avg = (
        popularity_over_time
        .withColumn(
            "moving_avg_7d",
            F.avg("average_views").over(window_spec)
        )
    )

    popularity_with_moving_avg.show(truncate=False)

    (
        popularity_with_moving_avg.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()