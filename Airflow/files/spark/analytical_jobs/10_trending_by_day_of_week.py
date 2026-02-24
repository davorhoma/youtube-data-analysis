from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, dayofweek
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("trending_by_day_of_week").getOrCreate()

    # Učitavanje podataka
    df = spark.read.parquet(input_file_path)

    # Ekstrakcija dana u nedelji (1 = Sunday, 7 = Saturday)
    df_with_day = df.withColumn("day_of_week", dayofweek(col("trending_date")))

    # Agregacija po danu u nedelji
    trending_by_day = (
        df_with_day.groupBy("day_of_week")
        .agg(
            F.count("video_id").alias("video_count"),
            F.avg("view_count").alias("avg_views"),
            F.avg("likes").alias("avg_likes"),
            F.avg("dislikes").alias("avg_dislikes"),
            F.avg("comment_count").alias("avg_comments"),
            F.avg("engagement").alias("avg_engagement")
        )
        .orderBy("day_of_week")
    )

    trending_by_day.show(truncate=False)

    # Upis u MongoDB
    (
        trending_by_day.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()