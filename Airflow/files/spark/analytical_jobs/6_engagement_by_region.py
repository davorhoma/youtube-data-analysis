from pyspark.sql import SparkSession, functions as F
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("engagement_by_region").getOrCreate()

    # Učitavanje podataka
    df = spark.read.parquet(input_file_path)

    # Grupisanje po regionu i računanje prosečnog engagement-a, lajkova, komentara i pregleda
    engagement_per_region = (
        df.groupBy("region_code")
        .agg(
            F.avg("engagement").alias("avg_engagement"),
            F.avg("likes").alias("avg_likes"),
            F.avg("comment_count").alias("avg_comments"),
            F.avg("view_count").alias("avg_views")
        )
        .orderBy(F.desc("avg_engagement"))
    )

    # Prikaz rezultata
    engagement_per_region.show(truncate=False)

    # Upis u MongoDB
    (
        engagement_per_region.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()