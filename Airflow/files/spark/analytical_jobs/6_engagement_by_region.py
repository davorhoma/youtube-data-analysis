from pyspark.sql import SparkSession, functions as F
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("engagement_by_region").getOrCreate()

    df = spark.read.parquet(input_file_path)

    engagement_per_region = (
        df.groupBy("region_code")
        .agg(
            F.avg("engagement").alias("average_engagement"),
            F.avg("likes").alias("average_likes"),
            F.avg("comment_count").alias("average_comments"),
            F.avg("view_count").alias("average_views")
        )
        .orderBy(F.desc("average_engagement"))
    )

    engagement_per_region.show(truncate=False)

    (
        engagement_per_region.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()