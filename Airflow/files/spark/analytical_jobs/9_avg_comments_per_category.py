from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("avg_comments_per_category").getOrCreate()

    # Učitavanje glavnog dataseta
    df = spark.read.parquet(input_file_path)

    df.show(5, truncate=False)

    # Računanje prosečnog broja komentara po kategoriji
    avg_comments_per_category = (
        df.groupBy("category_id", "category_name")
        .agg(F.avg("comment_count").alias("avg_comments"))
        .select("category_id", "category_name", "avg_comments")
        .orderBy(F.desc("avg_comments"))
        .limit(20)
    )

    avg_comments_per_category.show(truncate=False)

    # Upis u MongoDB
    (
        avg_comments_per_category.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()