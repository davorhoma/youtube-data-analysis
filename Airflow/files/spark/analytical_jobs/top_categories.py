from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("top_categories").getOrCreate()

    df = spark.read.parquet(input_file_path)

    top_categories = (
        df.groupBy("category_id", "category_name")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .limit(10)
    )

    top_categories.show(truncate=False)

    (
        top_categories.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
