from pyspark.sql import SparkSession, functions as F
import sys


def to_snake_case(name: str) -> str:
    import re

    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_initial_data").getOrCreate()

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")  # važno za polja sa opisom koji prelazi liniju
        .csv(input_file_path)
    )

    df.select("*").show(5, truncate=False)

    df = df.select(*[F.col(c).alias(to_snake_case(c)) for c in df.columns])

    category_map = {
        1: "Film & Animation",
        2: "Autos & Vehicles",
        10: "Music",
        15: "Pets & Animals",
        17: "Sports",
        18: "Short Movies",
        19: "Travel & Events",
        20: "Gaming",
        21: "Videoblogging",
        22: "People & Blogs",
        23: "Comedy",
        24: "Entertainment",
        25: "News & Politics",
        26: "Howto & Style",
        27: "Education",
        28: "Science & Technology",
        29: "Nonprofits & Activism",
        30: "Movies",
        31: "Anime/Animation",
        32: "Action/Adventure",
        33: "Classics",
        34: "Comedy",
        35: "Documentary",
        36: "Drama",
        37: "Family",
        38: "Foreign",
        39: "Horror",
        40: "Sci-Fi/Fantasy",
        41: "Thriller",
        42: "Shorts",
        43: "Shows",
        44: "Trailers",
    }

    mapping_expr = F.create_map(*[F.lit(x) for x in sum(category_map.items(), ())])

    df = df.withColumn(
        "category_name",
        F.coalesce(mapping_expr[F.col("category_id")], F.lit("Unknown")),
    )

    selected_columns = [
        "region_code",
        "video_id",
        "title",
        "published_at",
        "channel_id",
        "channel_title",
        "category_id",
        "category_name",
        "trending_date",
        "tags",
        "view_count",
        "likes",
        "dislikes",
        "comment_count",
        "duration",
    ]

    df_selected = df.select(*selected_columns)

    df_selected = (
        df_selected.withColumn("trending_date", F.to_date("trending_date"))
        .withColumn("published_at", F.to_timestamp("published_at"))
        .withColumn("view_count", F.col("view_count").cast("long"))
        .withColumn("likes", F.col("likes").cast("long"))
        .withColumn("dislikes", F.col("dislikes").cast("long"))
        .withColumn("comment_count", F.col("comment_count").cast("long"))
    )

    # Parsiranje ISO 8601 trajanja (PT#M#S, PT#H#M#S itd.)
    df_selected = (
        df_selected.withColumn(
            "duration_h",
            F.regexp_extract("duration", r"PT(\d+)H", 1).cast("int"),
        )
        .withColumn(
            "duration_m",
            F.regexp_extract("duration", r"PT(?:\d+H)?(\d+)M", 1).cast("int"),
        )
        .withColumn(
            "duration_s",
            F.regexp_extract("duration", r"PT(?:\d+H)?(?:\d+M)?(\d+)S", 1).cast("int"),
        )
    )

    df_selected = df_selected.withColumn(
        "duration_seconds",
        (
            F.coalesce(F.col("duration_h"), F.lit(0)) * 3600
            + F.coalesce(F.col("duration_m"), F.lit(0)) * 60
            + F.coalesce(F.col("duration_s"), F.lit(0))
        ),
    ).drop("duration_h", "duration_m", "duration_s", "duration")

    df_selected = df_selected.withColumn(
        "engagement",
        F.when(
            F.col("view_count") > 0,
            (F.col("likes") + F.col("comment_count")) / F.col("view_count"),
        ).otherwise(0),
    )

    (df_selected.write.mode("overwrite").parquet(output_path))

    df_selected.show(truncate=False)

    spark.stop()
