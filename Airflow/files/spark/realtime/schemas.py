from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

video_schema = StructType([
    StructField("collected_at", StringType()),
    StructField("region_code", StringType()),
    StructField("video_id", StringType()),
    StructField("title", StringType()),
    StructField("published_at", StringType()),
    StructField("channel_id", StringType()),
    StructField("channel_title", StringType()),
    StructField("tags", StringType()),
    StructField("category_id", StringType()),
    StructField("view_count", DoubleType()),
    StructField("like_count", DoubleType()),
    StructField("dislike_count", DoubleType()),
    StructField("comment_count", DoubleType()),
    StructField("duration", StringType()),
])

def duration_to_seconds(col):
    h = F.coalesce(F.regexp_extract(col, r"(\d+)H", 1).cast("long"), F.lit(0))
    m = F.coalesce(F.regexp_extract(col, r"(\d+)M", 1).cast("long"), F.lit(0))
    s = F.coalesce(F.regexp_extract(col, r"(\d+)S", 1).cast("long"), F.lit(0))
    return h * 3600 + m * 60 + s