import dlt as dp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType

twitch_schema = StructType([
    StructField("ingestion_timestamp_utc", LongType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_login", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("game_id", StringType(), True),
    StructField("game_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("viewer_count", LongType(), True),
    StructField("started_at", LongType(), True),
    StructField("language", StringType(), True),
    StructField("thumbnail_url", StringType(), True),
    StructField("tag_ids", ArrayType(StringType()), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("is_mature", BooleanType(), True)
])

@dp.table(
    name="bronze_twitch",
    comment="r",
    table_properties={"quality": "bronze"}
)
def bronze_twitch():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .schema(twitch_schema)
        .load("/Volumes/workspace/default/my_volume/raw/twitch")
    )