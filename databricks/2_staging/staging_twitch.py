import dlt as dp
from pyspark.sql.functions import col, xxhash64, to_timestamp, timestamp_seconds

@dp.table(
    name="stg_twitch",
    comment="",
    table_properties={"quality": "staging"}
)
def stg_twitch():
    return (
        dp.read_stream("bronze_twitch")
        .select(
            xxhash64(col("user_id"), col("ingestion_timestamp_utc")).alias("sk_deduplication"),
            col("user_id").alias("streamer_id"),
            col("user_login").alias("streamer_login"),
            col("user_name").alias("streamer_name"),
            col("game_id").alias("twitch_game_id"),
            col("game_name").alias("game_name"),
            col("type").alias("stream_type"),
            col("title").alias("stream_title"),
            col("viewer_count").alias("viewer_count"),
            col("language").alias("language"),
            timestamp_seconds(col("started_at")).alias("started_at_utc"),
            timestamp_seconds(col("ingestion_timestamp_utc")).alias("ingested_at_utc"),
            col("is_mature").alias("is_mature")
        )
    )