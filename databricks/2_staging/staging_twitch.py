import dlt as dp
from pyspark.sql.functions import col, xxhash64, to_timestamp

@dp.table(
    name="stg_twitch",
    comment="Padronização de tipos e chaves para Twitch a partir de dados tabulares",
    table_properties={"quality": "staging"}
)
# Regras de Qualidade ajustadas para streamer_id (user_id)
@dp.expect_or_drop("valid_streamer_id", "streamer_id IS NOT NULL")
@dp.expect("valid_viewer_count", "viewer_count >= 0")
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
            to_timestamp(col("started_at")).alias("started_at_utc"),
            to_timestamp(col("ingestion_timestamp_utc")).alias("ingested_at_utc"),
            col("is_mature").alias("is_mature")
        )
    )