import dlt as dp
from pyspark.sql.functions import col, xxhash64, timestamp_seconds

@dp.table(
    name="stg_steam",
    comment="",
    table_properties={"quality": "staging"}
)
def stg_steam():
    return (
        dp.read_stream("bronze_steam")
        .select(
            xxhash64(col("id"), col("ingestion_timestamp_utc")).alias("sk_deduplication"),
            col("id").alias("steam_game_id"),
            col("jogo").alias("game_name"),
            col("qtd_jogadores").alias("peak_players"),
           timestamp_seconds(col("ingestion_timestamp_utc")).alias("ingested_at_utc")
        )
    )