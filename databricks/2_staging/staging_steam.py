import dlt as dp
from pyspark.sql.functions import col, xxhash64, to_timestamp

@dp.table(
    name="stg_steam",
    comment="Padronização de tipos e chaves para Steam",
    table_properties={"quality": "staging"}
)
# Regras de Qualidade para manutenibilidade
@dp.expect_or_drop("valid_steam_game_id", "steam_game_id IS NOT NULL")
def stg_steam():
    return (
        dp.read_stream("bronze_steam")
        .select(
            xxhash64(col("id"), col("ingestion_timestamp_utc")).alias("sk_deduplication"),
            col("id").alias("steam_game_id"),
            col("jogo").alias("game_name"),
            col("qtd_jogadores").alias("peak_players"),
            to_timestamp(col("ingestion_timestamp_utc")).alias("ingested_at_utc")
        )
    )