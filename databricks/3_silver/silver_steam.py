import dlt as dp

@dp.table(
    name="silver_steam",
    comment="",
    table_properties={"quality": "silver"},
    cluster_by=["ingested_at_utc", "steam_game_id"]
)

@dp.expect_or_drop("game_id_not_null", "steam_game_id IS NOT NULL")
@dp.expect_or_drop("timestamp_not_null", "ingested_at_utc IS NOT NULL")
@dp.expect("valid_peak_players", "peak_players >= 0")
@dp.expect("valid_game_name", "game_name IS NOT NULL AND length(trim(game_name)) > 0")

def silver_steam():
    return (
        dp.read_stream("stg_steam")
        .withWatermark("ingested_at_utc", "1 minute")
        .dropDuplicates(["sk_deduplication"])
        .select(
            "steam_game_id",
            "game_name",
            "peak_players",
            "ingested_at_utc"
        )
    )

# DLQ
@dp.table(
    name="silver_steam_dlq",
    comment="Dead Letter Queue / Quarentena para registros com chaves nulas ou corrompidas",
    table_properties={"quality": "quarantine"}
)
def silver_steam_dlq():
    # Insere os registros que não passaram pelos testes.
    return (
        dp.read_stream("stg_steam")
        .filter("steam_game_id IS NULL OR ingested_at_utc IS NULL")
    )