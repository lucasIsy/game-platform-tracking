import dlt as dp

@dp.table(
    name="silver_steam",
    comment="",
    table_properties={"quality": "silver"},
    cluster_by=["ingested_at_utc", "steam_game_id"]
)
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