import dlt as dp
from pyspark.sql.functions import col, sum as _sum

@dp.table(
    name="silver_twitch",
    comment="",
    table_properties={"quality": "silver"},
    cluster_by=["ingested_at_utc", "twitch_game_id"]
)

@dp.expect_or_drop("game_id_not_null", "twitch_game_id IS NOT NULL")
@dp.expect_or_drop("timestamp_not_null", "ingested_at_utc IS NOT NULL")
@dp.expect("valid_viewer_count", "viewer_count >= 0")
@dp.expect("valid_game_name", "game_name IS NOT NULL AND length(trim(game_name)) > 0")

def silver_twitch():
    return (
        dp.read_stream("stg_twitch")
        .withWatermark("ingested_at_utc", "1 minute")
        .dropDuplicates(["sk_deduplication"])
        .groupBy(
            "twitch_game_id",
            "game_name",
            "ingested_at_utc"
        )
        .agg(_sum("viewer_count").alias("viewer_count"))
    )

# DLQ
@dp.table(
    name="silver_twitch_dlq",
    comment="Dead Letter Queue / Quarentena para registros com chaves nulas ou corrompidas",
    table_properties={"quality": "quarantine"}
)
def silver_twitch_dlq():
    # Insere os registros que não passaram pelos testes.
    return (
        dp.read_stream("stg_twitch")
        .filter("twitch_game_id IS NULL OR ingested_at_utc IS NULL")
    )