import dlt as dp
from pyspark.sql.functions import col, sum as _sum

@dp.table(
    name="silver_twitch",
    comment="",
    table_properties={"quality": "silver"},
    cluster_by=["ingested_at_utc", "twitch_game_id"]
)
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