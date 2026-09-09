import dlt as dp
from pyspark.sql.functions import col, to_date, max as _max, lower, trim, coalesce, xxhash64

@dp.table(
    name="gold_game_analytics_daily",
    comment="",
    table_properties={"quality": "gold"},
    cluster_by=["month", "sk_game_performance"]
)
def gold_game_analytics_daily():
    steam_daily = (
        dp.read("silver_steam")
        .withColumn("month", to_date(col("ingested_at_utc")))
        .withColumn("game_name", lower(trim(col("game_name"))))
        .withColumn("sk_game_performance", xxhash64(col("month"), col("game_name")))
        .groupBy("sk_game_performance", "month")
        .agg(
            _max("game_name").alias("steam_game_name"),
            _max("peak_players").alias("steam_peak_players") 
        )
    )

    twitch_daily = (
        dp.read("silver_twitch")
        .withColumn("month", to_date(col("ingested_at_utc")))
        .withColumn("game_name", lower(trim(col("game_name"))))
        .withColumn("sk_game_performance", xxhash64(col("month"), col("game_name")))
        .groupBy("sk_game_performance", "month")
        .agg(
            _max("game_name").alias("twitch_game_name"),
            _max("viewer_count").alias("twitch_peak_views")
        )
    )

    return (
        steam_daily.join(
            twitch_daily,
            on=["sk_game_performance", "month"],
            how="inner"
        )
        .select(
            col("sk_game_performance"),
            col("month"),
            coalesce(col("steam_game_name"), col("twitch_game_name")).alias("game_name"),
            col("steam_peak_players"),
            col("twitch_peak_views")
        )
    )