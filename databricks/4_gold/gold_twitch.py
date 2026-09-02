import dlt as dp
from pyspark.sql.functions import col, to_date, xxhash64, sum as _sum

@dp.table(
    name="gold_twitch_daily",
    comment="Camada Gold: Agregação diária de views da Twitch",
    table_properties={"quality": "gold"}
)
def gold_twitch_daily():
    return (
        dp.read("silver_twitch")
        .withColumn("Month", to_date(col("ingested_at_utc")))
        .withColumn("sk_game_performance", xxhash64(col("twitch_game_id"), col("Month")))
        .groupBy(
            "Month", 
            "twitch_game_id", 
            "game_name", 
            "sk_game_performance"
        )
        .agg(_sum("viewer_count").alias("total_views"))
    )