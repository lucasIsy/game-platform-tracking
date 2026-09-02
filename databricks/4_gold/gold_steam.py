import dlt as dp
from pyspark.sql.functions import col, to_date, xxhash64, sum as _sum

@dp.table(
    name="gold_steam_daily",
    comment="Camada Gold: Agregação diária de pico de jogadores da Steam",
    table_properties={"quality": "gold"}
)
def gold_steam_daily():
    return (
        dp.read("silver_steam")
        .withColumn("Month", to_date(col("ingested_at_utc")))
        .withColumn("sk_game_performance", xxhash64(col("steam_game_id"), col("Month")))
        .groupBy(
            "Month", 
            "steam_game_id", 
            "game_name", 
            "sk_game_performance"
        )
        .agg(_sum("peak_players").alias("peak_players"))
    )