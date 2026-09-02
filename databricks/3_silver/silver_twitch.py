import dlt as dp
from pyspark.sql.functions import sum as _sum

@dp.table(
    name="silver_twitch",
    comment="Agregação estática (Materialized View) de views da Twitch",
    table_properties={"quality": "silver"}
)
def silver_twitch():
    # Leitura como Materialized View (Lote em vez de Stream)
    return (
        dp.read("stg_twitch")
        .groupBy(
            "twitch_game_id",
            "game_name",
            "ingested_at_utc"
        )
        .agg(_sum("viewer_count").alias("viewer_count"))
    )