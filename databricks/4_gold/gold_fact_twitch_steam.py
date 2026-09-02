import dlt as dp
from pyspark.sql.functions import col, coalesce, lit

@dp.table(
    name="fact_twitch_steam",
    comment="Tabela Fato: Cruzamento (Inner Join) consolidado entre Twitch e Steam",
    table_properties={"quality": "gold"}
)
def fact_game_performance():
    t = dp.read("gold_twitch_daily").alias("t")
    s = dp.read("gold_steam_daily").alias("s")

    df_join = t.join(s, on="sk_game_performance", how="inner")

    return df_join.select(
        col("t.twitch_game_id"),
        col("s.steam_game_id"),
        coalesce(col("t.game_name"), col("s.game_name"), lit("N/A")).alias("game_name"),
        coalesce(col("t.Month"), col("s.Month"), lit("1900-01-01")).alias("Month"),
        col("t.total_views"),
        col("s.peak_players"),
        col("sk_game_performance")
    )