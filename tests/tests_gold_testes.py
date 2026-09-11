import pytest
from pyspark.sql import SparkSession
from transformations.gold_twitch_steam_daily import agregar_metricas_diarias, consolidar_gold

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_join_e_coalesce_nomes(spark):
    """
    Garante que jogos com nomes idênticos (mesmo com espaços) sofram INNER JOIN
    e que o coalesce priorize o nome correto.
    """
    # 1. Dados simulados da Steam
    steam_data = [
        ("2026-09-09 10:00:00", "  Big Walk  ", 500)
    ]
    # 2. Dados simulados da Twitch
    twitch_data = [
        ("2026-09-09 14:00:00", "Big Walk", 1200)
    ]
    # 3. Cria DataFrames
    df_steam = spark.createDataFrame(steam_data, ["ingested_at_utc", "game_name", "peak_players"])
    df_twitch = spark.createDataFrame(twitch_data, ["ingested_at_utc", "game_name", "viewer_count"])
    
    # 4. Executa as funções REAIS
    steam_agg = agregar_metricas_diarias(
        df_steam,
        "peak_players",
        "steam_peak_players",
        "steam_game_name"
    )
    twitch_agg = agregar_metricas_diarias(
        df_twitch,
        "viewer_count",
        "twitch_peak_views",
        "twitch_game_name"
    )
    
    df_resultado = consolidar_gold(steam_agg, twitch_agg)
    resultado = df_resultado.collect()

    # 5. Asserções
    assert len(resultado) == 1
    assert resultado[0]["game_name"] == "Big Walk"
    assert resultado[0]["steam_peak_players"] == 500
    assert resultado[0]["twitch_peak_views"] == 1200