import io
import polars as pl

def steam_data_to_stream_format(dados_steam: list[dict]) -> io.BytesIO:
    """
    Recebe a lista de dicionários da extração da Steam, garante a tipagem correta
    e converte para um buffer Parquet na memória.
    """
    buffer = io.BytesIO()
    
    # Tabelas da etapa extract
    schema = {
        "ingestion_timestamp_utc": pl.String,
        "id": pl.Int64,
        "jogo": pl.String,
        "qtd_jogadores": pl.Int64
    }
    
    df = pl.DataFrame(dados_steam, schema=schema)
    
    df.write_parquet(buffer, compression="snappy")

    buffer.seek(0)
    return buffer

def twitch_data_to_stream_format(dados_twitch: dict) -> io.BytesIO:
    """
    Recebe o dicionário da extração da Twitch, achata a lista de streams,
    injeta o timestamp de ingestão e converte para Parquet.
    """
    buffer = io.BytesIO()
    
    timestamp = dados_twitch.get("ingestion_timestamp_utc")
    streams_list = dados_twitch.get("twitch_data", [])
    
    # Se a lista de streams estiver vazia, cria um DataFrame vazio com a estrutura correta
    if not streams_list:
        df = pl.DataFrame({
            "ingestion_timestamp_utc": [timestamp],
            "id": [None],
            "user_id": [None],
            "user_login": [None],
            "user_name": [None],
            "game_id": [None],
            "game_name": [None],
            "type": [None],
            "title": [None],
            "viewer_count": [None],
            "started_at": [None],
            "language": [None],
            "is_mature": [None]
        }, 
        schema={
            "ingestion_timestamp_utc": pl.String,
            "id": pl.String,
            "user_id": pl.String,
            "user_login": pl.String,
            "user_name": pl.String,
            "game_id": pl.String,
            "game_name": pl.String,
            "type": pl.String,
            "title": pl.String,
            "viewer_count": pl.Int64,
            "started_at": pl.String,
            "language": pl.String,
            "is_mature": pl.Boolean
        })
    else:

        df_streams = pl.DataFrame(streams_list)
        
        df = df_streams.with_columns([
            pl.lit(timestamp).alias("ingestion_timestamp_utc"),
            pl.col("id").cast(pl.String),
            pl.col("viewer_count").cast(pl.Int64),
            pl.col("is_mature").cast(pl.Boolean)
        ])
        
        df = df.select(["ingestion_timestamp_utc"] + [col for col in df.columns if col != "ingestion_timestamp_utc"])

    # Grava no buffer
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    
    return buffer