import io
import polars as pl

def steam_data_to_stream_format(dados_steam: list[dict]) -> io.BytesIO:
    buffer = io.BytesIO()
    
    schema = {
        "ingestion_timestamp_utc": pl.Int64,
        "id": pl.String,
        "jogo": pl.String,
        "qtd_jogadores": pl.Int64
    }
    
    df = pl.DataFrame(dados_steam, schema=schema)
    
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    return buffer

def twitch_data_to_stream_format(dados_twitch: dict) -> io.BytesIO:
    buffer = io.BytesIO()
    
    timestamp = dados_twitch.get("ingestion_timestamp_utc")
    streams = dados_twitch.get("twitch_data") or [{}]
    
    schema = {
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
        "is_mature": pl.Boolean,
    }

    df = (
        pl.DataFrame(streams, schema=schema)
        .with_columns([
            pl.lit(timestamp, dtype=pl.Int64).alias("ingestion_timestamp_utc"),
            pl.col("started_at")
              .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False)
              .dt.epoch("s")
        ])
        .select(["ingestion_timestamp_utc", *schema.keys()])
    )

    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    return buffer