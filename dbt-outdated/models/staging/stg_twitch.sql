{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['sk_deduplication'],
    liquid_clustered_by=['ingested_at_utc', 'twitch_game_id'],
    file_format='delta'
) }}

with
source as (
        select * from {{ source("bronze_data", "bronze_twitch") }}
        {% if is_incremental() %}
            where ingestion_timestamp_utc > (select max(ingested_at_utc) from {{ this }})
        {% endif %}
    ),

    flattened as (
        select
            ingestion_timestamp_utc,
            explode(twitch_data) as stream_info
        from source
    )

select
    xxhash64(stream_info.id, ingestion_timestamp_utc) as sk_deduplication,
    -- IDs e Chaves
    stream_info.id as stream_id,
    stream_info.user_id as streamer_id,
    stream_info.game_id as twitch_game_id,

    -- Detalhes
    stream_info.game_name as game_name,

    -- Métricas
    stream_info.viewer_count as viewer_count,

    -- Datas
    to_timestamp(stream_info.started_at) as started_at_utc,
    to_timestamp(ingestion_timestamp_utc) as ingested_at_utc

from flattened