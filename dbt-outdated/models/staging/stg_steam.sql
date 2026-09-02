{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['sk_deduplication'],
    liquid_clustered_by=['ingested_at_utc', 'steam_game_id'],
    file_format='delta'
) }}

with
steam as (
        select * from {{ source("bronze_data", "bronze_steam") }}
        {% if is_incremental() %}
            where ingestion_timestamp_utc > (select max(ingested_at_utc) from {{ this }})
        {% endif %}
    )

select 
    xxhash64(id, ingestion_timestamp_utc) as sk_deduplication,
    -- IDs e Chaves
    id as steam_game_id,
    jogo as game_name,

    -- Métricas
    qtd_jogadores as peak_players,

    -- Datas
    to_timestamp(ingestion_timestamp_utc) as ingested_at_utc

from steam