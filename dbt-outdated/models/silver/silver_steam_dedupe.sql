{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ingested_at_utc','steam_game_id'],
    liquid_clustered_by=['ingested_at_utc', 'steam_game_id'],
    file_format='delta'
) }}

WITH silver_steam_dedupe AS (
    SELECT * FROM {{ ref('stg_steam') }}
    {% if is_incremental() %}
      WHERE ingested_at_utc > (SELECT max(ingested_at_utc) FROM {{ this }})
    {% endif %}
)

SELECT DISTINCT
    sk_deduplication,
    steam_game_id,
    game_name,
    peak_players,
    ingested_at_utc
FROM silver_steam_dedupe