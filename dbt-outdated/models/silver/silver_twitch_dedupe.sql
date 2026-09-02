{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ingested_at_utc', 'twitch_game_id'],
    liquid_clustered_by=['ingested_at_utc', 'twitch_game_id'],
    file_format='delta'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_twitch') }}
    {% if is_incremental() %}
      -- Filtro incremental para processar apenas o novo lote
      WHERE ingested_at_utc > (SELECT MAX(ingested_at_utc) FROM {{ this }})
    {% endif %}
),

deduplicado AS (
    SELECT
        sk_deduplication, -- Usando o Hash criado na Staging
        twitch_game_id,
        game_name,
        ingested_at_utc,
        viewer_count
    FROM source_data
    QUALIFY row_number() OVER (
        PARTITION BY sk_deduplication -- Muito mais rápido que particionar por strings
        ORDER BY ingested_at_utc DESC
    ) = 1
)

SELECT
    sk_deduplication,
    twitch_game_id,
    game_name,
    ingested_at_utc,
    SUM(viewer_count) as viewer_count
FROM deduplicado
GROUP BY
    twitch_game_id,
    game_name,
    ingested_at_utc,
    sk_deduplication