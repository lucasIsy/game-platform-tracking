{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_game_performance',
    liquid_clustered_by=['Month', 'steam_game_id'],
    file_format='delta'
) }}

{% if is_incremental() %}
    {% set max_month = dbt_utils.get_single_value("select max(Month) from " ~ this) %}
{% endif %}

WITH silver_steam_agg AS (
    SELECT * FROM {{ ref('silver_steam_dedupe') }}
    {% if is_incremental() %}
        WHERE ingested_at_utc >= '{{ max_month }}'
    {% endif %}
),

aggregated_data AS (
    SELECT
        CAST(date_format(ingested_at_utc, 'yyyy-MM-dd') AS DATE) AS Month,
        xxhash64(steam_game_id, Month) as sk_game_performance,
        steam_game_id,
        game_name,
        SUM(peak_players) as peak_players
    FROM silver_steam_agg
    GROUP BY 
    Month, 
    steam_game_id, 
    game_name
)

SELECT 
    *
FROM aggregated_data