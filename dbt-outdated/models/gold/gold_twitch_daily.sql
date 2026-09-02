{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_game_performance',
    liquid_clustered_by=['Month', 'twitch_game_id'],
    file_format='delta'
) }}

{% if is_incremental() %}
    {% set max_month = dbt_utils.get_single_value("select max(Month) from " ~ this) %}
{% endif %}

WITH silver_twitch_agg AS (
    SELECT * FROM {{ ref('silver_twitch_dedupe') }}
    {% if is_incremental() %}
      WHERE ingested_at_utc >= '{{ max_month }}'
    {% endif %}
),

aggregated_data AS (
    SELECT
        CAST(date_format(ingested_at_utc, 'yyyy-MM-dd') AS DATE) AS Month,
        xxhash64(twitch_game_id, Month) as sk_game_performance,
        twitch_game_id,
        game_name,
        SUM(viewer_count) as total_views
    FROM silver_twitch_agg
    GROUP BY 
        Month, 
        twitch_game_id, 
        game_name
)

SELECT 
    *
FROM aggregated_data