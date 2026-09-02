{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_game_performance',
    file_format='delta'
) }}

WITH twitch AS (
    SELECT * FROM {{ ref('gold_twitch_daily') }}
    {% if is_incremental() %}
    WHERE Month >= (SELECT MAX(Month) FROM {{ this }})
    {% endif %}
),

steam AS (
    SELECT * FROM {{ ref('gold_steam_daily') }}
    {% if is_incremental() %}
    WHERE Month >= (SELECT MAX(Month) FROM {{ this }})
    {% endif %}
)

SELECT
    t.twitch_game_id,
    s.steam_game_id,

    COALESCE(t.game_name, s.game_name, 'N/A') AS game_name,
    COALESCE(t.Month, s.Month, '1900-01') AS Month,

    t.total_views,
    s.peak_players,

    COALESCE(t.sk_game_performance, s.sk_game_performance, 'N/A') AS sk_game_performance
FROM twitch as t
INNER JOIN steam as s
    ON s.sk_game_performance = t.sk_game_performance