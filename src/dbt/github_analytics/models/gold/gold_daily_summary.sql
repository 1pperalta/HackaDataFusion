{{ config(
    materialized = 'table',
    schema = 'GOLD'
) }}

SELECT 
    DATE_TRUNC('hour', created_at) AS hour_bucket,
    COUNT(*) AS total_events,
    COUNT(DISTINCT actor_id) AS unique_actors,
    COUNT(DISTINCT repo_id) AS unique_repos
FROM {{ source('silver', 'SILVER_EVENTS') }}
GROUP BY hour_bucket
ORDER BY hour_bucket