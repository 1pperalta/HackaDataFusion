{{ config(
    materialized = 'table',
    schema = 'GOLD'
) }}

SELECT 
    repo_id,
    MAX(repo_name) AS repo_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT actor_id) AS unique_actors,
    MIN(created_at) AS first_event,
    MAX(created_at) AS last_event
FROM {{ source('silver', 'SILVER_EVENTS') }} e
GROUP BY repo_id