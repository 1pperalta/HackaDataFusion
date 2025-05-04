{{ config(
    materialized = 'table',
    schema = 'GOLD'
) }}

SELECT 
    actor_id,
    COUNT(*) AS total_events,
    COUNT(DISTINCT repo_id) AS unique_repos,
    MIN(created_at) AS first_event,
    MAX(created_at) AS last_event,
    MAX(actor_login) AS actor_login
FROM {{ source('silver', 'SILVER_EVENTS') }} e
GROUP BY actor_id