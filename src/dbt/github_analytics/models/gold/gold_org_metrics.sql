{{ config(
    materialized = 'table',
    schema = 'GOLD'
) }}

-- Extrayendo la organización del nombre del repo (owner/repo)
WITH events_with_org AS (
    SELECT 
        *,
        SPLIT_PART(repo_name, '/', 1) AS org_name
    FROM {{ source('silver', 'SILVER_EVENTS') }}
    WHERE repo_name LIKE '%/%'
)

SELECT 
    org_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT actor_id) AS unique_actors,
    COUNT(DISTINCT repo_id) AS unique_repos,
    MIN(created_at) AS first_event,
    MAX(created_at) AS last_event
FROM events_with_org
GROUP BY org_name