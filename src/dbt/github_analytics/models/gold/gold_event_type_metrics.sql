{{ config(
    materialized = 'table',
    schema = 'GOLD'
) }}

SELECT 
    event_type,
    COUNT(*) AS count
FROM {{ source('silver', 'SILVER_EVENTS') }}
GROUP BY event_type
ORDER BY count DESC