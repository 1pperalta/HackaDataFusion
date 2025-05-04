{{
    config(
        materialized = 'incremental',
        unique_key = 'org_id',
        schema = 'SILVER'
    )
}}

WITH source_data AS (
    SELECT
        raw_data,
        event_id,
        created_at
    FROM {{ source('bronze', 'EVENTS') }}
    
    {% if is_incremental() %}
    WHERE TRY_PARSE_JSON(raw_data):org:id::NUMBER NOT IN (SELECT org_id FROM {{ this }})
    {% endif %}
),

org_data AS (
    SELECT
        TRY_PARSE_JSON(raw_data):org:id::NUMBER AS org_id,
        TRY_PARSE_JSON(raw_data):org:login::STRING AS org_login,
        TRY_PARSE_JSON(raw_data):org:url::STRING AS org_url,
        TRY_PARSE_JSON(raw_data):org:avatar_url::STRING AS avatar_url,
        TRY_PARSE_JSON(raw_data):org:description::STRING AS description,
        TRY_TO_TIMESTAMP(created_at) AS event_time
    FROM source_data
    WHERE TRY_PARSE_JSON(raw_data):org:id IS NOT NULL
),

-- Agrupar y obtener first_seen y last_seen
org_aggregated AS (
    SELECT
        org_id,
        MAX(org_login) AS org_login, -- Tomar el valor más reciente
        MAX(org_url) AS org_url,
        MAX(avatar_url) AS avatar_url,
        MAX(description) AS description,
        MIN(event_time) AS first_seen_at,
        MAX(event_time) AS last_seen_at
    FROM org_data
    GROUP BY org_id
)

{% if is_incremental() %}
-- Para actualizaciones incrementales, combinar con datos existentes
SELECT
    s.org_id,
    COALESCE(s.org_login, e.org_login) AS org_login,
    COALESCE(s.org_url, e.org_url) AS org_url,
    COALESCE(s.avatar_url, e.avatar_url) AS avatar_url,
    COALESCE(s.description, e.description) AS description,
    CASE 
        WHEN e.first_seen_at IS NULL THEN s.first_seen_at
        WHEN s.first_seen_at IS NULL THEN e.first_seen_at
        ELSE LEAST(s.first_seen_at, e.first_seen_at) 
    END AS first_seen_at,
    CASE 
        WHEN e.last_seen_at IS NULL THEN s.last_seen_at
        WHEN s.last_seen_at IS NULL THEN e.last_seen_at
        ELSE GREATEST(s.last_seen_at, e.last_seen_at) 
    END AS last_seen_at
FROM org_aggregated s
FULL OUTER JOIN {{ this }} e ON s.org_id = e.org_id
{% else %}
-- Para la primera ejecución
SELECT
    org_id,
    org_login,
    org_url,
    avatar_url,
    description,
    first_seen_at,
    last_seen_at
FROM org_aggregated
{% endif %}