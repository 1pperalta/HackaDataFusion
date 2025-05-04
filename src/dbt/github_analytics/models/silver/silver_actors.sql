{{
    config(
        materialized = 'incremental',
        unique_key = 'actor_id',
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
    -- Si es incremental, solo procesar datos nuevos
    WHERE actor_id NOT IN (SELECT actor_id FROM {{ this }})
    {% endif %}
),

actor_data AS (
    SELECT
        TRY_PARSE_JSON(raw_data):actor:id::NUMBER AS actor_id,
        TRY_PARSE_JSON(raw_data):actor:login::STRING AS actor_login,
        TRY_PARSE_JSON(raw_data):actor:display_login::STRING AS actor_display_login,
        TRY_PARSE_JSON(raw_data):actor:url::STRING AS actor_url,
        TRY_PARSE_JSON(raw_data):actor:type::STRING AS actor_type,
        TRY_PARSE_JSON(raw_data):actor:site_admin::BOOLEAN AS actor_site_admin,
        TRY_PARSE_JSON(raw_data):actor:avatar_url::STRING AS avatar_url,
        TRY_PARSE_JSON(raw_data):actor:gravatar_id::STRING AS gravatar_id,
        TRY_TO_TIMESTAMP(created_at) AS event_time
    FROM source_data
    WHERE TRY_PARSE_JSON(raw_data):actor:id IS NOT NULL
),

-- Agrupar y obtener first_seen y last_seen
actor_aggregated AS (
    SELECT
        actor_id,
        MAX(actor_login) AS actor_login, -- Tomar el valor más reciente
        MAX(actor_display_login) AS actor_display_login,
        MAX(actor_url) AS actor_url,
        MAX(actor_type) AS actor_type,
        MAX(actor_site_admin) AS actor_site_admin,
        MAX(avatar_url) AS avatar_url,
        MAX(gravatar_id) AS gravatar_id,
        MIN(event_time) AS first_seen_at,
        MAX(event_time) AS last_seen_at
    FROM actor_data
    GROUP BY actor_id
)

{% if is_incremental() %}
-- Para actualizaciones incrementales, combinar con datos existentes
SELECT
    s.actor_id,
    COALESCE(s.actor_login, e.actor_login) AS actor_login,
    COALESCE(s.actor_display_login, e.actor_display_login) AS actor_display_login,
    COALESCE(s.actor_url, e.actor_url) AS actor_url,
    COALESCE(s.actor_type, e.actor_type) AS actor_type,
    COALESCE(s.actor_site_admin, e.actor_site_admin) AS actor_site_admin,
    COALESCE(s.avatar_url, e.avatar_url) AS avatar_url,
    COALESCE(s.gravatar_id, e.gravatar_id) AS gravatar_id,
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
FROM actor_aggregated s
FULL OUTER JOIN {{ this }} e ON s.actor_id = e.actor_id
{% else %}
-- Para la primera ejecución
SELECT
    actor_id,
    actor_login,
    actor_display_login,
    actor_url,
    actor_type,
    actor_site_admin,
    avatar_url,
    gravatar_id,
    first_seen_at,
    last_seen_at
FROM actor_aggregated
{% endif %}