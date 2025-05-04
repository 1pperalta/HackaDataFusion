{{
    config(
        materialized = 'incremental',
        unique_key = 'repo_id',
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
    WHERE repo_id NOT IN (SELECT repo_id FROM {{ this }})
    {% endif %}
),

repo_data AS (
    SELECT
        TRY_PARSE_JSON(raw_data):repo:id::NUMBER AS repo_id,
        TRY_PARSE_JSON(raw_data):repo:name::STRING AS repo_name,
        TRY_PARSE_JSON(raw_data):repo:url::STRING AS repo_url,
        TRY_TO_TIMESTAMP(created_at) AS event_time,
        -- Extraer owner del nombre del repo
        SPLIT_PART(TRY_PARSE_JSON(raw_data):repo:name::STRING, '/', 1) AS owner_login,
        -- Extraer owner id si existe
        TRY_PARSE_JSON(raw_data):repo:owner:id::NUMBER AS owner_id,
        -- Información adicional del repo si existe en el payload
        COALESCE(
            TRY_PARSE_JSON(raw_data):payload:repository:fork::BOOLEAN, 
            FALSE
        ) AS is_fork,
        TRY_PARSE_JSON(raw_data):payload:repository:language::STRING AS language,
        TRY_PARSE_JSON(raw_data):payload:repository:created_at::TIMESTAMP AS repo_created_at,
        TRY_PARSE_JSON(raw_data):payload:repository:updated_at::TIMESTAMP AS repo_updated_at,
        TRY_PARSE_JSON(raw_data):payload:repository:stargazers_count::NUMBER AS stars_count,
        TRY_PARSE_JSON(raw_data):payload:repository:forks_count::NUMBER AS forks_count,
        TRY_PARSE_JSON(raw_data):payload:repository:open_issues_count::NUMBER AS issues_count,
        TRY_PARSE_JSON(raw_data):payload:repository:watchers_count::NUMBER AS watchers_count,
        TRY_PARSE_JSON(raw_data):payload:repository:size::NUMBER AS size
    FROM source_data
    WHERE TRY_PARSE_JSON(raw_data):repo:id IS NOT NULL
),

-- Agrupar y obtener first_seen y last_seen
repo_aggregated AS (
    SELECT
        repo_id,
        MAX(repo_name) AS repo_name, -- Tomar el valor más reciente
        MAX(repo_url) AS repo_url,
        MAX(owner_id) AS owner_id,
        MAX(owner_login) AS owner_login,
        MAX(is_fork) AS is_fork,
        MAX(language) AS language,
        MAX(repo_created_at) AS created_at,
        MAX(repo_updated_at) AS updated_at,
        MIN(event_time) AS first_seen_at,
        MAX(event_time) AS last_seen_at,
        MAX(stars_count) AS stars_count,
        MAX(forks_count) AS forks_count,
        MAX(issues_count) AS issues_count,
        MAX(watchers_count) AS watchers_count,
        MAX(size) AS size
    FROM repo_data
    GROUP BY repo_id
)

{% if is_incremental() %}
-- Para actualizaciones incrementales, combinar con datos existentes
SELECT
    s.repo_id,
    COALESCE(s.repo_name, e.repo_name) AS repo_name,
    COALESCE(s.repo_url, e.repo_url) AS repo_url,
    COALESCE(s.owner_id, e.owner_id) AS owner_id,
    COALESCE(s.owner_login, e.owner_login) AS owner_login,
    COALESCE(s.is_fork, e.is_fork) AS is_fork,
    COALESCE(s.language, e.language) AS language,
    COALESCE(s.created_at, e.created_at) AS created_at,
    COALESCE(s.updated_at, e.updated_at) AS updated_at,
    CASE 
        WHEN e.first_seen_at IS NULL THEN s.first_seen_at
        WHEN s.first_seen_at IS NULL THEN e.first_seen_at
        ELSE LEAST(s.first_seen_at, e.first_seen_at) 
    END AS first_seen_at,
    CASE 
        WHEN e.last_seen_at IS NULL THEN s.last_seen_at
        WHEN s.last_seen_at IS NULL THEN e.last_seen_at
        ELSE GREATEST(s.last_seen_at, e.last_seen_at) 
    END AS last_seen_at,
    COALESCE(s.stars_count, e.stars_count) AS stars_count,
    COALESCE(s.forks_count, e.forks_count) AS forks_count,
    COALESCE(s.issues_count, e.issues_count) AS issues_count,
    COALESCE(s.watchers_count, e.watchers_count) AS watchers_count,
    COALESCE(s.size, e.size) AS size
FROM repo_aggregated s
FULL OUTER JOIN {{ this }} e ON s.repo_id = e.repo_id
{% else %}
-- Para la primera ejecución
SELECT
    repo_id,
    repo_name,
    repo_url,
    owner_id,
    owner_login,
    is_fork,
    language,
    created_at,
    updated_at,
    first_seen_at,
    last_seen_at,
    stars_count,
    forks_count,
    issues_count,
    watchers_count,
    size
FROM repo_aggregated
{% endif %}