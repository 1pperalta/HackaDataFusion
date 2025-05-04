{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        schema = 'SILVER'
    )
}}

WITH source_data AS (
    SELECT
        event_id,
        event_hash,
        event_type,
        created_at,
        actor_id,
        repo_id,
        raw_data,
        payload,
        processed_at,
        hour_bucket
    FROM {{ source('bronze', 'EVENTS') }}
    
    {% if is_incremental() %}
    -- Si es incremental, solo procesar datos nuevos
    WHERE event_hash NOT IN (SELECT event_hash FROM {{ this }})
    {% endif %}
),

parse_data AS (
    SELECT
        event_id,
        event_hash,
        event_type,
        TRY_TO_TIMESTAMP(created_at) AS created_at_ts,
        actor_id,
        repo_id,
        -- Extraer org_id del raw_data si existe
        TRY_PARSE_JSON(raw_data):org:id::NUMBER AS org_id,
        -- Detectar si es un bot basado en el login del actor
        CASE 
            WHEN REGEXP_LIKE(TRY_PARSE_JSON(raw_data):actor:login::STRING, 
                             '(-bot|[._]bot|bot[._]|^bot-|^bot$|\[bot\]$)', 'i') 
            THEN TRUE 
            ELSE FALSE 
        END AS is_bot,
        -- Verificar si el evento es público
        COALESCE(TRY_PARSE_JSON(raw_data):public::BOOLEAN, TRUE) AS public,
        hour_bucket,
        processed_at,
        CURRENT_TIMESTAMP() AS silver_processed_at
    FROM source_data
)

SELECT * FROM parse_data