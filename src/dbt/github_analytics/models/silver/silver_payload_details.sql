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
        event_type,
        payload,
        raw_data
    FROM {{ source('bronze', 'EVENTS') }}
    
    {% if is_incremental() %}
    -- Si es incremental, solo procesar datos nuevos
    WHERE event_id NOT IN (SELECT event_id FROM {{ this }})
    {% endif %}
),

parsed_payload AS (
    SELECT
        event_id,
        event_type,
        -- Extraer campos comunes
        TRY_PARSE_JSON(payload):action::STRING AS payload_action,
        
        -- Extraer IDs específicos según el tipo de evento
        CASE 
            WHEN event_type = 'IssuesEvent' THEN 
                TRY_PARSE_JSON(payload):issue:id::NUMBER
            ELSE NULL
        END AS payload_issue_id,
        
        CASE 
            WHEN event_type = 'PullRequestEvent' THEN 
                TRY_PARSE_JSON(payload):pull_request:id::NUMBER
            ELSE NULL
        END AS payload_pull_request_id,
        
        CASE 
            WHEN event_type IN ('IssueCommentEvent', 'CommitCommentEvent', 'PullRequestReviewCommentEvent') THEN 
                TRY_PARSE_JSON(payload):comment:id::NUMBER
            ELSE NULL
        END AS payload_comment_id,
        
        -- Campos para eventos de Push
        CASE 
            WHEN event_type = 'PushEvent' THEN 
                TRY_PARSE_JSON(payload):size::NUMBER
            ELSE NULL
        END AS payload_push_size,
        
        -- Campos para eventos de referencia (Push, Create, Delete)
        CASE 
            WHEN event_type IN ('PushEvent', 'CreateEvent', 'DeleteEvent') THEN 
                TRY_PARSE_JSON(payload):ref::STRING
            ELSE NULL
        END AS payload_ref,
        
        CASE 
            WHEN event_type IN ('CreateEvent', 'DeleteEvent') THEN 
                TRY_PARSE_JSON(payload):ref_type::STRING
            ELSE NULL
        END AS payload_ref_type,
        
        -- Campos adicionales para eventos de Push
        CASE 
            WHEN event_type = 'PushEvent' THEN 
                TRY_PARSE_JSON(payload):head::STRING
            ELSE NULL
        END AS payload_head,
        
        CASE 
            WHEN event_type = 'PushEvent' THEN 
                TRY_PARSE_JSON(payload):before::STRING
            ELSE NULL
        END AS payload_before,
        
        CASE 
            WHEN event_type = 'PushEvent' THEN 
                TRY_PARSE_JSON(payload):size::NUMBER
            ELSE NULL
        END AS payload_size,
        
        CASE 
            WHEN event_type = 'PushEvent' THEN 
                TRY_PARSE_JSON(payload):distinct_size::NUMBER
            ELSE NULL
        END AS payload_distinct_size
    FROM source_data
)

SELECT * FROM parsed_payload