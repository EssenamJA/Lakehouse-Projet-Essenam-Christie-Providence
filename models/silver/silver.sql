{{ config(materialized='table') }}

select
    ip,
    event_ts,
    request,
    http_method,
    page,
    page_clean,
    status_code,
    referer,
    referer_host,
    user_agent,
    is_bot,

    -- Device
    CASE
        WHEN LOWER(user_agent) LIKE '%mobile%' THEN 'mobile'
        ELSE 'desktop'
    END AS device_type,

    -- Browser
    CASE
        WHEN LOWER(user_agent) LIKE '%chrome%' THEN 'chrome'
        WHEN LOWER(user_agent) LIKE '%firefox%' THEN 'firefox'
        WHEN LOWER(user_agent) LIKE '%safari%' THEN 'safari'
        ELSE 'other'
    END AS browser
from {{ ref('stg_logs') }}
