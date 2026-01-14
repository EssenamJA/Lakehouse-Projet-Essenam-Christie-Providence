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
    is_bot
from {{ ref('stg_logs') }}
