WITH base AS (
    SELECT
        page,
        date_trunc('hour', event_ts) AS hour
    FROM {{ ref('silver') }}
),

joined AS (
    SELECT
        p.page_id,
        t.time_id
    FROM base b
    JOIN {{ ref('dim_page') }} p ON b.page = p.page
    JOIN {{ ref('dim_time') }} t ON b.hour = t.hour
)

SELECT
    page_id,
    time_id,
    COUNT(*) AS nb_hits
FROM joined
GROUP BY page_id, time_id
