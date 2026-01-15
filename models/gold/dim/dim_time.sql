WITH base AS (
    SELECT DISTINCT
        date_trunc('hour', event_ts) AS hour
    FROM {{ ref('silver') }}
)

SELECT
    hour,
    DATE(hour) AS date,
    EXTRACT(hour FROM hour) AS hour_of_day,
    EXTRACT(day FROM hour) AS day,
    EXTRACT(month FROM hour) AS month,
    EXTRACT(year FROM hour) AS year
FROM base
