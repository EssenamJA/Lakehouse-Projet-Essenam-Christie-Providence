WITH times AS (
    SELECT DISTINCT
        event_ts AS hour
    FROM {{ ref('silver') }}
    WHERE event_ts IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY hour) AS time_id,
    hour,
    DATE(hour) AS date,
    EXTRACT(hour FROM hour) AS hour_of_day,
    EXTRACT(day FROM hour) AS day,
    EXTRACT(month FROM hour) AS month,
    EXTRACT(year FROM hour) AS year
FROM times
