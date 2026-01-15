WITH logs_silver AS (

    SELECT
        page,
        date_trunc('hour', event_ts) AS hour
    FROM {{ ref('silver') }}
    WHERE status_code >= 400

),

dim_page AS (

    SELECT
        page_id,
        page
    FROM {{ ref('dim_page') }}

),

dim_time AS (

    SELECT
        time_id,
        hour
    FROM {{ ref('dim_time') }}

),

joined AS (

    SELECT
        dp.page_id,
        dt.time_id
    FROM silver ls
    JOIN dim_page dp
        ON ls.page = dp.page
    JOIN dim_time dt
        ON ls.hour = dt.hour

)

SELECT
    j.page_id,
    j.time_id,
    COUNT(*) AS nb_errors
FROM joined j
GROUP BY
    j.page_id,
    j.time_id
