WITH logs_silver AS (

    SELECT
        page,
        event_ts AS hour
    FROM {{ ref('silver') }}

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

renamed AS (

    SELECT
        dp.page_id,
        dt.time_id
    FROM {{ ref('silver') }} ls
    JOIN dim_page dp
        ON ls.page = dp.page
    JOIN dim_time dt
        ON ls.event_ts = dt.hour

)

SELECT
    page_id,
    time_id,
    COUNT(*) AS nb_hits
FROM renamed
GROUP BY page_id, time_id
