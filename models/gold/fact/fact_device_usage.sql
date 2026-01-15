WITH logs_silver AS (

    SELECT
        {{ device_type('user_agent') }} AS device_type,
        {{ browser_type('user_agent') }} AS browser,
        date_trunc('hour', event_ts) AS hour,
        ip
    FROM {{ ref('silver') }}

),

dim_device AS (

    SELECT
        device_id,
        device_type,
        browser
    FROM {{ ref('dim_device') }}

),

dim_time AS (

    SELECT
        time_id,
        hour
    FROM {{ ref('dim_time') }}

),

joined AS (

    SELECT
        dd.device_id,
        dt.time_id,
        ls.ip
    FROM logs_silver ls
    JOIN dim_device dd
        ON ls.device_type = dd.device_type
       AND ls.browser = dd.browser
    JOIN dim_time dt
        ON ls.hour = dt.hour

)

SELECT
    j.device_id,
    j.time_id,
    COUNT(DISTINCT j.ip) AS nb_visitors
FROM joined j
GROUP BY
    j.device_id,
    j.time_id
