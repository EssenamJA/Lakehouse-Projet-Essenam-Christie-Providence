WITH logs_silver AS (

    SELECT
        LOWER(referer) AS referer,
        date_trunc('hour', event_ts) AS hour
    FROM {{ ref('silver') }}
    WHERE referer IS NOT NULL

),

dim_referer AS (

    SELECT
        referer_id,
        referer
    FROM {{ ref('dim_referer') }}

),

dim_time AS (

    SELECT
        time_id,
        hour
    FROM {{ ref('dim_time') }}

),

joined AS (

    SELECT
        dr.referer_id,
        dt.time_id
    FROM silver ls
    JOIN dim_referer dr
        ON ls.referer = dr.referer
    JOIN dim_time dt
        ON ls.hour = dt.hour

)

SELECT
    j.referer_id,
    j.time_id,
    COUNT(*) AS nb_visits
FROM joined j
GROUP BY
    j.referer_id,
    j.time_id
