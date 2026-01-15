{{ config(
    materialized='table',
    schema='prod'
) }}

SELECT
    referer,
    COUNT(*) AS nb_visits,

    CASE
        WHEN referer ILIKE '%google%' THEN 'google'
        ELSE 'other'
    END AS referer_type

FROM {{ ref('logs_silver') }}
WHERE referer IS NOT NULL
GROUP BY referer, referer_type
ORDER BY nb_visits DESC;
