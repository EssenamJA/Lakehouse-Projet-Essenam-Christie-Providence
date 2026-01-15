WITH base AS (
    SELECT DISTINCT
        LOWER(referer) AS referer
    FROM {{ ref('silver') }}
    WHERE referer IS NOT NULL
)

SELECT
    referer,
    CASE
        WHEN referer LIKE '%google%' THEN 'google'
        ELSE 'other'
    END AS referer_type
FROM base