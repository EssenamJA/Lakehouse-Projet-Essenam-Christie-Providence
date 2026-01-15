WITH referers AS (
    SELECT DISTINCT
        LOWER(referer) AS referer
    FROM {{ ref('silver') }}
    WHERE referer IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY referer) AS referer_id,
    referer,
    CASE
        WHEN referer LIKE '%google%' THEN 'google'
        ELSE 'other'
    END AS referer_type
FROM referers
