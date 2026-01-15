WITH pages AS (
    SELECT DISTINCT page
    FROM {{ ref('silver') }}
    WHERE page IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY page) AS page_id,
    page
FROM pages