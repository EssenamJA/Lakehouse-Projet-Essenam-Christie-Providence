WITH base AS (
    SELECT DISTINCT page
    FROM {{ ref('silver') }}
    WHERE page IS NOT NULL
)

SELECT
    page
FROM base