WITH base AS (
    SELECT DISTINCT
        {{ device_type('user_agent') }} AS device_type,
        {{ browser_type('user_agent') }} AS browser
    FROM {{ ref('silver') }}
)

SELECT *
FROM base