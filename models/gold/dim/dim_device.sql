WITH devices AS (
    SELECT DISTINCT
        {{ device_type('user_agent') }} AS device_type,
        {{ browser_type('user_agent') }} AS browser
    FROM {{ ref('silver') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY device_type, browser) AS device_id,
    device_type,
    browser
FROM devices
