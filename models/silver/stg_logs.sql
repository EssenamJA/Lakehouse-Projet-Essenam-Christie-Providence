{{ config(materialized='table') }}

select
    log,

    -- IP
    split_part(log, ' ', 1) as ip,

    -- TIMESTAMP
    to_timestamp(
        trim(
            replace(
                split_part(split_part(log, '[', 2), ']', 1),
                ' ',
                ''
            )
        ),
        'DD/Mon/YYYY:HH24:MI:SS'
    ) as event_ts,

    -- requête complète
    substring(log from '"([^"]+)"') as request,

    -- méthode HTTP
    split_part(substring(log from '"([^"]+)"'), ' ', 1) as http_method,

    -- page
    split_part(substring(log from '"([^"]+)"'), ' ', 2) as page,

    -- page normalisée (sans slash final)
    regexp_replace(
        lower(split_part(substring(log from '"([^"]+)"'), ' ', 2)),
        '/$',
        ''
    ) as page_clean,

    -- status code
    split_part(log, ' ', 9)::int as status_code,

    -- referer (URL complète)
    split_part(log, '"', 4) as referer,

    -- host du referer
    split_part(
        regexp_replace(split_part(log, '"', 4), '^https?://', ''),
        '/',
        1
    ) as referer_host,

    -- user agent
    split_part(log, '"', 6) as user_agent,

    -- détection bot (technique, pas métier)
    split_part(log, '"', 6) ilike '%bot%' as is_bot

from {{ source('bronze', 'bronze') }}
