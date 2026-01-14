{% macro clean_page(page_col) %}
    regexp_replace(
        lower({{ page_col }}),
        '/$',
        ''
    )
{% endmacro %}


{% macro referer_host(referer_col) %}
    split_part(
        regexp_replace({{ referer_col }}, '^https?://', ''),
        '/',
        1
    )
{% endmacro %}


{% macro is_bot(user_agent_col) %}
    {{ user_agent_col }} ilike '%bot%'
{% endmacro %}
