{% macro device_type(user_agent_col) %}
    CASE
        WHEN LOWER({{ user_agent_col }}) LIKE '%mobile%' THEN 'mobile'
        ELSE 'desktop'
    END
{% endmacro %}


{% macro browser_type(user_agent_col) %}
    CASE
        WHEN LOWER({{ user_agent_col }}) LIKE '%chrome%' THEN 'chrome'
        WHEN LOWER({{ user_agent_col }}) LIKE '%firefox%' THEN 'firefox'
        WHEN LOWER({{ user_agent_col }}) LIKE '%safari%' THEN 'safari'
        WHEN LOWER({{ user_agent_col }}) LIKE '%edge%' THEN 'edge'
        ELSE 'other'
    END
{% endmacro %}
