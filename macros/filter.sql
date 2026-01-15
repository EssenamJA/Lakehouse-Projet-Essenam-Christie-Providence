{% macro filter_static_assets(page_col) %}
    {{ page_col }} NOT LIKE '%.css'
    AND {{ page_col }} NOT LIKE '%.js'
    AND {{ page_col }} NOT LIKE '%.png'
    AND {{ page_col }} NOT LIKE '%.jpg'
    AND {{ page_col }} NOT LIKE '%.jpeg'
    AND {{ page_col }} NOT LIKE '%.gif'
    AND {{ page_col }} NOT LIKE '%.ico'
{% endmacro %}
