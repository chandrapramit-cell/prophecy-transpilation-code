
{% macro create_data(n, alias) %}
{% for x in range(n) %}
    SELECT {{ x + 1 }} AS {{ alias }}
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}
{% endmacro %}
