{% macro generate_partitions_to_replace(lookback) %}
    {% set partitions_to_replace = [] %}
    {% for i in range(lookback) %}
        {% do partitions_to_replace.append("current_date - " ~ i) %}
    {% endfor %}
    {{ return(partitions_to_replace) }}
{% endmacro %}