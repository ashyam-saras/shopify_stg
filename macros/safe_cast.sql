{% macro safe_cast(col_name, data_type) %}
    {% if target.type == 'snowflake' %}
        TRY_CAST({{ col_name }} AS {{ data_type }}) 
    {% else %}
        SAFE_CAST({{ col_name }} AS {{ data_type }})
    {% endif %}
{% endmacro %}
