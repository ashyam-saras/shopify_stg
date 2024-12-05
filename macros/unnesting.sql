{%- macro unnesting(variable) -%}
    {%- if target.type == "snowflake" -%}, lateral flatten(input => parse_json({{ variable }})) {{ variable }}
    {%- else -%} left join unnest({{ variable }}) {{ variable }}
    {%- endif -%}
{%- endmacro -%}
