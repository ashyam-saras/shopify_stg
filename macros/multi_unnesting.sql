{%- macro multi_unnesting(variable, variable1) -%}
    {%- if target.type == "snowflake" -%}
        , lateral flatten(input => parse_json({{ variable }}.value:{{ variable1 }})) as {{ variable1 }}
    {%- else -%} left join unnest({{ variable }}.{{ variable1 }}) as {{ variable1 }}
    {%- endif -%}
{%- endmacro -%}
