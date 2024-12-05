{% macro max_loaded_date(updated_date="_last_updated_staging", days_to_look_back=1) %}

    {% set max_date_query %}
    select coalesce(max(coalesce(date({{ updated_date }}), '1970-01-01')), '1970-01-01') - {{ days_to_look_back | int }} from {{ this }}
    {% endset %}

    {% set max_loaded = dbt_utils.get_single_value(max_date_query) %}

    {{return(max_loaded)}}
{% endmacro %}