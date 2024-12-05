{% macro tmp_suffix() %}
  {{ return("__dbt_tmp") }}
{% endmacro %}

{% macro make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {{ return(adapter.dispatch('make_temp_relation', 'dbt')(base_relation, tmp_suffix() )) }}
{% endmacro %}

{% macro incremental_tmp_table_dropper(bigQueryRelationObject) %}
    {% if bigQueryRelationObject is not none %}
        {%- set tmpTableName -%}
            {{ bigQueryRelationObject.database + '.' + bigQueryRelationObject.schema + '.' + bigQueryRelationObject.identifier + tmp_suffix()}}
        {%- endset -%}
        {% set query %}
           drop table if exists `{{tmpTableName}}`;
        {% endset %}
        {{ return(query) }}
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}
