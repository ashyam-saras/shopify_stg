{% macro drop_tables(schema_pattern, table_name_pattern_list=[], client_id_list=[], region='US', table_name_exclude_list=[], client_id_exclude_list=[]) %}

    {% set table_name_query = list_project_tables(
        schema_pattern=schema_pattern,
        table_name_pattern_list=table_name_pattern_list,
        client_id_list=client_id_list,
        region=region,
        table_name_exclude_list=table_name_exclude_list,
        client_id_exclude_list=client_id_exclude_list
    ) %}
    
    {% set results = run_query(table_name_query | safe).rows if execute else [] %}
    {% set table_name_query_clean = table_name_query | replace("\n", " ") | replace("\t", " ") %}
    {% if results | length == 0 %}
        SELECT "No tables matched" AS table_name, "Delete" AS event_name, "Pending: no tables matched" AS status, CURRENT_TIMESTAMP() AS event_timestamp, "{{ table_name_query_clean }}" AS tables_query
    {% else %}
        {% for row in results %}
            {% set table_complete_name = row['table_complete_name'] %}
            {% set table_type = row['table_type'] %}
            {% set table_schema = row['table_schema'] %}


            {% if var("user_confirmation", "no") == "yes" %}
                {% set sql_statements %}
                    {% if table_type == "BASE TABLE" %}
                        DROP TABLE IF EXISTS `{{ table_complete_name }}`;
                    {% elif table_type == "VIEW" %}
                        DROP VIEW IF EXISTS `{{ table_complete_name }}`;
                    {% elif table_type == "MATERIALIZED VIEW" %}
                        DROP MATERIALIZED VIEW `{{ table_complete_name }}`;
                    {% endif %}
                {% endset %}
                {% set run = run_query(sql_statements) %}
                SELECT "{{ table_complete_name }}" AS table_name,"{{ table_schema }}" table_schema, "Delete" AS event_name, "Done" AS status, CURRENT_TIMESTAMP() AS event_timestamp
            {% else %}
                SELECT "{{ table_complete_name }}" AS table_name,"{{ table_schema }}" table_schema, "Delete" AS event_name, "Pending: Please confirm the delete operation" AS status, CURRENT_TIMESTAMP() AS event_timestamp
            {% endif %}
            
            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

