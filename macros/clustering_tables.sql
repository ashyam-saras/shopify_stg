{% macro clustering_tables(schema_pattern, table_name_pattern_list=[], client_id_list=[], region='US', table_name_exclude_list=[], client_id_exclude_list=[], raw_table_clustering=true) %}

    {% set table_name_query = list_project_tables(
        schema_pattern=schema_pattern,
        table_name_pattern_list=table_name_pattern_list,
        client_id_list=client_id_list,
        region=region,
        table_name_exclude_list=table_name_exclude_list,
        client_id_exclude_list=client_id_exclude_list,
        raw_table_clustering=raw_table_clustering
    ) %}

    {% set results = run_query(table_name_query | safe).rows if execute else [] %}

    {% set table_name_query_clean = table_name_query | replace("\n", " ") | replace("\t", " ") %}

    {% if results | length == 0 %}
        SELECT "No tables matched" AS table_name, "Clustering" AS event_name, "Pending: no tables matched" AS status, CURRENT_TIMESTAMP() AS event_timestamp, "{{ table_name_query_clean }}" AS tables_query
    {% else %}
        {% for row in results %}
            {% set table_complete_name = row['table_complete_name'] %}
            {% set table_name = row['table_name'] %}
            {% set table_schema = row['table_schema'] %}

            {% if var("user_confirmation", "no") == "yes" %}
                {% set sql_statements %}
                    CREATE OR REPLACE TABLE `{{ table_complete_name }}_temp`
                    CLUSTER BY _daton_batch_runtime
                    AS SELECT * FROM `{{ table_complete_name }}`;
                    
                    DROP TABLE IF EXISTS `{{ table_complete_name }}`;
                    
                    ALTER TABLE `{{ table_complete_name }}_temp`
                    RENAME TO `{{ table_name }}`;
                {% endset %}
                {% set run = run_query(sql_statements) %}
                SELECT "{{ table_name }}" AS table_name,"{{ table_schema }}" table_schema, "Clustering" AS event_name, "Done" AS status, CURRENT_TIMESTAMP() AS event_timestamp
            {% else %}
                SELECT "{{ table_name }}" AS table_name,"{{ table_schema }}" table_schema, "Clustering" AS event_name, "Pending: Please confirm the clustering operation" AS status, CURRENT_TIMESTAMP() AS event_timestamp
            {% endif %}

            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}
