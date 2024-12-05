{% macro list_project_tables(schema_pattern, table_name_pattern_list=[], client_id_list=[], region='US', table_name_exclude_list=[], client_id_exclude_list=[], raw_table_clustering=false) %}

    {% set raw_database = var('raw_database') %}
    
    {% if raw_database == 'insightsprod' %}
        {% set region = 'us-central1' %}
    {% endif %}

    {% set joined_table_name_pattern_list = table_name_pattern_list | join("', '") %}
    {% set joined_client_id_list = client_id_list | join("', '") %}
    {% set joined_table_name_exclude_list = table_name_exclude_list | join("', '") %}
    {% set joined_client_id_exclude_list = client_id_exclude_list | join("', '") %}

    {% set table_name_query %}
    SELECT DISTINCT 
        CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_complete_name,
        table_schema, 
        table_name,
        table_type
    FROM `{{ raw_database }}.region-{{ region }}.INFORMATION_SCHEMA.TABLES`

    {% if raw_table_clustering is true %}
    join 
    (
    SELECT distinct 
    table_name,table_schema
    FROM `{{ raw_database }}.region-{{ region }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE 
    column_name='_daton_batch_runtime' and clustering_ordinal_position IS  NULL
    )
    using(table_name,table_schema)    
    {% endif %}


    WHERE 
        LOWER(table_schema) LIKE '{{ schema_pattern }}'

        {% if raw_table_clustering is false %}
            AND LOWER(table_schema) NOT LIKE '%_raw%'
        {% endif %}

        {% if raw_table_clustering %}
            AND table_type= "BASE TABLE"
        {% endif %}

        {% if joined_table_name_pattern_list != '' %}
            AND COLLATE(table_name, 'und:ci') LIKE ANY ('{{ joined_table_name_pattern_list }}')
        {% endif %}

        {% if joined_table_name_exclude_list != '' %}
            AND COLLATE(table_name, 'und:ci') NOT LIKE ANY ('{{ joined_table_name_exclude_list }}')
        {% endif %}

        {% if joined_client_id_list != '' %}
            AND LOWER(SPLIT(table_schema, "_")[SAFE_OFFSET(1)]) IN ('{{ joined_client_id_list }}')
        {% endif %}

        {% if joined_client_id_exclude_list != '' %}
            AND LOWER(SPLIT(table_schema, "_")[SAFE_OFFSET(1)]) NOT IN ('{{ joined_client_id_exclude_list }}')
        {% endif %}
    {% endset %}

    {{ table_name_query }}

{% endmacro %}
