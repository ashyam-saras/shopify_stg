{% if var('clustering_table_script') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{{ 
clustering_tables(
                schema_pattern="%saad_test_staging%",
                table_name_pattern_list=['%AddisonBay_GOOGLEADS_969_campaign_view%'],
                client_id_list=[''],
                table_name_exclude_list=[''],
                client_id_exclude_list=['']
            )
}}

/*
This model performs clustering on tables based on specified patterns in bulk. 
It is used to cluster table data for better performance by clustering raw tables on _daton_batch_runtime 
Execute this script after the daton integrations are paused to avoid missing any data


Accepted Variables:
- schema_pattern (string): Provide the pattern to match schema names. Example: "%saad_test_pre%" matches any schema containing 'saad_test_pre'.
- table_name_pattern_list (list of strings): Contains patterns to match table names. Example: ["%dim_address_%"] matches any table name that includes 'dim_address_'.
- client_id_list (optional, list of strings): Filters schema names by specified client IDs. If omitted, all schemas are considered. Example: ["1878", "3999"] matches schemas containing these IDs.
- table_name_exclude_list (optional, list of strings): Exclude tables based on table names pattern.
- client_id_exclude_list (optional, list of strings): Exclude schemas based on client IDs.

Method of Invocation:
1. To preview the tables affected by this script, run the following command and ensure that the list accurately reflects the tables intended for clustering:
   dbt run --select models/HouseKeeping/clustering_table_script.sql 

2. Once you are sure of the list of tables, execute the following command to perform the clustering operation:
   dbt run --select models/HouseKeeping/clustering_table_script.sql --vars "'user_confirmation': 'yes'"
*/
