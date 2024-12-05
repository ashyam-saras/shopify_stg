{% if var('drop_table_script') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{{ 
drop_tables(
                schema_pattern="%saad_test_pre%",
                table_name_pattern_list=["%fact_advertising_%",'dim_'],
                client_id_list=[''],
                table_name_exclude_list=['fact_targeting']
            )
}}


/*
This model deletes/drops tables from the warehouse based on specified patterns in bulk. 
It is required for incremental models where adding new columns or resolving historical bugs necessitates a full refresh. 
Execute this script to delete models you wish to refresh before DAG runs, ensuring they are rebuilt during the next run.

Accepted Variables:
- schema_pattern (string): Provide the pattern to match schema names. Example: "%saad_test_pre%" matches any schema containing 'saad_test_pre'.
- table_name_pattern_list (list of strings): Contains patterns to match table names. Example: ["%dim_address_%"] matches any table name that includes 'dim_address_'.
- client_id_list (optional, list of strings): Filters schema names by specified client IDs. If omitted, all schemas are considered. Example: ["1878", "3999"] matches schemas containing these IDs.
- table_name_exclude_list (optional, list of strings): Exclude tables based on table names pattern

Method of Invocation:
1. To preview the tables affected by this script, run the following command and ensure that the list accurately reflects the tables intended for deletion:
   dbt run --select models/HouseKeeping/drop_table_script.sql 

2. Once you are sure of the list of tables, execute the following command to perform the drop operation:
   dbt run --select models\HouseKeeping\drop_table_script.sql --vars "'user_confirmation': 'yes'"
*/