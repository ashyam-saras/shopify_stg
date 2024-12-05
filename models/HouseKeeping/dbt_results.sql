-- Save model as 'dbt_results.sql'

-- invocation_args_dict:
-- {{ invocation_args_dict }}

-- dbt_metadata_envs:
-- {{ dbt_metadata_envs }}



{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'result_id'
  )
}}

with empty_table as (
    select
        cast(null as string) as result_id,
        cast(null as string) as invocation_id,
        cast(null as string) as unique_id,
        cast(null as string) as generated_at,
        cast(null as string) as database_name,
        cast(null as string) as schema_name,
        cast(null as string) as name,
        cast(null as string) as resource_type,
        cast(null as string) as status,
        cast(null as numeric) as execution_time,
        cast(null as int) as rows_affected,
        cast(null as numeric) as bytes_processed,
        cast(null as numeric) as bytes_billed,
        cast(null as string) as job_id,
        cast(null as string) as airflow_run_id,
        current_timestamp() as record_created_at
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0