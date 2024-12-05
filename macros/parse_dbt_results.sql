{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}
        {% set bytes_processed = run_result_dict.get('adapter_response', {}).get('bytes_processed', 0) %}
        {%- if not bytes_processed -%}
            {% set bytes_processed = 0 %}
        {%- endif -%}
        {% set bytes_billed = run_result_dict.get('adapter_response', {}).get('bytes_billed', 0) %}
        {%- if not bytes_billed -%}
            {% set bytes_billed = 0 %}
        {%- endif -%}
        {% set job_id = run_result_dict.get('adapter_response', {}).get('job_id', 0) %}
        {%- if not job_id -%}
            {% set job_id = 0 %}
        {%- endif -%}
        {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'generated_at': generated_at,
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected,
                'bytes_processed': bytes_processed,
                'bytes_billed': bytes_billed,
                'job_id': job_id
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}
