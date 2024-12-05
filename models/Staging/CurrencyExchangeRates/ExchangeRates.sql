{% if var('currency_conversion_flag') and var('ExchangeRates',True) %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('shared_schema'),
table_pattern=var('currency_exchnage_rates_tbl_ptrn','%exchange_rates'),
exclude=var('currency_exchnage_rates_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

{% for i in relations %}
    
    SELECT * {{exclude()}} (row_num)
    From (
        select 
        date, 
        from_currency_code, 
        to_currency_code, 
        coalesce(value,cast(value_bn as numeric))value,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY date, from_currency_code, to_currency_code order by _daton_batch_runtime desc) row_num
        from {{i}}    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('currency_exchange_rates_lookback',2592000000) }},0) from {{ this }})
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
