{% if var('SHOPIFYV2') and var('ShopifyShop',True) %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_shop_tbl_ptrn','%shopify%shop'),
exclude=var('shopify_shop_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

with union_tables as (
    {{ dbt_utils.union_relations(
        relations = table_relations,
        where = max_loaded_batchruntime) }}
    )

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
{{timezone_conversion("created_at")}} as created_at,
{{timezone_conversion("updated_at")}} as updated_at,
a.* except(created_at, updated_at, _dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}),
{{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
a.{{daton_user_id()}} as _daton_user_id,
a.{{daton_batch_runtime()}} as _daton_batch_runtime,
a.{{daton_batch_id()}} as _daton_batch_id,
_dbt_source_relation,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from union_tables a
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(a.updated_at) = c.date and a.currency = c.to_currency_code
{% endif %}
qualify dense_rank() over (partition by a.id order by _daton_batch_runtime desc)=1

