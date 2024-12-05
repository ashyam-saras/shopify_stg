{% if var('SHOPIFYV2') and var('ShopifyInventoryLevels',True) %}
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
table_pattern=var('shopify_inventory_levels_tbl_ptrn','%shopify%inventory_levels'),
exclude=var('shopify_inventory_levels_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime) }}
)

select
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
b.* {{exclude()}} (_dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}, row_num),
b.{{daton_user_id()}} as _daton_user_id,
b.{{daton_batch_runtime()}} as _daton_batch_runtime,
b.{{daton_batch_id()}} as _daton_batch_id,
_dbt_source_relation,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from (
    select 
    coalesce(a.inventory_item_id, 0) as inventory_item_id,
    coalesce(a.location_id, 0) as location_id,
    {{timezone_conversion("updated_at")}} as updated_at,   
    a.* except(inventory_item_id,location_id,updated_at),
    dense_rank() over (partition by coalesce(a.inventory_item_id, 0),coalesce(a.location_id, 0) order by a.updated_at desc, a.{{daton_batch_runtime()}} desc, a.{{daton_batch_id()}} desc) as row_num
    from union_tables a
    ) b
where row_num =1