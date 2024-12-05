{% if var('SHOPIFYV2') and var('ShopifyFulfillmentOrders',False) %}
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
table_pattern=var('shopify_fulfillment_orders_tbl_ptrn','%shopify%fulfillment_orders'),
exclude=var('shopify_fulfillment_orders_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["assigned_location","delivery_method"],
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
    coalesce(a.id, 'N/A') as id,
    {{timezone_conversion("fulfill_at")}} as fulfill_at,
    {{timezone_conversion("created_at")}} as created_at,
    {{timezone_conversion("updated_at")}} as updated_at,
    a.* except(id, fulfill_at, created_at, updated_at, destination, line_items),
    destination.id as destination_id,
    destination.address1 as destination_address1,
    destination.address2 as destination_address2,
    destination.city as destination_city,
    destination.country as destination_country,
    destination.email as destination_email,
    destination.first_name as destination_first_name,
    destination.last_name as destination_last_name,
    destination.phone as destination_phone,
    destination.province as destination_province,
    destination.zip as destination_zip,
    -- destination.company as destination_company,
    coalesce(line_items.id, 'N/A') as line_items_id,
    line_items.shop_id as line_items_shop_id,
    line_items.fulfillment_order_id as line_items_fulfillment_order_id,
    line_items.quantity as line_items_quantity,
    line_items.line_item_id as line_items_line_item_id,
    line_items.inventory_item_id as line_items_inventory_item_id,
    line_items.fulfillable_quantity as line_items_fulfillable_quantity,
    line_items.variant_id as line_items_variant_id,
    row_number() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) as row_num
    from union_tables a
    {{unnesting("destination")}}
    {{unnesting("line_items")}}
    ) b
where row_num=1