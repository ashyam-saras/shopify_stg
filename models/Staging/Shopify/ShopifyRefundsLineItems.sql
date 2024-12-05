{% if var('SHOPIFYV2') and var('ShopifyRefundsLineItems',True) %}
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
table_pattern=var('shopify_refunds_tbl_ptrn','%shopify%refunds'),
exclude=var('shopify_refunds_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["transactions","total_duties_set","order_adjustments","admin_graphql_api_id",
    "line_item.admin_graphql_api_id","line_item.price_set","line_item.total_discount_set",
    "line_item.discount_allocations","line_item.tax_lines","line_item.properties",
    "line_item.origin_location","line_item.destination_location"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
b.* {{exclude()}} (row_num, _dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}),
{{ currency_conversion('c.value', 'c.from_currency_code', 'b.presentment_money_currency_code') }},
b.{{daton_user_id()}} as _daton_user_id,
b.{{daton_batch_runtime()}} as _daton_batch_runtime,
b.{{daton_batch_id()}} as _daton_batch_id,
_dbt_source_relation,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from (
    select 
    {{timezone_conversion("a.created_at")}} as created_at,
    {{timezone_conversion("a.processed_at")}} as processed_at,
    coalesce(a.id, 0) as refund_id,
    a.* {{exclude()}}(id,created_at,processed_at,refund_line_items),
    coalesce(refund_line_items.id, 0) as refund_line_items_id,
    refund_line_items.quantity as refund_line_items_quantity,
    refund_line_items.* {{exclude()}}(id,quantity,line_item,subtotal_set,total_tax_set),
    line_item.id as refund_line_items_line_item_id,
    coalesce(line_item.variant_id, 0) as variant_id,
    line_item.product_id as product_id, 
    line_item.sku as sku, 
    line_item.total_discount as total_discount,
    line_item.gift_card as gift_card,
    shop_money.amount as shop_money_amount,
    shop_money.currency_code as shop_money_currency_code,
    presentment_money.amount as presentment_money_amount,
    presentment_money.currency_code as presentment_money_currency_code,
    row_number() over (partition by coalesce(a.id, 0), {{extract_nested_value('refund_line_items','id','string')}} order by a.{{daton_batch_runtime()}} desc) as row_num
    from union_tables a
    {{unnesting("refund_line_items")}}
    {{multi_unnesting("refund_line_items","line_item")}}
    {{multi_unnesting("refund_line_items","subtotal_set")}}
    {{multi_unnesting("subtotal_set","shop_money")}}
    {{multi_unnesting("subtotal_set","presentment_money")}}
    
) b
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.presentment_money_currency_code = c.to_currency_code
{% endif %}
where row_num=1