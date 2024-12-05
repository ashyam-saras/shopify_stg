{% if var('SHOPIFYV2') and var('ShopifyOrders',True) %}
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
table_pattern=var('shopify_orders_tbl_ptrn','%shopify%orders'),
exclude=var('shopify_orders_exclude_tbl_ptrn','%shopify%t_orders'),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    exclude = ["client_details", "current_subtotal_price_set",
"current_total_discounts_set", "current_total_price_set", "current_total_tax_set", "note_attributes", 
"subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
"total_shipping_price_set", "total_tax_set", "billing_address", "shipping_address", "customer", "discount_codes",
"discount_applications", "line_items", "payment_details", "refunds", "shipping_lines", "payment_terms"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
coalesce(a.id, 0) as order_id,
{{timezone_conversion("a.created_at")}} as order_created_at,
{{timezone_conversion("a.updated_at")}} as order_updated_at,
coalesce({{extract_nested_value("fulfillments","id","string")}}, 'N/A') as fulfillments_id,
{{extract_nested_value("fulfillments","order_id","string")}} as fulfillments_order_id,
{{extract_nested_value("fulfillments","name","string")}} as fulfillments_name,
fulfillments.* {{exclude()}}(id, receipt, order_id, admin_graphql_api_id, name, line_items),
coalesce({{extract_nested_value("line_items","id","string")}}, 'N/A') as line_items_id,
{{extract_nested_value("line_items","name","string")}} as line_items_name,
line_items.* {{exclude()}}(id, name, price_set, properties, total_discount_set, tax_lines, discount_allocations),
{{ currency_conversion('b.value', 'b.from_currency_code', 'a.currency') }},
_dbt_source_relation,
a.{{daton_user_id()}} as _daton_user_id,
a.{{daton_batch_runtime()}} as _daton_batch_runtime,
a.{{daton_batch_id()}} as _daton_batch_id,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from union_tables a
{{ unnesting("fulfillments") }}
{{ multi_unnesting("fulfillments", "line_items") }}
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} b on date(a.created_at) = b.date and currency = b.to_currency_code
{% endif %}
qualify dense_rank() over (partition by coalesce(a.id, 0) order by a.{{daton_batch_runtime()}} desc) = 1