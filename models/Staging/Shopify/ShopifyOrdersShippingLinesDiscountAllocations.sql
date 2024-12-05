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
    exclude = ["client_details", "current_subtotal_price_set", "note_attributes", 
"current_total_discounts_set", "current_total_price_set", "current_total_tax_set", 
"subtotal_price_set", "tax_lines", "total_discounts_set", "total_line_items_price_set", "total_price_set",
"total_shipping_price_set", "total_tax_set", "customer", "discount_applications", "billing_address",
"shipping_address", "fulfillments", "line_items", "payment_details", "refunds", "discount_codes", "payment_terms",
"shipping_lines.tax_lines","shipping_lines.discount_allocations","shipping_lines.discounted_price_set","shipping_lines.price_set"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
cast(a.id as numeric) as order_id,
{{timezone_conversion("created_at")}} as created_at,
{{timezone_conversion("updated_at")}} as updated_at,
ifnull({{extract_nested_value("shipping_lines","id","numeric")}},0) as shipping_lines_id,
{{extract_nested_value("discount_allocations","amount","numeric")}} as shipping_lines_discount_amount,
ifnull(discount_application_index,0)discount_application_index,
{{ currency_conversion('b.value', 'b.from_currency_code', 'currency') }},
shipping_lines.* {{exclude()}}(id),
_dbt_source_relation,
a._daton_user_id as _daton_user_id,
a._daton_batch_runtime as _daton_batch_runtime,
a._daton_batch_id as _daton_batch_id,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_')[0], '_', split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_')[1]) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from union_tables a
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
{% endif %}
{{unnesting("shipping_lines")}} 
{{ multi_unnesting("shipping_lines", "discount_allocations") }}
where {{extract_nested_value("discount_allocations","amount","numeric")}} is not null
qualify dense_rank() over (partition by a.id order by a._daton_batch_runtime desc) = 1