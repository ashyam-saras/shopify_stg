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
"discount_applications", "fulfillments", "payment_details", "refunds", "shipping_lines", "payment_terms"],
    where = max_loaded_batchruntime) }}
)

select *, row_number() over (partition by order_id order by _daton_batch_runtime desc) _seq_id
from (
    select 
    replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
    coalesce(a.id, 0) as order_id,
    {{timezone_conversion("a.created_at")}} as created_at,
    {{timezone_conversion("a.updated_at")}} as updated_at,
    {{extract_nested_value("line_items","id","string")}} as line_items_id,
    {{extract_nested_value("line_items","product_id","string")}} as product_id,
    {{extract_nested_value("line_items","sku","string")}} as sku,
    {{extract_nested_value("line_items","variant_id","string")}} as variant_id,
    coalesce(tax_lines.title,'N/A') as title,
    tax_lines.* {{exclude()}}(price_set,title),
    shop_money.amount as shop_money_amount,
    shop_money.currency_code as shop_money_currency_code,
    presentment_money.amount as presentment_money_amount,
    presentment_money.currency_code as presentment_money_currency_code,
    {{ currency_conversion('b.value', 'b.from_currency_code', 'a.currency') }},
    _dbt_source_relation,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from union_tables a
    {{ unnesting("line_items") }}
    {{ multi_unnesting("line_items", "tax_lines") }}
    {{ multi_unnesting("tax_lines","price_set") }}
    {{ multi_unnesting("price_set","shop_money") }}
    {{ multi_unnesting("price_set","presentment_money") }}
    {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
    {% endif %}
    qualify dense_rank() over (partition by coalesce(a.id, 0) order by a.{{daton_batch_runtime()}} desc) = 1
)