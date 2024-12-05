{% if var('SHOPIFYV2') and var('ShopifyRefundsLineItemsTax',True) %}
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
    exclude = ["transactions","total_duties_set","order_adjustments"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
b.* {{exclude()}} (_dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}),
{{ currency_conversion('c.value', 'c.from_currency_code', 'presentment_money_currency_code') }},
b.{{daton_user_id()}} as _daton_user_id,
b.{{daton_batch_runtime()}} as _daton_batch_runtime,
b.{{daton_batch_id()}} as _daton_batch_id,
_dbt_source_relation,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
row_number() over (partition by refund_id order by b.{{daton_batch_runtime()}} desc) _seq_id
from (
    select 
    {{timezone_conversion("a.created_at")}} as created_at,
    {{timezone_conversion("a.processed_at")}} as processed_at,
    coalesce(a.id, 0) as refund_id,
    a.order_id,
    coalesce(refund_line_items.id, 0) as refund_line_items_id,
    refund_line_items.quantity as refund_line_items_quantity,
    line_item.id as refund_line_items_line_item_id,
    line_item.id,
    line_item.variant_id,
    line_item.product_id,
    line_item.sku,
    coalesce(tax_lines.title, 'N/A') as title,
    tax_lines.* {{exclude()}}(price_set, title),
    shop_money.amount as shop_money_amount,
    shop_money.currency_code as shop_money_currency_code,
    presentment_money.amount as presentment_money_amount,
    presentment_money.currency_code as presentment_money_currency_code,
    _dbt_source_relation,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    dense_rank() over (partition by coalesce(a.id, 0) order by _daton_batch_runtime desc) as row_num
    from union_tables a
    {{unnesting("refund_line_items")}}
    {{multi_unnesting("refund_line_items","line_item")}}
    {{multi_unnesting("line_item","tax_lines")}} 
    {{multi_unnesting("tax_lines","price_set")}} 
    {{multi_unnesting("price_set","shop_money")}} 
    {{multi_unnesting("price_set","presentment_money")}}  
) b
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.presentment_money_currency_code = c.to_currency_code
{% endif %}
where row_num=1