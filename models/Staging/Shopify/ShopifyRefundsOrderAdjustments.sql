{% if var('SHOPIFYV2') and var('ShopifyRefundsOrderAdjustments',True) %}
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
    exclude = ["transactions","total_duties_set","refund_line_items"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
coalesce(b.order_id, 0) as order_id,
b.* {{exclude()}} (tax_amount_set, _dbt_source_relation, order_id, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}),
shop_money.amount as tax_amount_set_shop_money_amount,
shop_money.currency_code as tax_amount_set_shop_money_currency_code,
presentment_money.amount as tax_amount_set_presentment_money_amount,
presentment_money.currency_code as tax_amount_set_presentment_money_currency_code,
{{ currency_conversion('c.value', 'c.from_currency_code', 'amount_set_presentment_money_currency_code') }},
b.{{daton_user_id()}} as _daton_user_id,
b.{{daton_batch_runtime()}} as _daton_batch_runtime,
b.{{daton_batch_id()}} as _daton_batch_id,
_dbt_source_relation,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
row_number() over (partition by refund_id order by b._daton_batch_runtime desc) _seq_id
from (
    select 
    {{timezone_conversion("a.created_at")}} as created_at,
    {{timezone_conversion("a.processed_at")}} as processed_at,
    coalesce(a.id, 0) as refund_id,
    a.note,
    coalesce(order_adjustments.id, 0) as order_adjustments_id,
    order_adjustments.refund_id as order_adjustments_refund_id,
    order_adjustments.* except(id, refund_id, amount_set),
    shop_money.amount as amount_set_shop_money_amount,
    shop_money.currency_code as amount_set_shop_money_currency_code,
    presentment_money.amount as amount_set_presentment_money_amount,
    presentment_money.currency_code as amount_set_presentment_money_currency_code,
    _dbt_source_relation,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    row_number() over (partition by coalesce(a.id, 0), a.order_id, {{extract_nested_value('order_adjustments','id','string')}} order by _daton_batch_runtime desc) as row_num
    from union_tables a
    {{unnesting("order_adjustments")}}
    {{multi_unnesting("order_adjustments","amount_set")}}
    {{multi_unnesting("amount_set","shop_money")}}
    {{multi_unnesting("amount_set","presentment_money")}}
) b
{{unnesting("tax_amount_set")}}
{{multi_unnesting("tax_amount_set","shop_money")}}
{{multi_unnesting("tax_amount_set","presentment_money")}}
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.amount_set_presentment_money_currency_code = c.to_currency_code
{% endif %}
where row_num=1