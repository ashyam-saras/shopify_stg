{% if var('SHOPIFYV2') and var('ShopifyRefundsTransactions',True) %}
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
    exclude = ["refund_line_items","total_duties_set","order_adjustments",
    "transactions.payment_details", "transactions.receipt", "transactions.payments_refund_attributes", 
    "transactions.total_unsettled_set"],
    where = max_loaded_batchruntime) }}
)


select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
coalesce(b.order_id, 0) as order_id,
b.* {{exclude()}} (_dbt_source_relation, order_id, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}),
{{ currency_conversion('c.value', 'c.from_currency_code', 'currency') }},
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
    {{timezone_conversion("a.created_at")}} as refund_created_at,
    {{timezone_conversion("a.processed_at")}} as refund_processed_at,
    coalesce(a.id, 0) as refund_id,
    coalesce(transactions.id, 0) as transactions_id,
    transactions.* except(id),
    _dbt_source_relation,
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    dense_rank() over (partition by a.id order by _daton_batch_runtime desc) as row_num
    from union_tables a
    {{unnesting("transactions")}} 
) b
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.currency = c.to_currency_code
{% endif %}
where row_num=1