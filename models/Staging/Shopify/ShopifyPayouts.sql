{% if var('SHOPIFYV2') and var('ShopifyPayouts',False) %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if is_incremental() %}
    {% set max_loaded_batchruntime = '_daton_batch_runtime >= (select coalesce(max(_daton_batch_runtime),0) from ' ~ this ~ ')' %}
{% else %}
    {% set max_loaded_batchruntime = '1=1' %}
{% endif %}

{% set table_relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_payouts_tbl_ptrn','%shopify%payouts'),
exclude=var('shopify_payouts_exclude_tbl_ptrn',''),
database=var('raw_database')) %}

with union_tables as (
{{ dbt_utils.union_relations(
    relations = table_relations,
    where = max_loaded_batchruntime) }}
)

select
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
b.* {{exclude()}} (_dbt_source_relation, {{daton_user_id()}}, {{daton_batch_runtime()}}, {{daton_batch_id()}}, row_num),
{{ currency_conversion('c.value', 'c.from_currency_code', 'b.currency') }},
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
    a.* except(id, summary),
    summary.adjustments_fee_amount as summary_adjustments_fee_amount,
    summary.adjustments_gross_amount as summary_adjustments_gross_amount,
    summary.charges_fee_amount as summary_charges_fee_amount,
    summary.charges_gross_amount as summary_charges_gross_amount,
    summary.refunds_fee_amount as summary_refunds_fee_amount,
    summary.refunds_gross_amount as summary_refunds_gross_amount,
    summary.reserved_funds_fee_amount as summary_reserved_funds_fee_amount,
    summary.reserved_funds_gross_amount as summary_reserved_funds_gross_amount,
    summary.retried_payouts_fee_amount as summary_retried_payouts_fee_amount,
    summary.retried_payouts_gross_amount as summary_retried_payouts_gross_amount,
    dense_rank() over (partition by a.date order by a.{{daton_batch_runtime()}} desc) as row_num
    from union_tables a
    {{ unnesting("summary") }} 
    ) b
{% if var('currency_conversion_flag') %}
    left join {{ref('ExchangeRates')}} c on date(b.date) = c.date and b.currency = c.to_currency_code
{% endif %}
where row_num=1