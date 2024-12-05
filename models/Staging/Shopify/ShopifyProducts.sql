{% if var('SHOPIFYV2') and var('ShopifyProducts',True) %}
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
table_pattern=var('shopify_products_tbl_ptrn','%shopify%products'),
exclude=var('shopify_products_exclude_tbl_ptrn',''),
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
    coalesce(a.id, 0) as product_id,
    {{timezone_conversion("a.created_at")}} as product_created_at,
    {{timezone_conversion("a.updated_at")}} as product_updated_at,
    a.* except(id,created_at,updated_at,variants,options,images,image),
    coalesce(variants.id, 0) as variants_id,
    variants.product_id as variants_product_id,
    variants.title as variants_title,
    variants.price as variants_price,
    coalesce(variants.sku, 'N/A') as variants_sku,
    variants.position as variants_position,
    variants.inventory_policy as variants_inventory_policy,
    variants.fulfillment_service as variants_fulfillment_service,
    variants.inventory_management as variants_inventory_management,
    -- variants.option1 as variants_option1,
    variants.created_at as variants_created_at,
    variants.updated_at as variants_updated_at,
    variants.taxable as variants_taxable,
    variants.barcode as variants_barcode,
    variants.grams as variants_grams,
    variants.image_id as variants_image_id,
    variants.weight as variants_weight,
    variants.weight_unit as variants_weight_unit,
    variants.inventory_item_id as variants_inventory_item_id,
    variants.inventory_quantity as variants_inventory_quantity,
    variants.old_inventory_quantity as variants_old_inventory_quantity,
    variants.requires_shipping as variants_requires_shipping,
    variants.admin_graphql_api_id as variants_admin_graphql_api_id,
    -- variants.tax_code as variants_tax_code,
    -- variants.compare_at_price as variants_compare_at_price,
    -- variants.option2 as variants_option2,
    price.amount as price_amount,
    price.currency_code as price_currency_code,
    dense_rank() over (partition by coalesce(a.id, 0) order by a.{{daton_batch_runtime()}} desc) as row_num
    from union_tables a
    {{unnesting("variants")}}
    {{multi_unnesting('variants','presentment_prices')}}
    {{multi_unnesting('presentment_prices','price')}}
) b
where row_num=1