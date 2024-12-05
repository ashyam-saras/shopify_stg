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
"total_shipping_price_set", "total_tax_set", "customer", "discount_applications", "discount_codes",
"fulfillments", "line_items", "payment_details", "refunds", "shipping_lines", "payment_terms"],
    where = max_loaded_batchruntime) }}
)

select 
replace(split(split(_dbt_source_relation,'.')[2],'_')[0],'`','') as brand,
coalesce(a.id, 0) as order_id,
{{timezone_conversion("created_at")}} as created_at,
{{timezone_conversion("updated_at")}} as updated_at,
{{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,  
{{extract_nested_value("billing_address","city","string")}} as billing_address_city,    
{{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,   
{{extract_nested_value("billing_address","province","string")}} as billing_address_province,  
{{extract_nested_value("billing_address","country","string")}} as billing_address_country,   
{{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,
{{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,   
{{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,  
{{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,  
{{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,    
{{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,   
{{extract_nested_value("shipping_address","country","string")}} as shipping_address_country,   
{{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,
{{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,  
coalesce(email, 'N/A') as email,
_dbt_source_relation,
a.{{daton_user_id()}} as _daton_user_id,
a.{{daton_batch_runtime()}} as _daton_batch_runtime,
a.{{daton_batch_id()}} as _daton_batch_id,
concat(split(lower(replace(split(_dbt_source_relation,'.')[2],'`','')),'_shopify')[0], '_', REGEXP_REPLACE(replace(split(_dbt_source_relation,'.')[2],'`',''),'[^0-9]','')) as _daton_sourceversion_integration_id,
current_timestamp() as _last_updated,
'{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
from union_tables a
{{unnesting("BILLING_ADDRESS")}} 
{{unnesting("SHIPPING_ADDRESS")}} 
qualify dense_rank() over (partition by coalesce(a.id, 0) order by a.{{daton_batch_runtime()}} desc) = 1