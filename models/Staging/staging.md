# Staging Model

## Staging Models Starter Template

Use this template to create a new staging model for a platform.

### Variables

- `PLATFORM_NAME`: The name of the platform. Create a variable for each platform or use existing variables if creating a new model for an existing platform.
- `raw_schema`: The schema of the raw data. Schema where Daton is configured to load data.
- `raw_database`: The database of the raw data. Database where Daton is configured to load data.
- `platform_name_tbl_ptrn`: The pattern of the raw table. Pattern to match the raw table for multiple integrations from the same platform.
- `platform_name_tbl_exclude_ptrn`: The pattern of the raw table to exclude. Pattern to exclude tables from the raw schema.
- `currency_conversion_flag`: Flag to indicate if the raw data contains currency information and to apply currency conversion.

### Template

```sql
{%- if var("PLATFORM_NAME") -%} {{ config(enabled=True) }}
{%- else -%} {{ config(enabled=False) }}
{%- endif -%}

{%- if is_incremental() -%}
    {%- set max_loaded_batchruntime = (
        "timestamp_millis(cast(_daton_batch_runtime as int64)) >= (select coalesce(max(_daton_batch_runtime), TIMESTAMP('1970-01-01 00:00:00')) from "
        ~ this
        ~ ")"
    ) -%}
{%- else -%} {% set max_loaded_batchruntime = "1=1" %}
{%- endif -%}

{%- set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern=var("raw_schema"),
    table_pattern=var("platform_name_tbl_ptrn", "%Raw_Table_Pattern%"),
    exclude=var("platform_name_tbl_exclude_ptrn", ""),
    database=var("raw_database"),
) %}

with union_tables as ({{ dbt_utils.union_relations(relations=relations, where=max_loaded_batchruntime) }})

select
    array_reverse(split(_dbt_source_relation, '_'))[safe_offset(3)] as store,
    replace(split(split(_dbt_source_relation, '.')[2], '_')[0], '`', '') as brand,
    -- Add columns from raw table below
    *,
    {%- if var("currency_conversion_flag") %}
        {{ currency_conversion("er.value", "er.from_currency_code", "ut.currency") }},
    {%- endif %}
    ut.{{ daton_user_id() }} as _daton_user_id,
    ut.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    ut.{{ daton_batch_id() }} as _daton_batch_id,
    ut._dbt_source_relation,
    current_timestamp() as _last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
from union_tables as ut
{% if var("currency_conversion_flag") -%}
    left join {{ ref("ExchangeRates") }} as er on date(ut.updated_at) = er.date and ut.currency = er.to_currency_code
{%- endif %}
{% if is_incremental() -%}
    where
        {{ daton_batch_runtime() }}
        >= (select coalesce(max(_daton_batch_runtime) - {{ var("_lookback", 2592000000) }}, 0) from {{ this }})
{%- endif %}
```

## Model Design Guidelines

### Partitioning and Clustering Best Practices

#### When to Use Partitioning
| Criteria | Recommendation | Source |
|----------|---------------|---------|
| Query Pattern | Queries filter on the partitioning column | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioned_tables) |
| Cost Control | Need to determine query costs before running | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioned_tables) |
| Data Management | Need partition-level operations (e.g., partition expiration) | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioned_tables) |
| Data Distribution | Even distribution (non-skewed) | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#partition_pruning) |
| Common Columns | DATE, TIMESTAMP, DATETIME, or INTEGER | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#types_of_partitioned_tables) |
| Limitations | Max 4000 partitions per table | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#limitations) |

#### When to Use Clustering Instead
| Scenario | Recommendation | Source |
|----------|---------------|---------|
| High Cardinality | Many unique values in column | [BigQuery Docs](https://cloud.google.com/bigquery/docs/clustered-tables#when_to_use_clustering) |
| Multiple Filters | Queries filter on multiple columns | [BigQuery Docs](https://cloud.google.com/bigquery/docs/clustered-tables#when_to_use_clustering) |
| Small Partitions | Partitioning would result in small partitions | [BigQuery Docs](https://cloud.google.com/bigquery/docs/partitioned-tables#when_to_use_partitioned_tables) |
| Frequent Updates | Data modified frequently (every few minutes) | [BigQuery Docs](https://cloud.google.com/bigquery/docs/clustered-tables#when_to_use_clustering) |

#### Column Selection Matrix
| Column Type | Partition | Cluster | Example |
|------------|-----------|---------|---------|
| Date/Timestamp | ✅ | ❌ | created_at, updated_at |
| High-cardinality IDs | ❌ | ✅ | customer_id, order_id |
| Status/Type fields | ❌ | ✅ | order_status, payment_status |
| Foreign keys | ❌ | ✅ | product_id, subscription_id |
| Boolean flags | ❌ | ❌ | is_active, is_deleted |

#### Example Configuration
```yaml
config:
  materialized: incremental
  incremental_strategy: merge
  partition_by:
    field: date(created_at)
    data_type: date
  cluster_by:
    - customer_id        # Primary identifier
    - subscription_id    # Secondary identifier
    - status             # Commonly filtered field
```

#### Cost-Performance Considerations
- Partitioning:
  - Reduces query costs by scanning only relevant partitions
  - Improves query performance for time-based filters
  - Helps with data lifecycle management

- Clustering:
  - Improves query performance by co-locating similar data
  - Most effective when querying specific ranges or values
  - Automatically maintained by BigQuery

Sources:
- [BigQuery Partitioning Documentation](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [BigQuery Clustering Documentation](https://cloud.google.com/bigquery/docs/clustered-tables)
- [dbt Partitioning & Clustering](https://docs.getdbt.com/reference/resource-configs/bigquery-configs#clustering)
