with source_orders as (
    select
        unified_order_id,
        item_no_ as product_id,
        item_name as product_name,
        item_division as division,        -- Updated: was 'division'
        item_category,
        item_subcategory,
        item_brand
    from {{ ref('fact_commercial') }}
    where transaction_type = 'Sale'
        and unified_order_id is not null
        and item_no_ is not null
),

deduplicated_orders as (
    select
        *,
        row_number() over (
            partition by unified_order_id, product_id
            order by unified_order_id
        ) as product_rank
    from source_orders
)

select
    unified_order_id,
    product_id,
    product_name,
    division,
    item_category,
    item_subcategory,
    item_brand
from deduplicated_orders
where product_rank = 1

