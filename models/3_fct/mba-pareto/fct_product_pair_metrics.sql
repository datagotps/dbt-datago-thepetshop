with order_items as (
    select *
    from {{ ref('stg_mba_order_items') }}
),

pair_counts as (
    select *
    from {{ ref('fct_product_pairs') }}
),

total_orders as (
    select count(distinct unified_order_id) as total_orders
    from order_items
),

product_order_counts as (
    select
        product_id,
        count(distinct unified_order_id) as product_order_count
    from order_items
    group by product_id
)

select
    pair_counts.*,
    product1_counts.product_order_count as product1_order_count,
    product2_counts.product_order_count as product2_order_count,
    total_orders.total_orders,
    pair_counts.pair_order_count * 1.0 / nullif(total_orders.total_orders, 0) as support,
    pair_counts.pair_order_count * 1.0 / nullif(product1_counts.product_order_count, 0) as confidence_p1_to_p2,
    pair_counts.pair_order_count * 1.0 / nullif(product2_counts.product_order_count, 0) as confidence_p2_to_p1
from pair_counts
join product_order_counts as product1_counts
    on pair_counts.product1_id = product1_counts.product_id
join product_order_counts as product2_counts
    on pair_counts.product2_id = product2_counts.product_id
cross join total_orders

