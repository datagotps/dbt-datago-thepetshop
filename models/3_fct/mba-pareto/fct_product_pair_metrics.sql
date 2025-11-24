-- Optimized MBA Product Pair Metrics (Gold Layer)
-- Combines pairing logic + metrics calculation + filtering in one model
-- Output: Only meaningful pairs (support >= 0.001) for Power BI consumption
-- Eliminates need for intermediate int_product_pairs model

with order_items as (
    select *
    from {{ ref('int_mba_order_items') }}
),

-- Self-join to find product pairs
paired_orders as (
    select
        left_items.unified_order_id,
        left_items.product_id as product1_id,
        left_items.product_name as product1_name,
        left_items.division as product1_division,
        left_items.item_category as product1_category,
        left_items.item_subcategory as product1_subcategory,
        left_items.item_brand as product1_brand,
        right_items.product_id as product2_id,
        right_items.product_name as product2_name,
        right_items.division as product2_division,
        right_items.item_category as product2_category,
        right_items.item_subcategory as product2_subcategory,
        right_items.item_brand as product2_brand
    from order_items left_items
    join order_items right_items
        on left_items.unified_order_id = right_items.unified_order_id
        and left_items.product_id < right_items.product_id
),

-- Aggregate pair counts
pair_counts as (
    select
        product1_id,
        product1_name,
        product1_division,
        product1_category,
        product1_subcategory,
        product1_brand,
        product2_id,
        product2_name,
        product2_division,
        product2_category,
        product2_subcategory,
        product2_brand,
        count(distinct unified_order_id) as pair_order_count
    from paired_orders
    group by
        product1_id, product1_name, product1_division, product1_category, 
        product1_subcategory, product1_brand,
        product2_id, product2_name, product2_division, product2_category,
        product2_subcategory, product2_brand
),

-- Calculate total orders
total_orders as (
    select count(distinct unified_order_id) as total_orders
    from order_items
),

-- Calculate product-level order counts
product_order_counts as (
    select
        product_id,
        count(distinct unified_order_id) as product_order_count
    from order_items
    group by product_id
),

-- Calculate MBA metrics and filter
metrics as (
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
)

-- Filter to only meaningful pairs (0.1%+ of orders)
select *
from metrics
where support >= 0.001
  and pair_order_count >= 100

