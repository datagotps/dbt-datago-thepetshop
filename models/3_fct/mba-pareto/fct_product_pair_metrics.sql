-- Optimized MBA Product Pair Metrics (Gold Layer)
-- Combines pairing logic + metrics calculation + filtering in one model
-- Output: Only meaningful pairs (support >= 0.001) for Power BI consumption
-- Eliminates need for intermediate int_product_pairs model
-- Full 5-Level Product Hierarchy: Division > Block > Category > Subcategory > Brand

{{ config(materialized='table') }}

with order_items as (
    select *
    from {{ ref('int_mba_order_items') }}
),

-- Self-join to find product pairs
paired_orders as (
    select
        left_items.unified_order_id,
        -- Product 1 Details
        left_items.product_id as product1_id,
        left_items.product_name as product1_name,
        -- Product 1 Hierarchy (5 Levels)
        left_items.item_division as product1_division,      -- Level 1
        left_items.item_block as product1_block,            -- Level 2
        left_items.item_category as product1_category,      -- Level 3
        left_items.item_subcategory as product1_subcategory,-- Level 4
        left_items.item_brand as product1_brand,            -- Level 5
        -- Product 2 Details
        right_items.product_id as product2_id,
        right_items.product_name as product2_name,
        -- Product 2 Hierarchy (5 Levels)
        right_items.item_division as product2_division,     -- Level 1
        right_items.item_block as product2_block,           -- Level 2
        right_items.item_category as product2_category,     -- Level 3
        right_items.item_subcategory as product2_subcategory,-- Level 4
        right_items.item_brand as product2_brand            -- Level 5
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
        product1_block,
        product1_category,
        product1_subcategory,
        product1_brand,
        product2_id,
        product2_name,
        product2_division,
        product2_block,
        product2_category,
        product2_subcategory,
        product2_brand,
        count(distinct unified_order_id) as pair_order_count
    from paired_orders
    group by
        product1_id, product1_name, product1_division, product1_block, product1_category, 
        product1_subcategory, product1_brand,
        product2_id, product2_name, product2_division, product2_block, product2_category,
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
        -- Pair Key for unique identification
        concat(pair_counts.product1_id, '||', pair_counts.product2_id) as pair_key,
        pair_counts.*,
        product1_counts.product_order_count as product1_order_count,
        product2_counts.product_order_count as product2_order_count,
        total_orders.total_orders,
        -- Support: P(A ∩ B) - probability both products appear together
        pair_counts.pair_order_count * 1.0 / nullif(total_orders.total_orders, 0) as support,
        -- Confidence P1→P2: P(B|A) - if bought P1, probability of buying P2
        pair_counts.pair_order_count * 1.0 / nullif(product1_counts.product_order_count, 0) as confidence_p1_to_p2,
        -- Confidence P2→P1: P(A|B) - if bought P2, probability of buying P1
        pair_counts.pair_order_count * 1.0 / nullif(product2_counts.product_order_count, 0) as confidence_p2_to_p1,
        -- Lift: How much more likely than random chance (>1 = positive association)
        (pair_counts.pair_order_count * 1.0 / nullif(total_orders.total_orders, 0)) 
            / nullif(
                (product1_counts.product_order_count * 1.0 / total_orders.total_orders) 
                * (product2_counts.product_order_count * 1.0 / total_orders.total_orders), 
                0
            ) as lift
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

