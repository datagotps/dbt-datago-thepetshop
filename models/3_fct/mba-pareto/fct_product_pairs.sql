with order_items as (
    select *
    from {{ ref('int_mba_order_items') }}
),

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
)

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
    product2_brand

