with source_orders as (
    select
        unified_order_id,
        item_no_ as product_id,
        item_name as product_name,
        -- Full Product Hierarchy (5 Levels)
        item_division,                    -- Level 1: Pet (DOG, CAT, FISH, etc.)
        item_block,                       -- Level 2: Block (FOOD, ACCESSORIES, etc.)
        item_category,                    -- Level 3: Category (Dry Food, Wet Food, etc.)
        item_subcategory,                 -- Level 4: Subcategory (item type)
        item_brand                        -- Level 5: Brand
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
    -- Full Product Hierarchy (5 Levels)
    item_division,                        -- Level 1: Pet
    item_block,                           -- Level 2: Block
    item_category,                        -- Level 3: Category
    item_subcategory,                     -- Level 4: Subcategory
    item_brand                            -- Level 5: Brand
from deduplicated_orders
where product_rank = 1

