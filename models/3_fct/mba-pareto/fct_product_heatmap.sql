-- Product Hierarchy Heatmap (Gold Layer)
-- Analyzes co-occurrence of product hierarchy levels within orders
-- Full 5-Level Product Hierarchy: Division > Block > Category > Subcategory > Brand
-- Use for Power BI heatmap visualizations showing category affinity

{{ config(materialized='table') }}

with order_attributes as (
    select distinct
        unified_order_id,
        -- Full Product Hierarchy (5 Levels) with Dynamic Sort Orders
        item_division,                    -- Level 1: Pet (DOG, CAT, FISH, etc.)
        item_division_sort_order,         -- Level 1 sort (by revenue)
        item_block,                       -- Level 2: Block (FOOD, ACCESSORIES, etc.)
        item_block_sort_order,            -- Level 2 sort (by revenue)
        item_category,                    -- Level 3: Category (Dry Food, Wet Food, etc.)
        item_category_sort_order,         -- Level 3 sort (by revenue) - NEW
        item_subcategory,                 -- Level 4: Subcategory (item type)
        item_subcategory_sort_order,      -- Level 4 sort (by revenue) - NEW
        item_brand,                       -- Level 5: Brand
        item_brand_sort_order             -- Level 5 sort (by revenue) - NEW
    from {{ ref('fact_commercial') }}
    where unified_order_id is not null
),

-- Level 1: Division pairs
division_orders as (
    select
        unified_order_id,
        item_division,
        item_division_sort_order
    from order_attributes
    where item_division is not null
),

-- Level 2: Block pairs
block_orders as (
    select
        unified_order_id,
        item_block,
        item_block_sort_order
    from order_attributes
    where item_block is not null
),

-- Level 3: Category pairs
category_orders as (
    select
        unified_order_id,
        item_category,
        item_category_sort_order
    from order_attributes
    where item_category is not null
),

-- Level 4: Subcategory pairs
subcategory_orders as (
    select
        unified_order_id,
        item_subcategory,
        item_subcategory_sort_order
    from order_attributes
    where item_subcategory is not null
),

-- Level 5: Brand pairs
brand_orders as (
    select
        unified_order_id,
        item_brand,
        item_brand_sort_order
    from order_attributes
    where item_brand is not null
),

-- Division pair co-occurrence (Level 1)
division_pairs as (
    select
        'division' as pair_type,
        div1.item_division as member1_value,
        div2.item_division as member2_value,
        div1.item_division_sort_order as member1_sort_order,
        div2.item_division_sort_order as member2_sort_order,
        count(distinct div1.unified_order_id) as pair_order_count
    from division_orders div1
    join division_orders div2
        on div1.unified_order_id = div2.unified_order_id
        and (
            coalesce(div1.item_division_sort_order, 9999) < coalesce(div2.item_division_sort_order, 9999)
            or (
                coalesce(div1.item_division_sort_order, 9999) = coalesce(div2.item_division_sort_order, 9999)
                and div1.item_division < div2.item_division
            )
        )
    group by 1, 2, 3, 4, 5
),

-- Block pair co-occurrence (Level 2)
block_pairs as (
    select
        'block' as pair_type,
        block1.item_block as member1_value,
        block2.item_block as member2_value,
        block1.item_block_sort_order as member1_sort_order,
        block2.item_block_sort_order as member2_sort_order,
        count(distinct block1.unified_order_id) as pair_order_count
    from block_orders block1
    join block_orders block2
        on block1.unified_order_id = block2.unified_order_id
        and (
            coalesce(block1.item_block_sort_order, 9999) < coalesce(block2.item_block_sort_order, 9999)
            or (
                coalesce(block1.item_block_sort_order, 9999) = coalesce(block2.item_block_sort_order, 9999)
                and block1.item_block < block2.item_block
            )
        )
    group by 1, 2, 3, 4, 5
),

-- Category pair co-occurrence (Level 3) - NOW WITH SORT ORDER
category_pairs as (
    select
        'category' as pair_type,
        cat1.item_category as member1_value,
        cat2.item_category as member2_value,
        cat1.item_category_sort_order as member1_sort_order,
        cat2.item_category_sort_order as member2_sort_order,
        count(distinct cat1.unified_order_id) as pair_order_count
    from category_orders cat1
    join category_orders cat2
        on cat1.unified_order_id = cat2.unified_order_id
        and (
            coalesce(cat1.item_category_sort_order, 9999) < coalesce(cat2.item_category_sort_order, 9999)
            or (
                coalesce(cat1.item_category_sort_order, 9999) = coalesce(cat2.item_category_sort_order, 9999)
        and cat1.item_category < cat2.item_category
            )
        )
    group by 1, 2, 3, 4, 5
),

-- Subcategory pair co-occurrence (Level 4) - NOW WITH SORT ORDER
subcategory_pairs as (
    select
        'subcategory' as pair_type,
        sub1.item_subcategory as member1_value,
        sub2.item_subcategory as member2_value,
        sub1.item_subcategory_sort_order as member1_sort_order,
        sub2.item_subcategory_sort_order as member2_sort_order,
        count(distinct sub1.unified_order_id) as pair_order_count
    from subcategory_orders sub1
    join subcategory_orders sub2
        on sub1.unified_order_id = sub2.unified_order_id
        and (
            coalesce(sub1.item_subcategory_sort_order, 9999) < coalesce(sub2.item_subcategory_sort_order, 9999)
            or (
                coalesce(sub1.item_subcategory_sort_order, 9999) = coalesce(sub2.item_subcategory_sort_order, 9999)
                and sub1.item_subcategory < sub2.item_subcategory
            )
        )
    group by 1, 2, 3, 4, 5
),

-- Brand pair co-occurrence (Level 5) - NOW WITH SORT ORDER
brand_pairs as (
    select
        'brand' as pair_type,
        br1.item_brand as member1_value,
        br2.item_brand as member2_value,
        br1.item_brand_sort_order as member1_sort_order,
        br2.item_brand_sort_order as member2_sort_order,
        count(distinct br1.unified_order_id) as pair_order_count
    from brand_orders br1
    join brand_orders br2
        on br1.unified_order_id = br2.unified_order_id
        and (
            coalesce(br1.item_brand_sort_order, 9999) < coalesce(br2.item_brand_sort_order, 9999)
            or (
                coalesce(br1.item_brand_sort_order, 9999) = coalesce(br2.item_brand_sort_order, 9999)
        and br1.item_brand < br2.item_brand
            )
        )
    group by 1, 2, 3, 4, 5
),

unioned_pairs as (
    select * from division_pairs
    union all
    select * from block_pairs
    union all
    select * from category_pairs
    union all
    select * from subcategory_pairs
    union all
    select * from brand_pairs
)

select
    concat(pair_type, '||', member1_value, '||', member2_value) as pair_key,
    pair_type,
    member1_value,
    member2_value,
    member1_sort_order,
    member2_sort_order,
    pair_order_count,
    -- Convenience columns for Power BI filtering
    case when pair_type = 'division' then member1_value end as division1,
    case when pair_type = 'division' then member2_value end as division2,
    case when pair_type = 'block' then member1_value end as block1,
    case when pair_type = 'block' then member2_value end as block2,
    case when pair_type = 'category' then member1_value end as category1,
    case when pair_type = 'category' then member2_value end as category2,
    case when pair_type = 'subcategory' then member1_value end as subcategory1,
    case when pair_type = 'subcategory' then member2_value end as subcategory2,
    case when pair_type = 'brand' then member1_value end as brand1,
    case when pair_type = 'brand' then member2_value end as brand2
from unioned_pairs

