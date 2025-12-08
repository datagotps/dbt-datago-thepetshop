{{ config(materialized='table') }}

with order_attributes as (
    select distinct
        unified_order_id,
        item_block,
        item_block_sort_order,
        item_category,
        item_brand
    from {{ ref('fact_commercial') }}
    where unified_order_id is not null
),

block_orders as (
    select
        unified_order_id,
        item_block,
        item_block_sort_order
    from order_attributes
    where item_block is not null
),

category_orders as (
    select
        unified_order_id,
        item_category
    from order_attributes
    where item_category is not null
),

brand_orders as (
    select
        unified_order_id,
        item_brand
    from order_attributes
    where item_brand is not null
),

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

category_pairs as (
    select
        'category' as pair_type,
        cat1.item_category as member1_value,
        cat2.item_category as member2_value,
        null as member1_sort_order,
        null as member2_sort_order,
        count(distinct cat1.unified_order_id) as pair_order_count
    from category_orders cat1
    join category_orders cat2
        on cat1.unified_order_id = cat2.unified_order_id
        and cat1.item_category < cat2.item_category
    group by 1, 2, 3, 4, 5
),

brand_pairs as (
    select
        'brand' as pair_type,
        br1.item_brand as member1_value,
        br2.item_brand as member2_value,
        null as member1_sort_order,
        null as member2_sort_order,
        count(distinct br1.unified_order_id) as pair_order_count
    from brand_orders br1
    join brand_orders br2
        on br1.unified_order_id = br2.unified_order_id
        and br1.item_brand < br2.item_brand
    group by 1, 2, 3, 4, 5
),

unioned_pairs as (
    select * from block_pairs
    union all
    select * from category_pairs
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
    case when pair_type = 'block' then member1_value end as block1,
    case when pair_type = 'block' then member2_value end as block2,
    case when pair_type = 'category' then member1_value end as category1,
    case when pair_type = 'category' then member2_value end as category2,
    case when pair_type = 'brand' then member1_value end as brand1,
    case when pair_type = 'brand' then member2_value end as brand2
from unioned_pairs

