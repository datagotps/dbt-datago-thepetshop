-- Pareto/ABC Analysis by Product Hierarchy (Gold Layer)
-- Calculates revenue rankings and cumulative percentages across all hierarchy levels
-- Full 5-Level Product Hierarchy: Division > Block > Category > Subcategory > Brand
-- Use for 80/20 analysis and top performer identification in Power BI

{{ config(materialized='table') }}

with base_revenue as (
    select
        item_no_ as item_id,
        sum(sales_amount__actual_) as revenue
    from {{ ref('fact_commercial') }}
    where transaction_type = 'Sale'
    group by item_no_
),

base_items as (
    select
        dim.item_id,
        dim.item_name,
        dim.primary_sales_channel,
        -- Full Product Hierarchy (5 Levels) - Consistent Naming
        dim.item_division,                -- Level 1: Pet (DOG, CAT, FISH, etc.)
        dim.item_division_sort_order,     -- Sort order by revenue
        dim.item_block,                   -- Level 2: Block (FOOD, ACCESSORIES, etc.)
        dim.item_block_sort_order,        -- Sort order by revenue
        dim.item_category,                -- Level 3: Category (Dry Food, Wet Food, etc.)
        dim.item_category_sort_order,     -- Sort order by revenue
        dim.item_subcategory,             -- Level 4: Subcategory (item type)
        dim.item_subcategory_sort_order,  -- Sort order by revenue
        dim.item_brand,                   -- Level 5: Brand
        dim.item_brand_sort_order,        -- Sort order by revenue
        coalesce(rev.revenue, 0) as revenue
    from {{ ref('dim_items') }} dim
    left join base_revenue rev
        on dim.item_id = rev.item_id
),

ranking_window_calculations as (
    select
        *,
        -- Global Rankings
        dense_rank() over (order by revenue desc) as rank_global,
        sum(revenue) over (order by revenue desc rows between unbounded preceding and current row) as cumulative_revenue_global,
        sum(revenue) over () as total_revenue_global,

        -- By Primary Sales Channel
        dense_rank() over (partition by primary_sales_channel order by revenue desc) as rank_channel,
        sum(revenue) over (
            partition by primary_sales_channel
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_channel,
        sum(revenue) over (partition by primary_sales_channel) as total_revenue_channel,

        -- Level 1: By Division
        dense_rank() over (partition by item_division order by revenue desc) as rank_division,
        sum(revenue) over (
            partition by item_division
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_division,
        sum(revenue) over (partition by item_division) as total_revenue_division,

        -- Level 2: By Block
        dense_rank() over (partition by item_block order by revenue desc) as rank_block,
        sum(revenue) over (
            partition by item_block
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_block,
        sum(revenue) over (partition by item_block) as total_revenue_block,

        -- Level 3: By Category
        dense_rank() over (partition by item_category order by revenue desc) as rank_category,
        sum(revenue) over (
            partition by item_category
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_category,
        sum(revenue) over (partition by item_category) as total_revenue_category,

        -- Level 4: By Subcategory
        dense_rank() over (partition by item_subcategory order by revenue desc) as rank_subcategory,
        sum(revenue) over (
            partition by item_subcategory
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_subcategory,
        sum(revenue) over (partition by item_subcategory) as total_revenue_subcategory,

        -- Level 5: By Brand
        dense_rank() over (partition by item_brand order by revenue desc) as rank_brand,
        sum(revenue) over (
            partition by item_brand
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_brand,
        sum(revenue) over (partition by item_brand) as total_revenue_brand
    from base_items
)

    select
    -- Item Identifiers
        item_id,
        item_name,
        primary_sales_channel,
    
    -- Full Product Hierarchy (5 Levels) with Sort Orders
    item_division,                        -- Level 1
    item_division_sort_order,             -- Sort by revenue (for Power BI Sort by Column)
    item_block,                           -- Level 2
    item_block_sort_order,                -- Sort by revenue
    item_category,                        -- Level 3
    item_category_sort_order,             -- Sort by revenue
    item_subcategory,                     -- Level 4
    item_subcategory_sort_order,          -- Sort by revenue
    item_brand,                           -- Level 5
    item_brand_sort_order,                -- Sort by revenue
    
    -- Item Revenue (single column, not duplicated)
    revenue,
    
    -- Global Pareto Metrics
    rank_global,
        cumulative_revenue_global / nullif(total_revenue_global, 0) as cumulative_pct_global,
    case
        when cumulative_revenue_global / nullif(total_revenue_global, 0) <= 0.80 then 'A'
        when cumulative_revenue_global / nullif(total_revenue_global, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_global,

    -- Channel Pareto Metrics
    rank_channel,
    cumulative_revenue_channel / nullif(total_revenue_channel, 0) as cumulative_pct_channel,
    case
        when cumulative_revenue_channel / nullif(total_revenue_channel, 0) <= 0.80 then 'A'
        when cumulative_revenue_channel / nullif(total_revenue_channel, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_channel,

    -- Level 1: Division Pareto Metrics
        rank_division,
        cumulative_revenue_division / nullif(total_revenue_division, 0) as cumulative_pct_division,
    case
        when cumulative_revenue_division / nullif(total_revenue_division, 0) <= 0.80 then 'A'
        when cumulative_revenue_division / nullif(total_revenue_division, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_division,

    -- Level 2: Block Pareto Metrics
    rank_block,
    cumulative_revenue_block / nullif(total_revenue_block, 0) as cumulative_pct_block,
    case
        when cumulative_revenue_block / nullif(total_revenue_block, 0) <= 0.80 then 'A'
        when cumulative_revenue_block / nullif(total_revenue_block, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_block,

    -- Level 3: Category Pareto Metrics
        rank_category,
        cumulative_revenue_category / nullif(total_revenue_category, 0) as cumulative_pct_category,
    case
        when cumulative_revenue_category / nullif(total_revenue_category, 0) <= 0.80 then 'A'
        when cumulative_revenue_category / nullif(total_revenue_category, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_category,

    -- Level 4: Subcategory Pareto Metrics
        rank_subcategory,
        cumulative_revenue_subcategory / nullif(total_revenue_subcategory, 0) as cumulative_pct_subcategory,
    case
        when cumulative_revenue_subcategory / nullif(total_revenue_subcategory, 0) <= 0.80 then 'A'
        when cumulative_revenue_subcategory / nullif(total_revenue_subcategory, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_subcategory,

    -- Level 5: Brand Pareto Metrics
        rank_brand,
    cumulative_revenue_brand / nullif(total_revenue_brand, 0) as cumulative_pct_brand,
    case
        when cumulative_revenue_brand / nullif(total_revenue_brand, 0) <= 0.80 then 'A'
        when cumulative_revenue_brand / nullif(total_revenue_brand, 0) <= 0.95 then 'B'
        else 'C'
    end as pareto_class_brand

    from ranking_window_calculations
