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
        -- Updated Business Hierarchy
        dim.item_division as division,      -- Level 1: Pet
        dim.item_block as category,         -- Level 2: Block
        dim.item_category as subcategory,   -- Level 3: Category
        dim.item_brand as brand,            -- Level 5: Brand
        coalesce(rev.revenue, 0) as revenue
    from {{ ref('dim_items') }} dim
    left join base_revenue rev
        on dim.item_id = rev.item_id
),

ranking_window_calculations as (
    select
        *,
        dense_rank() over (order by revenue desc) as rank_global,
        sum(revenue) over (order by revenue desc rows between unbounded preceding and current row) as cumulative_revenue_global,
        sum(revenue) over () as total_revenue_global,

        dense_rank() over (partition by primary_sales_channel order by revenue desc) as rank_primary_sales_channel,
        sum(revenue) over (
            partition by primary_sales_channel
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_primary_sales_channel,
        sum(revenue) over (partition by primary_sales_channel) as total_revenue_primary_sales_channel,

        dense_rank() over (partition by division order by revenue desc) as rank_division,
        sum(revenue) over (
            partition by division
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_division,
        sum(revenue) over (partition by division) as total_revenue_division,

        dense_rank() over (partition by category order by revenue desc) as rank_category,
        sum(revenue) over (
            partition by category
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_category,
        sum(revenue) over (partition by category) as total_revenue_category,

        dense_rank() over (partition by subcategory order by revenue desc) as rank_subcategory,
        sum(revenue) over (
            partition by subcategory
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_subcategory,
        sum(revenue) over (partition by subcategory) as total_revenue_subcategory,

        dense_rank() over (partition by brand order by revenue desc) as rank_brand,
        sum(revenue) over (
            partition by brand
            order by revenue desc rows between unbounded preceding and current row
        ) as cumulative_revenue_brand,
        sum(revenue) over (partition by brand) as total_revenue_brand
    from base_items
),

final_output as (
    select
        item_id,
        item_name,
        primary_sales_channel,
        division,
        category,
        subcategory,
        brand,
        revenue as revenue_per_global,
        rank_global as rank_global,
        cumulative_revenue_global / nullif(total_revenue_global, 0) as cumulative_pct_global,

        revenue as revenue_per_primary_sales_channel,
        rank_primary_sales_channel,
        cumulative_revenue_primary_sales_channel
            / nullif(total_revenue_primary_sales_channel, 0) as cumulative_pct_primary_sales_channel,

        revenue as revenue_per_division,
        rank_division,
        cumulative_revenue_division / nullif(total_revenue_division, 0) as cumulative_pct_division,

        revenue as revenue_per_category,
        rank_category,
        cumulative_revenue_category / nullif(total_revenue_category, 0) as cumulative_pct_category,

        revenue as revenue_per_subcategory,
        rank_subcategory,
        cumulative_revenue_subcategory / nullif(total_revenue_subcategory, 0) as cumulative_pct_subcategory,

        revenue as revenue_per_brand,
        rank_brand,
        cumulative_revenue_brand / nullif(total_revenue_brand, 0) as cumulative_pct_brand
    from ranking_window_calculations
)

select *
from final_output

