-- Test: Team Ripo
-- Test team to me
-- hi hi
with item_source as (
    select *
    from {{ ref('int_items_2') }}
    where varient_item = 0
),

pair_metrics as (
    select *
    from {{ ref('fct_product_pair_metrics') }}
),

product_support_union as (
    select
        product1_id as product_id,
        product1_order_count as product_order_count,
        total_orders
    from pair_metrics

    union all

    select
        product2_id as product_id,
        product2_order_count as product_order_count,
        total_orders
    from pair_metrics
),

product_support as (
    select
        product_id,
        max(product_order_count) as product_order_count,
        max(total_orders) as total_orders
    from product_support_union
    group by product_id
),

mba_support as (
    select
        product_id,
        coalesce(product_order_count * 1.0 / nullif(total_orders, 0), 0) as mba_support_score
    from product_support
)

select

-- Core Item Identifiers
items.item_id,                           -- dim (item unique identifier)
items.item_name,                         -- dim (item description)
items.item_description,                  -- dim (detailed item description)
items.varient_item,

-- Categorization Hierarchy (Updated Business Naming)
items.item_division,                     -- dim: Level 1 - Pet (DOG, CAT, FISH, BIRD, etc.)
items.item_division_sort_order,          -- dim: 1-11 (sort order for item_division)
items.item_block,                        -- dim: Level 2 - Block (FOOD, ACCESSORIES, etc.)
items.item_block_sort_order,             -- dim: 1-999 (sort order for item_block)
items.item_category,                     -- dim: Level 3 - Category (Dry Food, Wet Food, Treats, etc.)
items.item_category_sort_order,          -- dim: sort order for item_category
items.item_subcategory,                  -- dim: Level 4 - Subcategory (item type)
items.item_subcategory_sort_order,       -- dim: sort order for item_subcategory
items.item_brand,                        -- dim: Level 5 - Brand
items.item_brand_sort_order,             -- dim: sort order for item_brand
items.inventory_posting_group,           -- dim (inventory posting group)

-- Performance Metrics
items.lifetime_transactions,             -- fact (total transaction count)
items.total_sales,                       -- fact (sale transaction count)
items.total_refunds,                     -- fact (refund transaction count)
items.unique_customers,                  -- fact (distinct customer count)
items.units_sold,                        -- fact (total units sold)
items.units_returned,                    -- fact (total units returned)
items.return_rate_pct,                   -- fact (percentage of returns)

-- Financial Metrics
items.lifetime_revenue,                  -- fact (AED total revenue)
items.lifetime_refunds,                  -- fact (AED total refund amount)
items.lifetime_gross_revenue,            -- fact (AED gross revenue before discount)
items.lifetime_cost,                     -- fact (AED total cost)
items.lifetime_discounts,                -- fact (AED total discount given)
items.gross_margin_pct,                  -- fact (gross margin percentage)
items.avg_selling_price,                 -- fact (AED average price)
items.avg_quantity_per_sale,             -- fact (average units per transaction)
items.discount_rate_pct,                 -- fact (percentage of discounted sales)

-- ABC Classification
items.abc_classification,                -- dim: A, B, C
items.revenue_contribution_pct,          -- fact (percentage of total revenue)

-- Sales Velocity & Frequency
items.avg_monthly_sales_3m,              -- fact (3-month average monthly sales)
items.avg_weekly_sales_4w,               -- fact (4-week average weekly sales)
items.purchase_frequency_tier,           -- dim: Very High, High, Medium, Low, Very Low
items.velocity_classification,           -- dim: Fast Moving, Regular Moving, Slow Moving, Non Moving

-- Channel Mix
items.online_sales_pct,                  -- fact (percentage sold online)
items.shop_sales_pct,                    -- fact (percentage sold in shops)
items.affiliate_sales_pct,               -- fact (percentage sold via affiliates)
items.primary_sales_channel,             -- dim: Online, Shop, Affiliate, None

-- Status & Lifecycle
items.first_sale_date,                   -- dim (date of first sale)
items.last_sale_date,                    -- dim (date of last sale)
items.days_since_last_sale,              -- fact (days since last transaction)
items.active_months_count,               -- fact (number of active months)
items.item_status,                       -- dim: Active, Slow, Dormant, Inactive, Never Sold

-- MBA Support Metrics
items.is_high_support_item,              -- fact: 0, 1 (flag for MBA analysis)
items.cross_sell_potential_score,        -- fact: 0-100 (cross-sell opportunity score)
coalesce(mba_support.mba_support_score, 0) as mba_support_score,

-- Metadata
items.dim_created_date,                  -- dim (dimension creation date)
items.dim_last_updated_at                -- dim (dimension last update timestamp)

FROM item_source items
left join mba_support
    on items.item_id = mba_support.product_id


-- Test: Deploy keys with write access enabled
-- Test: Deploy keys with write access enabled


