

select

-- Core Item Identifiers
item_id,                           -- dim (item unique identifier)
item_name,                         -- dim (item description)
item_description,                  -- dim (detailed item description)
varient_item,

-- Categorization Hierarchy
division,                          -- dim: DOG, CAT, FISH, BIRD, etc
division_sort_order,               -- dim: 1-10 (sort order)
item_category,                     -- dim: FOOD, ACCESSORIES, HEALTH & HYGIENE, etc
item_category_sort_order,          -- dim: 1-999 (sort order)
item_subcategory,                  -- dim: Dry Food, Wet Food, Treats, etc
item_brand,                        -- dim (brand name)
inventory_posting_group,           -- dim (inventory posting group)

-- Performance Metrics
lifetime_transactions,             -- fact (total transaction count)
total_sales,                       -- fact (sale transaction count)
total_refunds,                     -- fact (refund transaction count)
unique_customers,                  -- fact (distinct customer count)
units_sold,                        -- fact (total units sold)
units_returned,                    -- fact (total units returned)
return_rate_pct,                   -- fact (percentage of returns)

-- Financial Metrics
lifetime_revenue,                  -- fact (AED total revenue)
lifetime_refunds,                  -- fact (AED total refund amount)
lifetime_gross_revenue,            -- fact (AED gross revenue before discount)
lifetime_cost,                     -- fact (AED total cost)
lifetime_discounts,                -- fact (AED total discount given)
gross_margin_pct,                  -- fact (gross margin percentage)
avg_selling_price,                 -- fact (AED average price)
avg_quantity_per_sale,             -- fact (average units per transaction)
discount_rate_pct,                 -- fact (percentage of discounted sales)

-- ABC Classification
abc_classification,                -- dim: A, B, C
revenue_contribution_pct,          -- fact (percentage of total revenue)

-- Sales Velocity & Frequency
avg_monthly_sales_3m,              -- fact (3-month average monthly sales)
avg_weekly_sales_4w,               -- fact (4-week average weekly sales)
purchase_frequency_tier,           -- dim: Very High, High, Medium, Low, Very Low
velocity_classification,           -- dim: Fast Moving, Regular Moving, Slow Moving, Non Moving

-- Channel Mix
online_sales_pct,                  -- fact (percentage sold online)
shop_sales_pct,                    -- fact (percentage sold in shops)
affiliate_sales_pct,               -- fact (percentage sold via affiliates)
primary_sales_channel,             -- dim: Online, Shop, Affiliate, None

-- Status & Lifecycle
first_sale_date,                   -- dim (date of first sale)
last_sale_date,                    -- dim (date of last sale)
days_since_last_sale,              -- fact (days since last transaction)
active_months_count,               -- fact (number of active months)
item_status,                       -- dim: Active, Slow, Dormant, Inactive, Never Sold

-- MBA Support Metrics
is_high_support_item,              -- fact: 0, 1 (flag for MBA analysis)
cross_sell_potential_score,        -- fact: 0-100 (cross-sell opportunity score)

-- Metadata
dim_created_date,                  -- dim (dimension creation date)
dim_last_updated_at                -- dim (dimension last update timestamp)


FROM {{ ref('int_items_2') }}

where varient_item = 0
-- Test: Final test with correct classic token


