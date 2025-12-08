-- =====================================================
-- DIM_ITEMS
-- Comprehensive dimension table for items in pet shop business
-- Combines static attributes with calculated metrics
-- =====================================================

WITH item_metrics AS (
    -- Calculate item performance metrics from transaction history
    SELECT 
        item_no_,
        
        -- Sales Metrics
        COUNT(DISTINCT document_no_) as total_transactions,
        COUNT(DISTINCT CASE WHEN transaction_type = 'Sale' THEN document_no_ END) as sale_transactions,
        COUNT(DISTINCT CASE WHEN transaction_type = 'Refund' THEN document_no_ END) as refund_transactions,
        COUNT(DISTINCT unified_customer_id) as unique_customers,
        
        -- Quantity Metrics
        SUM(CASE WHEN transaction_type = 'Sale' THEN invoiced_quantity ELSE 0 END) as total_units_sold,
        SUM(CASE WHEN transaction_type = 'Refund' THEN ABS(invoiced_quantity) ELSE 0 END) as total_units_returned,
        
        -- Revenue Metrics
        SUM(CASE WHEN transaction_type = 'Sale' THEN sales_amount__actual_ ELSE 0 END) as total_revenue,
        SUM(CASE WHEN transaction_type = 'Refund' THEN ABS(sales_amount__actual_) ELSE 0 END) as total_refund_amount,
        SUM(CASE WHEN transaction_type = 'Sale' THEN sales_amount_gross ELSE 0 END) as total_gross_revenue,
        SUM(CASE WHEN transaction_type = 'Sale' THEN cost_amount__actual_ ELSE 0 END) as total_cost,
        
        -- Discount Metrics
        SUM(CASE WHEN transaction_type = 'Sale' THEN discount_amount ELSE 0 END) as total_discount_given,
        COUNT(DISTINCT CASE WHEN has_discount = 1 AND transaction_type = 'Sale' THEN document_no_ END) as discounted_transactions,
        
        -- Channel Distribution
        COUNT(DISTINCT CASE WHEN sales_channel = 'Online' THEN document_no_ END) as online_transactions,
        COUNT(DISTINCT CASE WHEN sales_channel = 'Shop' THEN document_no_ END) as shop_transactions,
        COUNT(DISTINCT CASE WHEN sales_channel = 'Affiliate' THEN document_no_ END) as affiliate_transactions,
        
        -- Time-based Metrics
        MIN(posting_date) as first_sale_date,
        MAX(posting_date) as last_sale_date,
        COUNT(DISTINCT DATE_TRUNC(posting_date, MONTH)) as active_months,
        
        -- Average Metrics
        AVG(CASE WHEN transaction_type = 'Sale' THEN sales_amount__actual_ ELSE NULL END) as avg_selling_price,
        AVG(CASE WHEN transaction_type = 'Sale' THEN invoiced_quantity ELSE NULL END) as avg_quantity_per_transaction

    FROM {{ ref('fact_commercial') }}
    --WHERE posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)  -- Last 12 months for metrics
    GROUP BY item_no_
),

item_abc_classification AS (
    -- Calculate ABC classification based on revenue
    SELECT 
        item_no_,
        total_revenue,
        SUM(total_revenue) OVER () as grand_total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS UNBOUNDED PRECEDING) as cumulative_revenue
    FROM item_metrics
),

item_velocity AS (
    -- Calculate sales velocity and seasonality
    SELECT 
        item_no_,
        COUNT(DISTINCT DATE_TRUNC(posting_date, WEEK)) as weeks_with_sales,
        COUNT(DISTINCT DATE_TRUNC(posting_date, MONTH)) as months_with_sales,
        
        -- Monthly velocity (last 3 months)
        COUNT(DISTINCT CASE 
            WHEN posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) 
            THEN document_no_ 
        END) / 3.0 as avg_monthly_transactions_3m,
        
        -- Weekly velocity (last 4 weeks)
        COUNT(DISTINCT CASE 
            WHEN posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK) 
            THEN document_no_ 
        END) / 4.0 as avg_weekly_transactions_4w
        
    FROM {{ ref('fact_commercial') }}
    WHERE transaction_type = 'Sale'
        --AND posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY item_no_
),

-- =====================================================
-- XYZ Classification (Demand Variability)
-- Based on Coefficient of Variation (CV) of monthly sales
-- X = Low variability (CV < 0.5) - Predictable demand
-- Y = Medium variability (0.5 <= CV < 1.0) - Variable demand
-- Z = High variability (CV >= 1.0) - Sporadic/unpredictable
-- =====================================================
monthly_sales AS (
    -- Get monthly sales count per item for last 12 months
    SELECT 
        item_no_,
        DATE_TRUNC(posting_date, MONTH) as sale_month,
        COUNT(DISTINCT document_no_) as monthly_transactions,
        SUM(sales_amount__actual_) as monthly_revenue
    FROM {{ ref('fact_commercial') }}
    WHERE transaction_type = 'Sale'
        AND posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY item_no_, DATE_TRUNC(posting_date, MONTH)
),

xyz_classification AS (
    SELECT 
        item_no_,
        -- Statistics for transactions
        AVG(monthly_transactions) as avg_monthly_transactions,
        STDDEV(monthly_transactions) as stddev_monthly_transactions,
        -- Statistics for revenue
        AVG(monthly_revenue) as avg_monthly_revenue,
        STDDEV(monthly_revenue) as stddev_monthly_revenue,
        -- Count of months with sales
        COUNT(DISTINCT sale_month) as months_with_data,
        -- Coefficient of Variation (CV) = StdDev / Mean
        -- Using transactions as the basis (more stable than revenue)
        CASE 
            WHEN AVG(monthly_transactions) > 0 
            THEN STDDEV(monthly_transactions) / AVG(monthly_transactions)
            ELSE 999  -- No data = high variability
        END as coefficient_of_variation,
        -- XYZ Classification
        CASE 
            WHEN COUNT(DISTINCT sale_month) < 3 THEN 'Z'  -- Not enough data
            WHEN AVG(monthly_transactions) = 0 THEN 'Z'   -- No sales
            WHEN STDDEV(monthly_transactions) / NULLIF(AVG(monthly_transactions), 0) < 0.5 THEN 'X'
            WHEN STDDEV(monthly_transactions) / NULLIF(AVG(monthly_transactions), 0) < 1.0 THEN 'Y'
            ELSE 'Z'
        END as xyz_class
    FROM monthly_sales
    GROUP BY item_no_
)

SELECT DISTINCT
    -- =====================================================
    -- Core Item Attributes (from int_items)
    -- =====================================================
    it.item_no_ as item_id,
    it.item_name,
    it.item_name as item_description,  -- Using item_name as description
    
    -- =====================================================
    -- Categorization Hierarchy (Updated Business Naming)
    -- =====================================================
    it.item_division,              -- Level 1: Pet (was division)
    it.item_division_sort_order,
    it.item_block,                 -- Level 2: Block (was item_category)
    it.item_block_sort_order,
    it.item_category,              -- Level 3: Category (was item_subcategory)
    it.item_category_sort_order,
    it.item_subcategory,           -- Level 4: Subcategory (was item_type)
    it.item_subcategory_sort_order,
    it.item_brand,                 -- Level 5: Brand
    it.item_brand_sort_order,
    it.inventory_posting_group,
    it.varient_item,
    
    -- =====================================================
    -- Performance Metrics (from item_metrics)
    -- =====================================================
    COALESCE(m.total_transactions, 0) as lifetime_transactions,
    COALESCE(m.sale_transactions, 0) as total_sales,
    COALESCE(m.refund_transactions, 0) as total_refunds,
    COALESCE(m.unique_customers, 0) as unique_customers,
    COALESCE(m.total_units_sold, 0) as units_sold,
    COALESCE(m.total_units_returned, 0) as units_returned,
    
    -- Return Rate
    CASE 
        WHEN m.total_units_sold > 0 
        THEN ROUND(m.total_units_returned * 100.0 / m.total_units_sold, 2)
        ELSE 0 
    END as return_rate_pct,
    
    -- =====================================================
    -- Financial Metrics
    -- =====================================================
    ROUND(COALESCE(m.total_revenue, 0), 2) as lifetime_revenue,
    ROUND(COALESCE(m.total_refund_amount, 0), 2) as lifetime_refunds,
    ROUND(COALESCE(m.total_gross_revenue, 0), 2) as lifetime_gross_revenue,
    ROUND(COALESCE(m.total_cost, 0), 2) as lifetime_cost,
    ROUND(COALESCE(m.total_discount_given, 0), 2) as lifetime_discounts,
    
    -- Margin Calculation
    CASE 
        WHEN m.total_revenue > 0 
        THEN ROUND((m.total_revenue - m.total_cost) * 100.0 / m.total_revenue, 2)
        ELSE 0 
    END as gross_margin_pct,
    
    -- Average Prices
    ROUND(COALESCE(m.avg_selling_price, 0), 2) as avg_selling_price,
    ROUND(COALESCE(m.avg_quantity_per_transaction, 0), 2) as avg_quantity_per_sale,
    
    -- Discount Rate
    CASE 
        WHEN m.sale_transactions > 0 
        THEN ROUND(m.discounted_transactions * 100.0 / m.sale_transactions, 2)
        ELSE 0 
    END as discount_rate_pct,
    
    -- =====================================================
    -- ABC Classification
    -- =====================================================
    CASE 
        WHEN abc.cumulative_revenue <= abc.grand_total_revenue * 0.80 THEN 'A'
        WHEN abc.cumulative_revenue <= abc.grand_total_revenue * 0.95 THEN 'B'
        ELSE 'C'
    END as abc_classification,
    
    -- Revenue Contribution
    CASE 
        WHEN abc.grand_total_revenue > 0 
        THEN ROUND(abc.total_revenue * 100.0 / abc.grand_total_revenue, 4)
        ELSE 0 
    END as revenue_contribution_pct,
    
    -- =====================================================
    -- Sales Velocity & Frequency Tiers
    -- =====================================================
    COALESCE(v.avg_monthly_transactions_3m, 0) as avg_monthly_sales_3m,
    COALESCE(v.avg_weekly_transactions_4w, 0) as avg_weekly_sales_4w,
    
    -- Purchase Frequency Classification
    CASE 
        WHEN m.total_transactions >= 100 THEN 'Very High'
        WHEN m.total_transactions >= 50 THEN 'High'
        WHEN m.total_transactions >= 20 THEN 'Medium'
        WHEN m.total_transactions >= 5 THEN 'Low'
        ELSE 'Very Low'
    END as purchase_frequency_tier,
    -- Sort Order: Very High=1, High=2, Medium=3, Low=4, Very Low=5
    CASE 
        WHEN m.total_transactions >= 100 THEN 1
        WHEN m.total_transactions >= 50 THEN 2
        WHEN m.total_transactions >= 20 THEN 3
        WHEN m.total_transactions >= 5 THEN 4
        ELSE 5
    END as purchase_frequency_tier_sort_order,
    
    -- Velocity Classification
    CASE 
        WHEN v.avg_weekly_transactions_4w >= 10 THEN 'Fast Moving'
        WHEN v.avg_weekly_transactions_4w >= 2 THEN 'Regular Moving'
        WHEN v.avg_weekly_transactions_4w >= 0.5 THEN 'Slow Moving'
        ELSE 'Non Moving'
    END as velocity_classification,
    -- Sort Order: Fast=1, Regular=2, Slow=3, Non=4
    CASE 
        WHEN v.avg_weekly_transactions_4w >= 10 THEN 1
        WHEN v.avg_weekly_transactions_4w >= 2 THEN 2
        WHEN v.avg_weekly_transactions_4w >= 0.5 THEN 3
        ELSE 4
    END as velocity_classification_sort_order,
    
    -- =====================================================
    -- Channel Mix
    -- =====================================================
    CASE 
        WHEN m.total_transactions > 0 
        THEN ROUND(m.online_transactions * 100.0 / m.total_transactions, 2)
        ELSE 0 
    END as online_sales_pct,
    
    CASE 
        WHEN m.total_transactions > 0 
        THEN ROUND(m.shop_transactions * 100.0 / m.total_transactions, 2)
        ELSE 0 
    END as shop_sales_pct,
    
    CASE 
        WHEN m.total_transactions > 0 
        THEN ROUND(m.affiliate_transactions * 100.0 / m.total_transactions, 2)
        ELSE 0 
    END as affiliate_sales_pct,
    
    -- Primary Sales Channel
    CASE 
        WHEN m.online_transactions >= m.shop_transactions AND m.online_transactions >= m.affiliate_transactions THEN 'Online'
        WHEN m.shop_transactions >= m.online_transactions AND m.shop_transactions >= m.affiliate_transactions THEN 'Shop'
        WHEN m.affiliate_transactions > 0 THEN 'Affiliate'
        ELSE 'None'
    END as primary_sales_channel,
    -- Sort Order: Online=1, Shop=2, Affiliate=3, None=4
    CASE 
        WHEN m.online_transactions >= m.shop_transactions AND m.online_transactions >= m.affiliate_transactions THEN 1
        WHEN m.shop_transactions >= m.online_transactions AND m.shop_transactions >= m.affiliate_transactions THEN 2
        WHEN m.affiliate_transactions > 0 THEN 3
        ELSE 4
    END as primary_sales_channel_sort_order,
    
    -- =====================================================
    -- Status & Lifecycle
    -- =====================================================
    COALESCE(m.first_sale_date, CURRENT_DATE()) as first_sale_date,
    COALESCE(m.last_sale_date, DATE('1900-01-01')) as last_sale_date,
    DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) as days_since_last_sale,
    COALESCE(m.active_months, 0) as active_months_count,
    
    -- Item Status
    CASE 
        WHEN m.last_sale_date IS NULL THEN 'Never Sold'
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 30 THEN 'Active'
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 90 THEN 'Slow'
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 180 THEN 'Dormant'
        ELSE 'Inactive'
    END as item_status,
    -- Sort Order: Active=1, Slow=2, Dormant=3, Inactive=4, Never Sold=5
    CASE 
        WHEN m.last_sale_date IS NULL THEN 5
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 30 THEN 1
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 90 THEN 2
        WHEN DATE_DIFF(CURRENT_DATE(), m.last_sale_date, DAY) <= 180 THEN 3
        ELSE 4
    END as item_status_sort_order,
    
    -- =====================================================
    -- MBA Support Metrics
    -- =====================================================
    -- Item popularity for basket analysis
    CASE 
        WHEN m.total_transactions >= 50 THEN 1  -- High support item
        ELSE 0 
    END as is_high_support_item,
    
    -- Cross-sell potential score (0-100)
    CASE 
        WHEN m.unique_customers > 0 AND m.total_transactions > 0
        THEN LEAST(100, 
            (m.unique_customers * 0.3 +  -- Customer reach weight
             m.total_transactions * 0.4 +  -- Transaction frequency weight  
             CASE WHEN abc.cumulative_revenue <= abc.grand_total_revenue * 0.80 THEN 30 ELSE 0 END)  -- ABC weight
        )
        ELSE 0
    END as cross_sell_potential_score,
    
    -- =====================================================
    -- XYZ Classification (Demand Predictability)
    -- =====================================================
    COALESCE(xyz.coefficient_of_variation, 999) as coefficient_of_variation,
    COALESCE(xyz.avg_monthly_transactions, 0) as avg_monthly_transactions_12m,
    COALESCE(xyz.stddev_monthly_transactions, 0) as stddev_monthly_transactions,
    COALESCE(xyz.months_with_data, 0) as months_with_sales_data,
    COALESCE(xyz.xyz_class, 'Z') as xyz_class,
    -- Sort Order: X=1, Y=2, Z=3
    CASE COALESCE(xyz.xyz_class, 'Z')
        WHEN 'X' THEN 1
        WHEN 'Y' THEN 2
        ELSE 3
    END as xyz_class_sort_order,
    
    -- ABC-XYZ Combined Classification
    CASE 
        WHEN abc.cumulative_revenue <= abc.grand_total_revenue * 0.80 THEN 'A'
        WHEN abc.cumulative_revenue <= abc.grand_total_revenue * 0.95 THEN 'B'
        ELSE 'C'
    END || '-' || COALESCE(xyz.xyz_class, 'Z') as abc_xyz_class,
    
    -- =====================================================
    -- Metadata
    -- =====================================================
    CURRENT_DATE() as dim_created_date,
    CURRENT_DATETIME() as dim_last_updated_at

FROM {{ ref('int_items') }} as it
LEFT JOIN item_metrics as m ON it.item_no_ = m.item_no_
LEFT JOIN item_abc_classification as abc ON it.item_no_ = abc.item_no_
LEFT JOIN item_velocity as v ON it.item_no_ = v.item_no_
LEFT JOIN xyz_classification as xyz ON it.item_no_ = xyz.item_no_

-- Optional: Filter out items with no sales history if needed
-- WHERE m.total_transactions > 0 OR m.first_sale_date IS NOT NULL