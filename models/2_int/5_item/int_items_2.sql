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
)

SELECT DISTINCT
    -- =====================================================
    -- Core Item Attributes (from int_items)
    -- =====================================================
    it.item_no_ as item_id,
    it.item_name,
    it.item_name as item_description,  -- Using item_name as description
    
    -- =====================================================
    -- Categorization Hierarchy
    -- =====================================================
    it.division,
    it.division_sort_order,
    it.item_category,
    it.item_category_sort_order,
    it.item_subcategory,
    it.item_brand,
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
    
    -- Velocity Classification
    CASE 
        WHEN v.avg_weekly_transactions_4w >= 10 THEN 'Fast Moving'
        WHEN v.avg_weekly_transactions_4w >= 2 THEN 'Regular Moving'
        WHEN v.avg_weekly_transactions_4w >= 0.5 THEN 'Slow Moving'
        ELSE 'Non Moving'
    END as velocity_classification,
    
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
    -- Metadata
    -- =====================================================
    CURRENT_DATE() as dim_created_date,
    CURRENT_DATETIME() as dim_last_updated_at

FROM {{ ref('int_items') }} as it
LEFT JOIN item_metrics as m ON it.item_no_ = m.item_no_
LEFT JOIN item_abc_classification as abc ON it.item_no_ = abc.item_no_
LEFT JOIN item_velocity as v ON it.item_no_ = v.item_no_

-- Optional: Filter out items with no sales history if needed
-- WHERE m.total_transactions > 0 OR m.first_sale_date IS NOT NULL