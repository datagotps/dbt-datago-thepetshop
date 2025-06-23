SELECT 
    -- ORDER IDENTIFICATION COLUMNS
    source_no_,
    unified_order_id,
    document_no_,
    web_order_id,
    
    -- ORDER CORE DATA & REVENUE METRICS
    order_date,
    order_value,                    -- Sales Invoice amounts only (NEW)
    refund_amount,                  -- Credit Memo amounts only (NEW)
    sales_amount__actual_,          -- Original raw amount
    document_type_2,                -- 'Sales Invoice' or 'Sales Credit Memo' (NEW)
    transaction_type,               -- 'Sale', 'Refund', or 'Other' (NEW)
    
    -- CHANNEL & ORDER DETAILS
    sales_channel,                  -- 'Online' or 'Shop'
    store_location,                 -- Physical store location
    platform,                      -- 'website', 'Android', 'iOS', 'CRM', 'Unmapped'
    order_type,                     -- 'EXPRESS', 'NORMAL', 'EXCHANGE'
    paymentgateway,                 -- Payment gateway details
    paymentmethodcode,              -- 'PREPAID', 'COD', 'creditCard'
    
    -- CUSTOMER INFORMATION
    customer_name,
    raw_phone_no_,
    customer_identity_status,
    customer_acquisition_date,
    
    -- TIME DIMENSIONS
    order_month,                    -- Truncated to month
    order_week,                     -- Truncated to week
    order_year,                     -- Extracted year
    order_month_num,                -- Month number (1-12)
    year_month,                     -- 'YYYY-MM' format
    
    -- HYPERLOCAL ANALYSIS DIMENSIONS
    hyperlocal_period,              -- 'Pre-Launch' or 'Post-Launch'
    delivery_service_type,          -- '60-Min Hyperlocal', '4-Hour Express', 'Standard Delivery', 'Exchange Order', 'Refund Transaction'
    service_tier,                   -- 'Express Service', 'Standard Service', 'Refund Transaction'
    hyperlocal_order_flag,          -- 'Hyperlocal Order', 'Non-Hyperlocal Order', 'Refund Transaction'
    days_since_hyperlocal_launch,   -- Days since 2025-01-16
    
    -- CUSTOMER SEGMENTATION
    hyperlocal_customer_segment,           -- 'New Post-Hyperlocal', 'Existing Pre-Hyperlocal'
    hyperlocal_usage_flag,                 -- 'Used Hyperlocal', 'Never Used Hyperlocal'
    hyperlocal_customer_detailed_segment,  -- 'Post-HL Acq + HL User', 'Pre-HL Acq + HL User', etc.
    hyperlocal_customer_detailed_segment_order, -- 1, 2, 3, 4, 5 (for sorting)
    
    -- ADDITIONAL CLASSIFICATIONS
    order_channel,                  -- 'Online', 'Store', 'Unknown'
    payment_category,               -- 'Cash/COD', 'Card Payment', 'BNPL (Tabby)', 'Loyalty/Points', 'Other Payment'
    order_size_category,            -- 'Large Order (500+ AED)', 'Medium Order (200-499 AED)', 'Small Order (100-199 AED)', 'Micro Order (<100 AED)', 'Refund Transaction'
    customer_tenure_days_at_order,  -- Days between customer acquisition and this order
    customer_lifecycle_at_order,     -- 'New Customer Order', 'Returning Customer Order', 'Customer Refund'

    -- REPORT METADATA
    CURRENT_DATETIME() AS report_last_updated_at, 


    -- NEW: Retention analytics columns
    customer_order_sequence,            -- Sequential order number for this customer (1, 2, 3...)
    channel_order_sequence,             -- Order sequence within specific channel (Online/Shop)
    previous_order_date,                -- Date of customer's previous order
    previous_channel_order_date,        -- Date of last order in same channel
    total_lifetime_orders,              -- Total orders placed by customer across lifetime
    total_channel_orders,               -- Total orders in each specific channel
    days_since_last_order,              -- Days elapsed since previous order
    days_since_last_channel_order,      -- Days since last order in same channel
    recency_cohort,                     -- Time-based return classification (New/M1-M6/Dormant)
    customer_engagement_status,          -- Engagement level (New/Active/At Risk/Lapsed/Reactivated)
    customer_engagement_status_sort,
    new_vs_returning,                   -- Simple binary flag (New/Returning)
    is_test_customer,                   -- Flag for accounts with 200+ orders (likely test)
    acquisition_channel,                -- Channel where customer first purchased (Online/Shop)
    acquisition_store,                  -- Store location of first purchase
    acquisition_platform,               -- Platform of first purchase (Website/iOS/Android/CRM)
    acquisition_payment_method,         -- Payment method used in first order
    channels_used_count,                -- Number of different channels customer has used
    stores_visited_count,               -- Number of different stores customer has shopped
    channel_preference_type,             -- Customer classification (Omnichannel/Online-Only/Store-Only)

    acquisition_quarter,        -- Acquisition quarter for quarterly cohorts
    acquisition_month,          -- Acquisition month for monthly cohorts  
    acquisition_year,           -- Acquisition year for yearly cohorts
    acquisition_quarter_sort,   -- For proper sorting in Power BI
    acquisition_month_sort,      -- For proper sorting in Power BI

    cohort_month_number,            -- Month-level cohort position: 0 = same month as acquisition, 1 = next month, 2 = two months later, etc.
    cohort_month_label,             -- String: "Month 0", "Month 1"... for display
    weeks_since_acquisition,        -- For weekly cohort analysis
    acquisition_week,               -- For weekly cohorts
    is_acquisition_month,           -- Boolean flag for Month 0
    cohort_age_bucket,              -- For simplified views

    acquisition_month_date,

    transaction_frequency_segment,      -- 'Transaction_1', 'Transaction_2', etc.
    transaction_frequency_sort_order,   -- 1, 2, 3, 4, 5, 6, 7, 8, 9
    customer_loyalty_tier,              -- 'One-Time Buyer', 'Early Repeat', etc.





FROM {{ ref('int_commercial_model') }}