{{ config(
    materialized='table',
    description='Customer master analysis with RFM segmentation - Rebuilt from int_orders with unified_customer_id and multiple source tracking'
) }}

-- =====================================================
-- STEP 1: Identify customers with multiple source_no_
-- =====================================================
WITH customer_sources AS (
    SELECT 
        unified_customer_id,
        source_no_,
        MIN(order_date) AS first_order_date_for_source
    FROM {{ ref('int_orders') }}
    WHERE unified_customer_id IS NOT NULL
    GROUP BY unified_customer_id, source_no_
),

customer_source_aggregates AS (
    SELECT 
        unified_customer_id,
        COUNT(DISTINCT source_no_) AS source_count,
        STRING_AGG(DISTINCT source_no_, ' | ' ORDER BY source_no_) AS all_source_nos
    FROM customer_sources
    GROUP BY unified_customer_id
),

customer_source_mapping AS (
    SELECT 
        cs.unified_customer_id,
        cs.source_no_,
        csa.source_count,
        csa.all_source_nos,
        ROW_NUMBER() OVER (PARTITION BY cs.unified_customer_id ORDER BY cs.first_order_date_for_source) AS rn
    FROM customer_sources cs
    JOIN customer_source_aggregates csa ON cs.unified_customer_id = csa.unified_customer_id
),

primary_source_mapping AS (
    SELECT 
        unified_customer_id,
        source_no_ AS primary_source_no,
        all_source_nos,
        source_count,
        CASE WHEN source_count > 1 THEN 'Yes' ELSE 'No' END AS has_multiple_sources
    FROM customer_source_mapping
    WHERE rn = 1
),

-- =====================================================
-- STEP 2: Customer Base Metrics from int_orders
-- =====================================================
customer_base_metrics AS (
    SELECT 
        o.unified_customer_id,
        psm.primary_source_no AS source_no_,
        psm.all_source_nos,
        psm.source_count AS duplicate_customer_count,
        psm.has_multiple_sources AS duplicate_flag,
        
        -- Customer identification
        MAX(o.customer_name) AS customer_name,
        MAX(o.std_phone_no_) AS std_phone_no_,
        MAX(o.customer_identity_status) AS customer_identity_status,
        
        -- Placeholder for loyalty
        MAX(o.loyality_member_id ) AS loyality_member_id,
        MAX(o.raw_phone_no_) AS raw_phone_no_,
        
        -- Active months count
        COUNT(DISTINCT DATE_TRUNC(o.order_date, MONTH)) AS active_months_count,
        
        -- Date metrics
        MIN(o.order_date) AS customer_acquisition_date,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        
        -- Channel distribution lists (using subquery to handle DISTINCT properly)
        (SELECT STRING_AGG(store, ' | ' ORDER BY store) 
         FROM (SELECT DISTINCT o2.store_location AS store 
               FROM {{ ref('int_orders') }} o2 
               WHERE o2.unified_customer_id = o.unified_customer_id 
                 AND o2.sales_channel = 'Shop' 
                 AND o2.store_location IS NOT NULL)
        ) AS stores_used,
        
        (SELECT STRING_AGG(platform, ' | ' ORDER BY platform) 
         FROM (SELECT DISTINCT o2.platform AS platform 
               FROM {{ ref('int_orders') }} o2 
               WHERE o2.unified_customer_id = o.unified_customer_id 
                 AND o2.sales_channel = 'Online' 
                 AND o2.platform IS NOT NULL)
        ) AS platforms_used,
        
        -- Order counts
        COUNT(DISTINCT o.unified_order_id) AS total_order_count,
        COUNT(DISTINCT CASE WHEN o.sales_channel = 'Online' THEN o.unified_order_id END) AS online_order_count,
        COUNT(DISTINCT CASE WHEN o.sales_channel = 'Shop' THEN o.unified_order_id END) AS offline_order_count,
        
        -- Sales values
        SUM(CASE WHEN o.transaction_type = 'Sale' THEN o.order_value ELSE 0 END) AS total_sales_value,
        SUM(CASE WHEN o.transaction_type = 'Sale' AND o.sales_channel = 'Online' THEN o.order_value ELSE 0 END) AS online_sales_value,
        SUM(CASE WHEN o.transaction_type = 'Sale' AND o.sales_channel = 'Shop' THEN o.order_value ELSE 0 END) AS offline_sales_value,
        
        -- YTD Sales
        SUM(CASE 
            WHEN o.transaction_type = 'Sale'
                AND o.order_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
                AND o.order_date <= CURRENT_DATE()
            THEN o.order_value 
            ELSE 0 
        END) AS ytd_sales,
        
        -- MTD Sales
        SUM(CASE 
            WHEN o.transaction_type = 'Sale'
                AND o.order_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
                AND o.order_date <= CURRENT_DATE()
            THEN o.order_value 
            ELSE 0 
        END) AS mtd_sales,
        
        -- Hyperlocal metrics
        COUNT(DISTINCT CASE 
            WHEN o.order_date < '2025-01-16' AND o.transaction_type = 'Sale'
            THEN o.unified_order_id 
        END) AS pre_hyperlocal_orders,
        
        SUM(CASE 
            WHEN o.order_date < '2025-01-16' AND o.transaction_type = 'Sale'
            THEN o.order_value 
            ELSE 0 
        END) AS pre_hyperlocal_revenue,
        
        COUNT(DISTINCT CASE 
            WHEN o.order_date >= '2025-01-16' AND o.transaction_type = 'Sale'
            THEN o.unified_order_id 
        END) AS post_hyperlocal_orders,
        
        SUM(CASE 
            WHEN o.order_date >= '2025-01-16' AND o.transaction_type = 'Sale'
            THEN o.order_value 
            ELSE 0 
        END) AS post_hyperlocal_revenue,
        
        COUNT(DISTINCT CASE 
            WHEN o.delivery_service_type = '60-Min Hyperlocal'
            THEN o.unified_order_id 
        END) AS hyperlocal_60min_orders,
        
        SUM(CASE 
            WHEN o.delivery_service_type = '60-Min Hyperlocal'
            THEN o.order_value 
            ELSE 0 
        END) AS hyperlocal_60min_revenue,
        
        COUNT(DISTINCT CASE 
            WHEN o.delivery_service_type = '4-Hour Express'
            THEN o.unified_order_id 
        END) AS express_4hour_orders,
        
        SUM(CASE 
            WHEN o.delivery_service_type = '4-Hour Express'
            THEN o.order_value 
            ELSE 0 
        END) AS express_4hour_revenue
        
    FROM {{ ref('int_orders') }} o
    LEFT JOIN primary_source_mapping psm ON o.unified_customer_id = psm.unified_customer_id
    WHERE o.unified_customer_id IS NOT NULL
    GROUP BY 
        o.unified_customer_id,
        psm.primary_source_no,
        psm.all_source_nos,
        psm.source_count,
        psm.has_multiple_sources
),

-- =====================================================
-- STEP 3: Order Lists and Acquisition Details
-- =====================================================
customer_order_lists AS (
    SELECT 
        unified_customer_id,
        STRING_AGG(unified_order_id, ' | ' ORDER BY order_date ASC) AS document_ids_list,
        STRING_AGG(
            CASE WHEN sales_channel = 'Online' THEN unified_order_id END, 
            ' | ' ORDER BY CASE WHEN sales_channel = 'Online' THEN order_date END ASC
        ) AS online_order_ids,
        STRING_AGG(
            CASE WHEN sales_channel = 'Shop' THEN unified_order_id END, 
            ' | ' ORDER BY CASE WHEN sales_channel = 'Shop' THEN order_date END ASC
        ) AS offline_order_ids
    FROM {{ ref('int_orders') }}
    WHERE unified_customer_id IS NOT NULL
        AND transaction_type = 'Sale'
    GROUP BY unified_customer_id
),

customer_acquisition_details AS (
    SELECT DISTINCT
        unified_customer_id,
        FIRST_VALUE(acquisition_store) OVER (PARTITION BY unified_customer_id ORDER BY order_date) AS first_acquisition_store,
        FIRST_VALUE(acquisition_platform) OVER (PARTITION BY unified_customer_id ORDER BY order_date) AS first_acquisition_platform,
        FIRST_VALUE(acquisition_channel) OVER (PARTITION BY unified_customer_id ORDER BY order_date) AS customer_acquisition_channel,
        FIRST_VALUE(paymentgateway) OVER (PARTITION BY unified_customer_id ORDER BY order_date) AS first_acquisition_paymentgateway,
        FIRST_VALUE(order_type) OVER (PARTITION BY unified_customer_id ORDER BY order_date) AS first_acquisition_order_type,
        COALESCE(
            FIRST_VALUE(acquisition_platform) OVER (PARTITION BY unified_customer_id ORDER BY order_date),
            FIRST_VALUE(acquisition_store) OVER (PARTITION BY unified_customer_id ORDER BY order_date),
            'Unknown'
        ) AS customer_acquisition_channel_detail
    FROM {{ ref('int_orders') }}
    WHERE customer_order_sequence = 1
        AND unified_customer_id IS NOT NULL
),

-- =====================================================
-- STEP 3B: Customer Offer/Discount Usage
-- =====================================================
customer_offer_usage AS (
    SELECT 
        ol.unified_customer_id,
        
        -- Count orders with discounts/offers
        COUNT(DISTINCT CASE 
            WHEN ol.has_discount = 1 
            THEN ol.unified_order_id 
        END) AS orders_with_discount_count,
        
        -- Total discount amount
        SUM(CASE 
            WHEN ol.transaction_type = 'Sale' 
            THEN ABS(COALESCE(ol.discount_amount, 0))
            ELSE 0
        END) AS total_discount_amount,
        
        -- Count distinct offers used
        COUNT(DISTINCT CASE 
            WHEN ol.online_offer_no_ IS NOT NULL AND ol.online_offer_no_ != '' 
            THEN ol.online_offer_no_
        END) + COUNT(DISTINCT CASE 
            WHEN ol.offline_offer_no_ IS NOT NULL AND ol.offline_offer_no_ != '' 
            THEN ol.offline_offer_no_
        END) AS distinct_offers_used
        
    FROM {{ ref('int_order_lines') }} ol
    WHERE ol.unified_customer_id IS NOT NULL
        AND ol.transaction_type = 'Sale'
    GROUP BY ol.unified_customer_id
),

-- =====================================================
-- STEP 3C: Customer Pet Ownership Analysis
-- =====================================================
customer_pet_ownership AS (
    SELECT 
        ol.unified_customer_id,
        
        -- Revenue by pet division
        SUM(CASE WHEN ol.division = 'DOG' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS dog_revenue,
        SUM(CASE WHEN ol.division = 'CAT' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS cat_revenue,
        SUM(CASE WHEN ol.division = 'FISH' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS fish_revenue,
        SUM(CASE WHEN ol.division = 'BIRD' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS bird_revenue,
        SUM(CASE WHEN ol.division = 'SMALL PET' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS small_pet_revenue,
        SUM(CASE WHEN ol.division = 'REPTILE' AND ol.transaction_type = 'Sale' THEN ol.sales_amount__actual_ ELSE 0 END) AS reptile_revenue,
        
        -- Order counts by pet division
        COUNT(DISTINCT CASE WHEN ol.division = 'DOG' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS dog_orders,
        COUNT(DISTINCT CASE WHEN ol.division = 'CAT' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS cat_orders,
        COUNT(DISTINCT CASE WHEN ol.division = 'FISH' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS fish_orders,
        COUNT(DISTINCT CASE WHEN ol.division = 'BIRD' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS bird_orders,
        COUNT(DISTINCT CASE WHEN ol.division = 'SMALL PET' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS small_pet_orders,
        COUNT(DISTINCT CASE WHEN ol.division = 'REPTILE' AND ol.transaction_type = 'Sale' THEN ol.unified_order_id END) AS reptile_orders,
        
        -- Pet ownership flags (has purchased at least once)
        CASE WHEN SUM(CASE WHEN ol.division = 'DOG' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_dog_owner,
        CASE WHEN SUM(CASE WHEN ol.division = 'CAT' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_cat_owner,
        CASE WHEN SUM(CASE WHEN ol.division = 'FISH' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_fish_owner,
        CASE WHEN SUM(CASE WHEN ol.division = 'BIRD' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_bird_owner,
        CASE WHEN SUM(CASE WHEN ol.division = 'SMALL PET' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_small_pet_owner,
        CASE WHEN SUM(CASE WHEN ol.division = 'REPTILE' AND ol.transaction_type = 'Sale' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_reptile_owner
        
    FROM {{ ref('int_order_lines') }} ol
    WHERE ol.unified_customer_id IS NOT NULL
        AND ol.transaction_type = 'Sale'
        AND ol.division IN ('DOG', 'CAT', 'FISH', 'BIRD', 'SMALL PET', 'REPTILE')
    GROUP BY ol.unified_customer_id
),

-- =====================================================
-- STEP 4: Join base metrics with lists and acquisition
-- =====================================================
customer_combined AS (
    SELECT 
        cbm.unified_customer_id,
        cbm.source_no_,
        cbm.all_source_nos,
        cbm.duplicate_customer_count,
        cbm.duplicate_flag,
        cbm.customer_name,
        cbm.std_phone_no_,
        cbm.customer_identity_status,
        cbm.loyality_member_id,
        cbm.raw_phone_no_,
        cbm.active_months_count,
        cbm.customer_acquisition_date,
        cbm.first_order_date,
        cbm.last_order_date,
        cbm.stores_used,
        cbm.platforms_used,
        cbm.total_order_count,
        cbm.online_order_count,
        cbm.offline_order_count,
        cbm.total_sales_value,
        cbm.online_sales_value,
        cbm.offline_sales_value,
        cbm.ytd_sales,
        cbm.mtd_sales,
        cbm.pre_hyperlocal_orders,
        cbm.pre_hyperlocal_revenue,
        cbm.post_hyperlocal_orders,
        cbm.post_hyperlocal_revenue,
        cbm.hyperlocal_60min_orders,
        cbm.hyperlocal_60min_revenue,
        cbm.express_4hour_orders,
        cbm.express_4hour_revenue,
        col.document_ids_list,
        col.online_order_ids,
        col.offline_order_ids,
        cad.first_acquisition_store,
        cad.first_acquisition_platform,
        cad.customer_acquisition_channel,
        cad.first_acquisition_paymentgateway,
        cad.first_acquisition_order_type,
        cad.customer_acquisition_channel_detail,
        
        -- Offer/Discount metrics
        COALESCE(cou.orders_with_discount_count, 0) AS orders_with_discount_count,
        COALESCE(cou.total_discount_amount, 0) AS total_discount_amount,
        COALESCE(cou.distinct_offers_used, 0) AS distinct_offers_used,
        
        -- Pet Ownership metrics
        COALESCE(cpo.dog_revenue, 0) AS dog_revenue,
        COALESCE(cpo.cat_revenue, 0) AS cat_revenue,
        COALESCE(cpo.fish_revenue, 0) AS fish_revenue,
        COALESCE(cpo.bird_revenue, 0) AS bird_revenue,
        COALESCE(cpo.small_pet_revenue, 0) AS small_pet_revenue,
        COALESCE(cpo.reptile_revenue, 0) AS reptile_revenue,
        COALESCE(cpo.dog_orders, 0) AS dog_orders,
        COALESCE(cpo.cat_orders, 0) AS cat_orders,
        COALESCE(cpo.fish_orders, 0) AS fish_orders,
        COALESCE(cpo.bird_orders, 0) AS bird_orders,
        COALESCE(cpo.small_pet_orders, 0) AS small_pet_orders,
        COALESCE(cpo.reptile_orders, 0) AS reptile_orders,
        COALESCE(cpo.is_dog_owner, 0) AS is_dog_owner,
        COALESCE(cpo.is_cat_owner, 0) AS is_cat_owner,
        COALESCE(cpo.is_fish_owner, 0) AS is_fish_owner,
        COALESCE(cpo.is_bird_owner, 0) AS is_bird_owner,
        COALESCE(cpo.is_small_pet_owner, 0) AS is_small_pet_owner,
        COALESCE(cpo.is_reptile_owner, 0) AS is_reptile_owner
        
    FROM customer_base_metrics cbm
    LEFT JOIN customer_order_lists col ON cbm.unified_customer_id = col.unified_customer_id
    LEFT JOIN customer_acquisition_details cad ON cbm.unified_customer_id = cad.unified_customer_id
    LEFT JOIN customer_offer_usage cou ON cbm.unified_customer_id = cou.unified_customer_id
    LEFT JOIN customer_pet_ownership cpo ON cbm.unified_customer_id = cpo.unified_customer_id
),

-- =====================================================
-- STEP 5: Calculate Derived Metrics and Segments
-- =====================================================
customer_calculated_metrics AS (
    SELECT *,
        -- Loyalty status
        CASE 
            WHEN loyality_member_id IS NOT NULL AND loyality_member_id != '' 
            THEN 'Enrolled'
            ELSE 'Not Enrolled'
        END AS loyalty_enrollment_status,
        
        -- Core RFM metrics
        DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days,
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) AS customer_tenure_days,
        total_order_count AS frequency_orders,
        total_sales_value AS monetary_total_value,
        ROUND(total_sales_value / NULLIF(total_order_count, 0), 2) AS monetary_avg_order_value,
        ROUND(total_sales_value / NULLIF(active_months_count, 0), 2) AS avg_monthly_demand,
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, MONTH) AS months_since_acquisition,
        
        -- Hyperlocal segmentation
        CASE 
            WHEN customer_acquisition_date >= '2025-01-16' THEN 'Acquired Post-Launch'
            WHEN customer_acquisition_date < '2025-01-16' THEN 'Acquired Pre-Launch'
            ELSE 'Unknown'
        END AS hyperlocal_customer_segment,
        
        -- M1 Retention Segment
        CASE 
            WHEN DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 
                                               EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 1)
                AND DATE(last_order_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
                AND DATE(last_order_date) < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
                AND CURRENT_DATE() >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 21)
            THEN 'M1 Retention Target'
            ELSE 'Not M1 Target'
        END AS m1_retention_segment,
        
        -- Transaction flags
        CASE 
            WHEN DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 
                                               EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), 1)
                AND DATE(last_order_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
            THEN 'Yes' ELSE 'No'
        END AS transacted_last_month,
        
        CASE 
            WHEN DATE(last_order_date) >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
                AND DATE(last_order_date) <= CURRENT_DATE()
            THEN 'Yes' ELSE 'No'
        END AS transacted_current_month,
        
        -- Hyperlocal usage flags
        CASE 
            WHEN hyperlocal_60min_orders > 0 THEN 'Used Hyperlocal'
            ELSE 'Never Used Hyperlocal'
        END AS hyperlocal_usage_flag,
        
        CASE 
            WHEN hyperlocal_60min_orders > 0 AND express_4hour_orders > 0 THEN 'Both Express Types'
            WHEN hyperlocal_60min_orders > 0 AND express_4hour_orders = 0 THEN '60-Min Only'
            WHEN hyperlocal_60min_orders = 0 AND express_4hour_orders > 0 THEN '4-Hour Only'
            ELSE 'Standard Delivery Only'
        END AS delivery_service_preference,
        
        -- Hyperlocal detailed segment
        CASE 
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders > 0 
            THEN 'Post-HL Acq + HL User'
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders > 0 
            THEN 'Pre-HL Acq + HL User'
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders = 0 
            THEN 'Post-HL Acq + Non-HL User'
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders = 0 
            THEN 'Pre-HL Acq + Non-HL User'
            ELSE 'UNCLASSIFIED'
        END AS hyperlocal_customer_detailed_segment,
        
        CASE 
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 1
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders > 0 THEN 2
            WHEN customer_acquisition_date >= '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 3
            WHEN customer_acquisition_date < '2025-01-16' AND hyperlocal_60min_orders = 0 THEN 4
            ELSE 5
        END AS hyperlocal_customer_detailed_segment_order,
        
        -- Purchase frequency bucket
        CASE 
            WHEN total_order_count <= 1 THEN '1 Order'
            WHEN total_order_count BETWEEN 2 AND 3 THEN '2-3 Orders'
            WHEN total_order_count BETWEEN 4 AND 6 THEN '4-6 Orders'
            WHEN total_order_count BETWEEN 7 AND 10 THEN '7-10 Orders'
            WHEN total_order_count >= 11 THEN '11+ Orders'
            ELSE 'Unknown'
        END AS purchase_frequency_bucket,
        
        CASE 
            WHEN total_order_count <= 1 THEN 1
            WHEN total_order_count BETWEEN 2 AND 3 THEN 2
            WHEN total_order_count BETWEEN 4 AND 6 THEN 3
            WHEN total_order_count BETWEEN 7 AND 10 THEN 4
            WHEN total_order_count >= 11 THEN 5
            ELSE 6
        END AS purchase_frequency_bucket_order,
        
        -- Customer recency segment
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 31 AND 60 THEN 'Recent'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 61 AND 90 THEN 'At Risk'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 91 AND 180 THEN 'Churn'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 181 AND 365 THEN 'Inactive'
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) > 365 THEN 'Lost'
            ELSE 'Unclassified'
        END AS customer_recency_segment,
        
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 31 AND 60 THEN 2
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 61 AND 90 THEN 3
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 91 AND 180 THEN 4
            WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) BETWEEN 181 AND 365 THEN 5
            ELSE 6
        END AS customer_recency_segment_order,
        
        -- Customer tenure segment
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 0 AND 29 THEN '1 Month'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 30 AND 89 THEN '3 Months'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 90 AND 179 THEN '6 Months'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 180 AND 364 THEN '1 Year'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 365 AND 729 THEN '2 Years'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 730 AND 1094 THEN '3 Years'
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) >= 1095 THEN '4+ Years'
            ELSE 'Unknown'
        END AS customer_tenure_segment,
        
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 0 AND 29 THEN 1
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 30 AND 89 THEN 2
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 90 AND 179 THEN 3
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 180 AND 364 THEN 4
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 365 AND 729 THEN 5
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) BETWEEN 730 AND 1094 THEN 6
            WHEN DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) >= 1095 THEN 7
            ELSE 8
        END AS customer_tenure_segment_order,
        
        -- Customer type
        CASE 
            WHEN total_order_count <= 1 AND customer_acquisition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'New'
            WHEN total_order_count > 1 THEN 'Repeat'
            WHEN total_order_count <= 1 AND customer_acquisition_date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'One-Time'
            ELSE 'Unknown'
        END AS customer_type,
        
        -- Customer channel distribution
        CASE 
            WHEN online_order_count > 0 AND offline_order_count > 0 THEN 'Hybrid'
            WHEN online_order_count > 0 AND offline_order_count = 0 THEN 'Online'
            WHEN online_order_count = 0 AND offline_order_count > 0 THEN 'Shop'
            ELSE 'Unknown'
        END AS customer_channel_distribution,
        
        -- Acquisition cohort
        CASE 
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1) 
            THEN 'MTD'
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
                AND customer_acquisition_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN CONCAT(FORMAT_DATE('%b', customer_acquisition_date), ' ', RIGHT(CAST(EXTRACT(YEAR FROM customer_acquisition_date) AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 2 AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 3 AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 4 AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5
            THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 5 AS STRING), 2))
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6
            THEN 'Year 19 & Before'
            ELSE 'Unknown'
        END AS acquisition_cohort,
        
        CASE 
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN 1000
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
                AND customer_acquisition_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN 900 + EXTRACT(MONTH FROM customer_acquisition_date)
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN 800
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2 THEN 700
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3 THEN 600
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4 THEN 500
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5 THEN 400
            WHEN EXTRACT(YEAR FROM customer_acquisition_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6 THEN 100
            ELSE 1
        END AS acquisition_cohort_rank,
        
        -- Offer Seeker Segment
        CASE 
            WHEN orders_with_discount_count >= 3 THEN 'Offer Seeker'
            WHEN orders_with_discount_count BETWEEN 1 AND 2 THEN 'Occasional Offer User'
            ELSE 'Non-Offer User'
        END AS offer_seeker_segment,
        
        CASE 
            WHEN orders_with_discount_count >= 3 THEN 1
            WHEN orders_with_discount_count BETWEEN 1 AND 2 THEN 2
            ELSE 3
        END AS offer_seeker_segment_order,
        
        -- Discount Affinity Metrics
        ROUND(
            CASE 
                WHEN total_order_count > 0 
                THEN (orders_with_discount_count * 100.0) / total_order_count
                ELSE 0 
            END, 2
        ) AS discount_usage_rate_pct,
        
        ROUND(
            CASE 
                WHEN total_sales_value > 0 
                THEN (total_discount_amount * 100.0) / total_sales_value
                ELSE 0 
            END, 2
        ) AS discount_dependency_pct,
        
        -- Pet Ownership Categorization
        -- Count how many pet types the customer owns
        (is_dog_owner + is_cat_owner + is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) AS pet_types_count,
        
        -- Primary pet type (based on highest revenue)
        CASE 
            WHEN dog_revenue >= cat_revenue AND dog_revenue >= fish_revenue AND dog_revenue >= bird_revenue 
                AND dog_revenue >= small_pet_revenue AND dog_revenue >= reptile_revenue AND dog_revenue > 0 
            THEN 'Dog'
            WHEN cat_revenue >= dog_revenue AND cat_revenue >= fish_revenue AND cat_revenue >= bird_revenue 
                AND cat_revenue >= small_pet_revenue AND cat_revenue >= reptile_revenue AND cat_revenue > 0 
            THEN 'Cat'
            WHEN fish_revenue >= dog_revenue AND fish_revenue >= cat_revenue AND fish_revenue >= bird_revenue 
                AND fish_revenue >= small_pet_revenue AND fish_revenue >= reptile_revenue AND fish_revenue > 0 
            THEN 'Fish'
            WHEN bird_revenue >= dog_revenue AND bird_revenue >= cat_revenue AND bird_revenue >= fish_revenue 
                AND bird_revenue >= small_pet_revenue AND bird_revenue >= reptile_revenue AND bird_revenue > 0 
            THEN 'Bird'
            WHEN small_pet_revenue >= dog_revenue AND small_pet_revenue >= cat_revenue AND small_pet_revenue >= fish_revenue 
                AND small_pet_revenue >= bird_revenue AND small_pet_revenue >= reptile_revenue AND small_pet_revenue > 0 
            THEN 'Small Pet'
            WHEN reptile_revenue >= dog_revenue AND reptile_revenue >= cat_revenue AND reptile_revenue >= fish_revenue 
                AND reptile_revenue >= bird_revenue AND reptile_revenue >= small_pet_revenue AND reptile_revenue > 0 
            THEN 'Reptile'
            ELSE 'No Pet Products'
        END AS primary_pet_type,
        
        -- Customer pet owner profile
        CASE 
            WHEN (is_dog_owner + is_cat_owner + is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) = 0 
            THEN 'No Pet Products'
            WHEN (is_dog_owner + is_cat_owner + is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) = 1 
            THEN CASE 
                WHEN is_dog_owner = 1 THEN 'Dog Owner'
                WHEN is_cat_owner = 1 THEN 'Cat Owner'
                WHEN is_fish_owner = 1 THEN 'Fish Owner'
                WHEN is_bird_owner = 1 THEN 'Bird Owner'
                WHEN is_small_pet_owner = 1 THEN 'Small Pet Owner'
                WHEN is_reptile_owner = 1 THEN 'Reptile Owner'
            END
            WHEN (is_dog_owner + is_cat_owner + is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) >= 2 
            THEN 'Multi-Pet Owner'
            ELSE 'Unknown'
        END AS pet_owner_profile,
        
        -- Detailed multi-pet profile (for multi-pet owners)
        CASE 
            WHEN is_dog_owner = 1 AND is_cat_owner = 1 AND (is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) = 0 
            THEN 'Dog + Cat Owner'
            WHEN is_dog_owner = 1 AND is_cat_owner = 1 AND (is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) > 0 
            THEN 'Dog + Cat + Others'
            WHEN (is_dog_owner + is_cat_owner + is_fish_owner + is_bird_owner + is_small_pet_owner + is_reptile_owner) >= 3 
            THEN 'Multi-Pet Owner (3+)'
            ELSE NULL
        END AS multi_pet_detail
        
    FROM customer_combined
),

-- =====================================================
-- STEP 5A: Discount Affinity Scoring
-- =====================================================
customer_discount_scoring AS (
    SELECT *,
        -- Create composite discount affinity score (0-100)
        ROUND(
            (discount_usage_rate_pct * 0.5) +  -- 50% weight on usage frequency
            (discount_dependency_pct * 0.3) +   -- 30% weight on discount dependency
            (LEAST(distinct_offers_used * 5, 20)) -- 20% weight on variety (capped at 20)
        , 2) AS discount_affinity_score,
        
        -- Calculate percentile for distribution-based thresholds
        PERCENT_RANK() OVER (ORDER BY 
            (discount_usage_rate_pct * 0.5) + 
            (discount_dependency_pct * 0.3) + 
            (LEAST(distinct_offers_used * 5, 20))
        ) AS discount_affinity_percentile
        
    FROM customer_calculated_metrics
),

-- =====================================================
-- STEP 5B: Discount Affinity Segmentation
-- =====================================================
customer_discount_segments AS (
    SELECT *,
        -- Discount Affinity Dimension
        CASE 
            WHEN discount_affinity_percentile >= 0.70 THEN 'High Discount Affinity'
            WHEN discount_affinity_percentile >= 0.30 THEN 'Medium Discount Affinity'
            WHEN discount_affinity_percentile > 0 THEN 'Low Discount Affinity'
            ELSE 'No Discount Usage'
        END AS discount_affinity_segment,
        
        CASE 
            WHEN discount_affinity_percentile >= 0.70 THEN 1
            WHEN discount_affinity_percentile >= 0.30 THEN 2
            WHEN discount_affinity_percentile > 0 THEN 3
            ELSE 4
        END AS discount_affinity_segment_order
        
    FROM customer_discount_scoring
),

-- =====================================================
-- STEP 6: RFM Scoring
-- =====================================================
customer_rfm_scores AS (
    SELECT *,
        -- R Score: Recency (business logic based)
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END AS r_score,
        
        -- F Score: Frequency (percentile based)
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.8 THEN 5
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.6 THEN 4
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.4 THEN 3
            WHEN PERCENT_RANK() OVER (ORDER BY frequency_orders) >= 0.2 THEN 2
            ELSE 1
        END AS f_score,
        
        -- M Score: Monetary (percentile based)
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.8 THEN 5
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.6 THEN 4
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.4 THEN 3
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value) >= 0.2 THEN 2
            ELSE 1
        END AS m_score
        
    FROM customer_discount_segments
),

-- =====================================================
-- STEP 7: Order Pattern Analysis
-- =====================================================
customer_order_patterns AS (
    SELECT 
        unified_customer_id,
        AVG(days_since_last_order) AS avg_days_between_orders,
        STDDEV(days_since_last_order) AS stddev_days_between_orders,
        MAX(days_since_last_order) AS max_days_between_orders
    FROM {{ ref('int_orders') }}
    WHERE days_since_last_order IS NOT NULL
        AND unified_customer_id IS NOT NULL
    GROUP BY unified_customer_id
),

-- =====================================================
-- STEP 8: Final Customer Segments
-- =====================================================
customer_segments AS (
    SELECT 
        -- Keep unified_customer_id as primary identifier
        cs.unified_customer_id,
        cs.source_no_,
        cs.customer_name,
        cs.std_phone_no_,
        cs.customer_identity_status,
        cs.duplicate_flag,
        cs.raw_phone_no_,
        cs.loyality_member_id,
        cs.active_months_count,
        cs.loyalty_enrollment_status,
        cs.customer_acquisition_date,
        cs.first_order_date,
        cs.last_order_date,
        cs.stores_used,
        cs.platforms_used,
        cs.total_order_count,
        cs.online_order_count,
        cs.offline_order_count,
        cs.total_sales_value,
        cs.online_sales_value,
        cs.offline_sales_value,
        cs.ytd_sales,
        cs.mtd_sales,
        cs.pre_hyperlocal_orders,
        cs.pre_hyperlocal_revenue,
        cs.post_hyperlocal_orders,
        cs.post_hyperlocal_revenue,
        cs.hyperlocal_60min_orders,
        cs.hyperlocal_60min_revenue,
        cs.express_4hour_orders,
        cs.express_4hour_revenue,
        cs.document_ids_list,
        cs.online_order_ids,
        cs.offline_order_ids,
        cs.first_acquisition_store,
        cs.first_acquisition_platform,
        cs.customer_acquisition_channel,
        cs.first_acquisition_paymentgateway,
        cs.first_acquisition_order_type,
        cs.customer_acquisition_channel_detail,
        cs.recency_days,
        cs.customer_tenure_days,
        cs.frequency_orders,
        cs.monetary_total_value,
        cs.monetary_avg_order_value,
        cs.avg_monthly_demand,
        cs.months_since_acquisition,
        cs.hyperlocal_customer_segment,
        cs.m1_retention_segment,
        cs.transacted_last_month,
        cs.transacted_current_month,
        cs.hyperlocal_usage_flag,
        cs.delivery_service_preference,
        cs.hyperlocal_customer_detailed_segment,
        cs.hyperlocal_customer_detailed_segment_order,
        cs.purchase_frequency_bucket,
        cs.purchase_frequency_bucket_order,
        cs.customer_recency_segment,
        cs.customer_recency_segment_order,
        cs.customer_tenure_segment,
        cs.customer_tenure_segment_order,
        cs.customer_type,
        cs.customer_channel_distribution,
        cs.acquisition_cohort,
        cs.acquisition_cohort_rank,
        
        -- Offer/Discount metrics
        cs.orders_with_discount_count,
        cs.total_discount_amount,
        cs.distinct_offers_used,
        cs.offer_seeker_segment,
        cs.offer_seeker_segment_order,
        
        -- Discount Affinity metrics
        cs.discount_usage_rate_pct,
        cs.discount_dependency_pct,
        cs.discount_affinity_score,
        cs.discount_affinity_percentile,
        cs.discount_affinity_segment,
        cs.discount_affinity_segment_order,
        
        -- Pet Ownership metrics
        cs.dog_revenue,
        cs.cat_revenue,
        cs.fish_revenue,
        cs.bird_revenue,
        cs.small_pet_revenue,
        cs.reptile_revenue,
        cs.dog_orders,
        cs.cat_orders,
        cs.fish_orders,
        cs.bird_orders,
        cs.small_pet_orders,
        cs.reptile_orders,
        cs.is_dog_owner,
        cs.is_cat_owner,
        cs.is_fish_owner,
        cs.is_bird_owner,
        cs.is_small_pet_owner,
        cs.is_reptile_owner,
        cs.pet_types_count,
        cs.primary_pet_type,
        cs.pet_owner_profile,
        cs.multi_pet_detail,
        
        -- Multiple source tracking
        cs.all_source_nos,
        cs.duplicate_customer_count,
        
        cop.avg_days_between_orders,
        cop.stddev_days_between_orders,
        
        -- RFM segment code
        CONCAT(cs.r_score, cs.f_score, cs.m_score) AS rfm_segment,
        
        -- Customer value segment
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.01 THEN 'Top 1%'
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.20 THEN 'Top 20%'
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.60 THEN 'Middle 30-60%'
            ELSE 'Bottom 40%'
        END AS customer_value_segment,
        
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.01 THEN 1
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.20 THEN 2
            WHEN PERCENT_RANK() OVER (ORDER BY cs.monetary_total_value DESC) <= 0.60 THEN 3
            ELSE 4
        END AS customer_value_segment_order,
        
        -- RFM segment classification
        CASE 
            WHEN cs.r_score >= 4 AND cs.f_score >= 4 AND cs.m_score >= 4 THEN 'Champions'
            WHEN cs.r_score >= 3 AND cs.f_score >= 3 AND cs.m_score >= 3 THEN 'Loyal Customers'
            WHEN cs.f_score >= 4 AND cs.m_score >= 4 THEN 'Cant Lose Them'
            WHEN cs.r_score >= 4 AND (cs.f_score >= 2 OR cs.m_score >= 2) THEN 'Potential Loyalists'
            WHEN cs.customer_tenure_days <= 90 OR cs.r_score >= 4 THEN 'New Customers'
            WHEN cs.f_score >= 2 AND cs.m_score >= 2 OR cs.m_score IN (2,3) THEN 'At Risk'
            WHEN cs.r_score = 1 AND cs.f_score = 1 OR cs.m_score = 1 THEN 'Lost'
            WHEN cs.m_score >= 4 THEN 'Cant Lose Them'
            WHEN cs.f_score >= 3 THEN 'At Risk'
            WHEN cs.r_score >= 3 THEN 'At Risk'
            WHEN cs.f_score >= 2 OR cs.m_score >= 2 THEN 'At Risk'
            ELSE 'Lost'
        END AS customer_rfm_segment,
        
        CASE 
            WHEN cs.r_score >= 4 AND cs.f_score >= 4 AND cs.m_score >= 4 THEN 1
            WHEN cs.r_score >= 3 AND cs.f_score >= 3 AND cs.m_score >= 3 THEN 2
            WHEN cs.f_score >= 4 AND cs.m_score >= 4 THEN 3
            WHEN cs.r_score >= 4 AND (cs.f_score >= 2 OR cs.m_score >= 2) THEN 4
            WHEN cs.customer_tenure_days <= 90 OR cs.r_score >= 4 THEN 5
            WHEN cs.f_score >= 2 AND cs.m_score >= 2 OR cs.m_score IN (2,3) THEN 6
            WHEN cs.r_score = 1 AND cs.f_score = 1 OR cs.m_score = 1 THEN 7
            WHEN cs.m_score >= 4 THEN 3
            WHEN cs.f_score >= 3 THEN 6
            WHEN cs.r_score >= 3 THEN 6
            WHEN cs.f_score >= 2 OR cs.m_score >= 2 THEN 6
            ELSE 7
        END AS customer_rfm_segment_order,
        
        cs.r_score,
        cs.f_score,
        cs.m_score,
        
        -- Purchase frequency type
        CASE 
            WHEN cs.customer_tenure_days <= 30 THEN 'New Customer'
            WHEN cs.total_order_count = 1 AND cs.customer_tenure_days > 30 THEN 'One-Time Buyer'
            WHEN cop.avg_days_between_orders <= 7 AND cs.total_order_count >= 4 THEN 'Weekly Buyer'
            WHEN cop.avg_days_between_orders BETWEEN 8 AND 35 AND cs.total_order_count >= 3 THEN 'Monthly Buyer'
            WHEN cop.avg_days_between_orders BETWEEN 36 AND 120 AND cs.total_order_count >= 2 THEN 'Quarterly Buyer'
            WHEN cop.avg_days_between_orders BETWEEN 121 AND 400 AND cs.total_order_count >= 2 THEN 'Annual Buyer'
            ELSE 'Inconsistent Buyer'
        END AS purchase_frequency_type,
        
        CASE 
            WHEN cs.customer_tenure_days <= 30 THEN 1
            WHEN cs.total_order_count = 1 AND cs.customer_tenure_days > 30 THEN 2
            WHEN cop.avg_days_between_orders <= 7 AND cs.total_order_count >= 4 THEN 3
            WHEN cop.avg_days_between_orders BETWEEN 8 AND 35 AND cs.total_order_count >= 3 THEN 4
            WHEN cop.avg_days_between_orders BETWEEN 36 AND 120 AND cs.total_order_count >= 2 THEN 5
            WHEN cop.avg_days_between_orders BETWEEN 121 AND 400 AND cs.total_order_count >= 2 THEN 6
            ELSE 7
        END AS purchase_frequency_type_order,
        
        -- Customer pattern type (with division by zero protection)
        CASE 
            WHEN cop.avg_days_between_orders IS NULL OR cop.stddev_days_between_orders IS NULL THEN 'No Pattern'
            WHEN cop.avg_days_between_orders = 0 OR cop.avg_days_between_orders IS NULL THEN 'No Pattern'
            WHEN cop.stddev_days_between_orders = 0 THEN 'Perfect Consistency'
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.1 THEN 'Highly Consistent'
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.3 THEN 'Consistent'
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.5 THEN 'Moderately Consistent'
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 1.0 THEN 'Variable'
            ELSE 'Highly Variable'
        END AS customer_pattern_type,
        
        CASE 
            WHEN cop.avg_days_between_orders IS NULL OR cop.stddev_days_between_orders IS NULL THEN 0
            WHEN cop.avg_days_between_orders = 0 OR cop.avg_days_between_orders IS NULL THEN 0
            WHEN cop.stddev_days_between_orders = 0 THEN 1
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.1 THEN 2
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.3 THEN 3
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 0.5 THEN 4
            WHEN cop.stddev_days_between_orders / NULLIF(cop.avg_days_between_orders, 0) <= 1.0 THEN 5
            ELSE 6
        END AS customer_pattern_type_order,
        
        -- Churn risk score (with division by zero protection)
        CASE 
            WHEN cs.total_order_count <= 1 THEN 
                CASE 
                    WHEN cs.recency_days <= 30 THEN 10
                    WHEN cs.recency_days <= 60 THEN 30
                    WHEN cs.recency_days <= 90 THEN 50
                    WHEN cs.recency_days <= 180 THEN 70
                    ELSE 90
                END
            WHEN cop.avg_days_between_orders IS NOT NULL AND cop.avg_days_between_orders > 0 THEN
                CASE 
                    WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 
                        LEAST(90 + (cs.recency_days - cop.avg_days_between_orders * 3) / 10, 100)
                    WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 
                        70 + ((cs.recency_days - cop.avg_days_between_orders * 2) / NULLIF(cop.avg_days_between_orders, 0)) * 20
                    WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 
                        50 + ((cs.recency_days - cop.avg_days_between_orders * 1.5) / NULLIF(cop.avg_days_between_orders, 0)) * 20
                    WHEN cs.recency_days > cop.avg_days_between_orders THEN 
                        30 + ((cs.recency_days - cop.avg_days_between_orders) / NULLIF(cop.avg_days_between_orders, 0)) * 20
                    ELSE GREATEST(10, (cs.recency_days / NULLIF(cop.avg_days_between_orders, 0)) * 30)
                END
            ELSE 
                CASE 
                    WHEN cs.recency_days <= 30 THEN 20
                    WHEN cs.recency_days <= 60 THEN 40
                    WHEN cs.recency_days <= 90 THEN 60
                    WHEN cs.recency_days <= 180 THEN 80
                    ELSE 95
                END
        END AS churn_risk_score,
        
        -- Churn risk level
        CASE 
            WHEN cs.total_order_count <= 1 AND cs.recency_days <= 30 THEN 'New Customer'
            WHEN cs.recency_days <= 30 THEN 'Low'
            WHEN cs.recency_days <= 60 THEN 'Medium'
            WHEN cs.recency_days <= 90 THEN 'High'
            ELSE 'Critical'
        END AS churn_risk_level,
        
        CASE 
            WHEN cs.total_order_count <= 1 AND cs.recency_days <= 30 THEN 0
            WHEN cs.recency_days <= 30 THEN 1
            WHEN cs.recency_days <= 60 THEN 2
            WHEN cs.recency_days <= 90 THEN 3
            ELSE 4
        END AS churn_risk_level_order,
        
        -- Days until expected order
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL AND cs.recency_days < cop.avg_days_between_orders 
            THEN ROUND(cop.avg_days_between_orders - cs.recency_days, 0)
            ELSE NULL
        END AS days_until_expected_order,
        
        -- Is overdue
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL AND cop.avg_days_between_orders > 0 
                 AND cs.recency_days > cop.avg_days_between_orders * 1.2 
            THEN 'Yes'
            ELSE 'No'
        END AS is_overdue,
        
        -- Overdue confidence (with division by zero protection)
        CASE 
            WHEN cop.avg_days_between_orders IS NOT NULL 
                 AND cop.stddev_days_between_orders IS NOT NULL 
                 AND cop.avg_days_between_orders > 0
                 AND cop.stddev_days_between_orders > 0 THEN
                CASE 
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 3 * cop.stddev_days_between_orders) 
                    THEN '99.7% Confidence Overdue'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + 2 * cop.stddev_days_between_orders) 
                    THEN '95% Confidence Overdue'
                    WHEN cs.recency_days > (cop.avg_days_between_orders + cop.stddev_days_between_orders) 
                    THEN '68% Confidence Overdue'
                    WHEN cs.recency_days > cop.avg_days_between_orders 
                    THEN 'Slightly Late (Within Normal Variation)'
                    ELSE 'On Schedule'
                END
            ELSE NULL
        END AS overdue_confidence,
        
        -- Churn action required
        CASE 
            WHEN cs.total_order_count = 1 THEN 'Single Order - Monitor'
            WHEN cs.recency_days > 365 THEN 'Dormant - Reactivation Needed'
            WHEN cop.avg_days_between_orders IS NOT NULL AND cop.avg_days_between_orders > 0 THEN
                CASE 
                    WHEN cs.recency_days > cop.avg_days_between_orders * 3 THEN 'Severely Overdue'
                    WHEN cs.recency_days > cop.avg_days_between_orders * 2 THEN 'Overdue - Action Required'
                    WHEN cs.recency_days > cop.avg_days_between_orders * 1.5 THEN 'At Risk - Engage Soon'
                    ELSE 'On Track'
                END
            WHEN cs.monetary_total_value > 10000 AND cs.recency_days > 60 THEN 'High Value - At Risk'
            ELSE 'Monitor'
        END AS churn_action_required
        
    FROM customer_rfm_scores cs
    LEFT JOIN customer_order_patterns cop ON cs.unified_customer_id = cop.unified_customer_id
)

-- =====================================================
-- FINAL OUTPUT
-- =====================================================
SELECT * FROM customer_segments
ORDER BY total_sales_value DESC