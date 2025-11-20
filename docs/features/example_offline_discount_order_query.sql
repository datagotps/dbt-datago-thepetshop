-- =====================================================
-- EXAMPLE: Offline Order with Discount Analysis
-- Shows complete order details from header and line level
-- =====================================================

-- Step 1: Find an offline order with discount
WITH sample_offline_order AS (
    SELECT 
        document_no_,
        posting_date,
        sales_channel,
        offline_order_channel
    FROM {{ ref('fact_commercial') }}
    WHERE sales_channel = 'Shop'  -- Offline orders
        AND has_discount = 1       -- Orders with discount
        AND transaction_type = 'Sale'
        AND offline_offer_name IS NOT NULL  -- Has offer name
        AND posting_date >= '2024-01-01'
    LIMIT 1
),

-- Step 2: Get ORDER HEADER summary from fact_commercial
order_header AS (
    SELECT 
        fc.document_no_,
        fc.posting_date as order_date,
        fc.sales_channel,
        fc.offline_order_channel as offline_store,
        
        -- Customer Info
        MAX(fc.customer_name) as customer_name,
        MAX(fc.std_phone_no_) as std_phone_no_,
        MAX(fc.source_no_) as customer_id,
        
        -- Order Totals
        SUM(fc.sales_amount_gross) as total_gross_amount,
        SUM(fc.discount_amount) as total_discount_amount,
        SUM(fc.sales_amount__actual_) as total_paid_amount,
        COUNT(*) as line_items_count,
        
        -- Payment Info
        MAX(fc.paymentgateway) as paymentgateway,
        MAX(fc.paymentmethodcode) as paymentmethodcode,
        
        -- Offer Info
        STRING_AGG(DISTINCT fc.offline_offer_name, ', ') as offers_used
        
    FROM {{ ref('fact_commercial') }} fc
    INNER JOIN sample_offline_order so 
        ON fc.document_no_ = so.document_no_
    WHERE fc.transaction_type = 'Sale'
    GROUP BY 1,2,3,4
),

-- Step 3: Get ORDER LINE details from fact_commercial
order_lines AS (
    SELECT 
        fc.document_no_,
        fc.unified_order_id,
        fc.item_ledger_entry_no_,
        
        -- Item Details
        fc.item_no_,
        fc.item_name,
        fc.item_category,
        fc.item_brand,
        fc.invoiced_quantity,
        
        -- Discount Analysis
        fc.discount_status,
        fc.has_discount,
        fc.sales_amount_gross,      -- Original price before discount
        fc.discount_amount,          -- Discount given
        fc.sales_amount__actual_,   -- Final paid amount
        
        -- Discount Offer Details
        fc.offline_discount_amount,
        fc.offline_offer_no_,
        fc.offline_offer_name,
        
        -- Calculate discount percentage
        ROUND(
            CASE 
                WHEN fc.sales_amount_gross != 0 
                THEN (fc.discount_amount / fc.sales_amount_gross) * 100
                ELSE 0
            END, 2
        ) as discount_percentage,
        
        -- Financial Details
        fc.cost_amount__actual_,
        fc.posting_date,
        fc.location_code
        
    FROM {{ ref('fact_commercial') }} fc
    INNER JOIN sample_offline_order so 
        ON fc.document_no_ = so.document_no_
    WHERE fc.transaction_type = 'Sale'
    ORDER BY fc.item_ledger_entry_no_
)

-- Final Output: Order Header + Line Details
SELECT 
    '=== ORDER HEADER SUMMARY ===' as section,
    oh.document_no_ as order_number,
    CAST(oh.order_date AS STRING) as order_date,
    oh.sales_channel,
    oh.offline_store,
    oh.customer_name,
    oh.std_phone_no_,
    CAST(oh.total_gross_amount AS STRING) as total_gross,
    CAST(oh.total_discount_amount AS STRING) as total_discount,
    CAST(oh.total_paid_amount AS STRING) as total_paid,
    CAST(oh.line_items_count AS STRING) as items_count,
    oh.paymentgateway,
    oh.offers_used,
    NULL as item_no_,
    NULL as item_name,
    NULL as quantity,
    NULL as unit_gross,
    NULL as unit_discount,
    NULL as unit_paid,
    NULL as discount_pct
FROM order_header oh

UNION ALL

SELECT 
    '=== ORDER LINE DETAILS ===' as section,
    ol.document_no_ as order_number,
    CAST(ol.posting_date AS STRING) as order_date,
    NULL as sales_channel,
    ol.location_code as offline_store,
    NULL as customer_name,
    NULL as std_phone_no_,
    NULL as total_gross,
    NULL as total_discount,
    NULL as total_paid,
    NULL as items_count,
    NULL as paymentgateway,
    ol.offline_offer_name as offers_used,
    ol.item_no_,
    ol.item_name,
    CAST(ol.invoiced_quantity AS STRING) as quantity,
    CAST(ol.sales_amount_gross AS STRING) as unit_gross,
    CAST(ol.discount_amount AS STRING) as unit_discount,
    CAST(ol.sales_amount__actual_ AS STRING) as unit_paid,
    CAST(ol.discount_percentage AS STRING) as discount_pct
FROM order_lines ol

ORDER BY section DESC;


-- =====================================================
-- ALTERNATIVE: Simple Check - Find Offline Orders with Discounts
-- =====================================================
-- Run this first to verify data exists:

/*
SELECT 
    document_no_,
    posting_date,
    sales_channel,
    offline_order_channel,
    customer_name,
    item_name,
    sales_amount_gross,
    discount_amount,
    sales_amount__actual_,
    offline_offer_no_,
    offline_offer_name
FROM {{ ref('fact_commercial') }}
WHERE sales_channel = 'Shop'
    AND has_discount = 1
    AND transaction_type = 'Sale'
    AND posting_date >= '2024-01-01'
ORDER BY posting_date DESC
LIMIT 10;
*/
