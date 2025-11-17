-- =====================================================
-- Shopify vs OFS Coupon Code Comparison Queries
-- Test Case: Order O3083424S (INV00441924)
-- =====================================================

-- =====================================================
-- QUERY 1: fact_commercial - Current dbt Model
-- =====================================================
-- Shows all line items with discount details from current model
SELECT 
    document_no_,
    web_order_id,
    posting_date,
    customer_name,
    item_no_,
    item_name,
    invoiced_quantity,
    sales_amount_gross,
    online_discount_amount,
    sales_amount__actual_,
    online_offer_no_
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE web_order_id = 'O3083424S'
    AND transaction_type = 'Sale'
ORDER BY item_no_;

-- Summary for fact_commercial
SELECT 
    web_order_id,
    document_no_,
    posting_date,
    MAX(customer_name) as customer_name,
    COUNT(*) as line_items,
    SUM(sales_amount_gross) as total_gross,
    SUM(online_discount_amount) as total_discount,
    SUM(sales_amount__actual_) as total_paid,
    MAX(online_offer_no_) as coupon_code
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE web_order_id = 'O3083424S'
    AND transaction_type = 'Sale'
GROUP BY 1,2,3;


-- =====================================================
-- QUERY 2: Shopify Orders - Native Shopify Data
-- =====================================================
-- Shows order-level details including coupon code from Shopify
-- NOTE: Shopify stores amounts INCLUDING VAT (5% in UAE)
-- Adding VAT-exclusive calculations for comparison with dbt models
SELECT 
    id as order_id,
    name as order_number,
    created_at,
    
    -- Original Shopify amounts (INCLUDING VAT)
    total_price as total_price_incl_vat,
    subtotal_price as subtotal_price_incl_vat,
    total_discounts as total_discounts_incl_vat,
    
    -- VAT-exclusive amounts (for comparison with fact_orders/fact_commercial)
    ROUND(total_price / 1.05, 2) as total_price_excl_vat,
    ROUND(subtotal_price / 1.05, 2) as subtotal_price_excl_vat,
    ROUND(total_discounts / 1.05, 2) as total_discounts_excl_vat,
    
    -- Coupon code (KEY FIELD - missing in OFS)
    discount_codes,
    
    -- Other details
    financial_status,
    customer
FROM `tps-data-386515`.`shopify`.`orders`
WHERE name = 'O3083424S';


-- =====================================================
-- QUERY 3: fact_orders - Order Header Level
-- =====================================================
-- Shows order-level summary from fact_orders (no discount fields)
SELECT 
    unified_order_id,
    document_no_,
    web_order_id,
    order_date,
    customer_name,
    unified_customer_id,
    order_value,
    refund_amount,
    total_order_amount,
    line_items_count,
    sales_channel,
    order_channel,
    order_type,
    paymentgateway,
    paymentmethodcode
FROM `tps-data-386515.dbt_dev_datago_fct.fact_orders`
WHERE web_order_id = 'O3083424S';


-- =====================================================
-- QUERY 4: OFS Payment Line - Current Source for Coupons
-- =====================================================
-- Shows what's in OFS payment line table (currently used for coupon codes)
SELECT 
    weborderno,
    itemid,
    couponcode,
    isheader,
    amount,
    discount
FROM `tps-data-386515.dbt_dev_datago_stg.stg_ofs_inboundpaymentline`
WHERE weborderno = 'O3083424S'
ORDER BY isheader, itemid;

-- Summary for OFS
SELECT 
    weborderno,
    COUNT(*) as line_items,
    SUM(amount) as total_amount,
    SUM(discount) as total_discount,
    MAX(couponcode) as coupon_code
FROM `tps-data-386515.dbt_dev_datago_stg.stg_ofs_inboundpaymentline`
WHERE weborderno = 'O3083424S'
    AND isheader = 0
GROUP BY 1;


-- =====================================================
-- QUERY 4: Find Online Orders with Discount but NO Coupon
-- =====================================================
-- Finds orders that have discounts but missing coupon codes in fact_commercial
SELECT 
    document_no_,
    web_order_id,
    posting_date,
    customer_name,
    SUM(sales_amount_gross) as total_gross,
    SUM(online_discount_amount) as total_discount,
    SUM(sales_amount__actual_) as total_paid,
    MAX(online_offer_no_) as coupon_code
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE sales_channel = 'Online'
    AND has_discount = 1
    AND transaction_type = 'Sale'
    AND (online_offer_no_ IS NULL OR online_offer_no_ = '')
    AND posting_date >= '2025-02-01'
    AND posting_date < '2025-02-19'
GROUP BY 1,2,3,4
ORDER BY posting_date DESC
LIMIT 10;


-- =====================================================
-- QUERY 5: Check if Shopify has Coupon for Same Orders
-- =====================================================
-- Cross-check if Shopify has coupon codes for orders missing them in fact_commercial
SELECT 
    id as order_id,
    name as order_number,
    created_at,
    total_price,
    total_discounts,
    discount_codes
FROM `tps-data-386515`.`shopify`.`orders`
WHERE name IN ('O3083424S', 'O3083200S', 'O3083106S', 'O3083023S', 'O3083077S')
ORDER BY created_at DESC;


-- =====================================================
-- QUERY 6: Shopify Orders with Discounts (Sample)
-- =====================================================
-- Find all Shopify orders with discount codes in the available date range
SELECT 
    id as order_id,
    name as order_number,
    created_at,
    total_price,
    total_discounts,
    discount_codes
FROM `tps-data-386515`.`shopify`.`orders`
WHERE total_discounts > 0
    AND created_at >= '2025-02-01'
    AND created_at < '2025-02-20'
ORDER BY created_at DESC
LIMIT 20;


-- =====================================================
-- QUERY 7: Proposed Join - Shopify + fact_commercial
-- =====================================================
-- Shows how to join Shopify data to enrich fact_commercial with coupon codes
SELECT 
    fc.document_no_,
    fc.web_order_id,
    fc.posting_date,
    fc.customer_name,
    
    -- Current model (often empty)
    fc.online_offer_no_ as ofs_coupon_code,
    
    -- Shopify data (reliable)
    s.discount_codes[0].code as shopify_coupon_code,
    s.discount_codes[0].type as shopify_discount_type,
    
    -- Proposed logic: Use Shopify as primary source
    COALESCE(
        s.discount_codes[0].code,
        fc.online_offer_no_
    ) as final_coupon_code,
    
    -- Amounts
    fc.sales_amount_gross,
    fc.online_discount_amount as fc_discount,
    s.total_discounts as shopify_discount,
    fc.sales_amount__actual_
    
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial` fc
LEFT JOIN `tps-data-386515`.`shopify`.`orders` s
    ON fc.web_order_id = s.name
WHERE fc.web_order_id = 'O3083424S'
    AND fc.transaction_type = 'Sale'
LIMIT 5;


-- =====================================================
-- QUERY 8: Gap Analysis - Orders Missing Coupon Codes
-- =====================================================
-- Count how many online orders have discounts but no coupon code
SELECT 
    DATE(posting_date) as order_date,
    COUNT(DISTINCT web_order_id) as orders_with_discount,
    COUNT(DISTINCT CASE 
        WHEN online_offer_no_ IS NULL OR online_offer_no_ = '' 
        THEN web_order_id 
    END) as orders_missing_coupon,
    ROUND(
        COUNT(DISTINCT CASE 
            WHEN online_offer_no_ IS NULL OR online_offer_no_ = '' 
            THEN web_order_id 
        END) * 100.0 / COUNT(DISTINCT web_order_id),
        2
    ) as pct_missing_coupon
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE sales_channel = 'Online'
    AND has_discount = 1
    AND transaction_type = 'Sale'
    AND posting_date >= '2025-02-01'
    AND posting_date < '2025-02-19'
GROUP BY 1
ORDER BY 1 DESC;


-- =====================================================
-- QUERY 9: Discount Amount Reconciliation
-- =====================================================
-- Compare discount amounts between fact_commercial and Shopify
WITH fc_summary AS (
    SELECT 
        web_order_id,
        SUM(online_discount_amount) as fc_total_discount,
        SUM(sales_amount_gross) as fc_total_gross,
        SUM(sales_amount__actual_) as fc_total_paid
    FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
    WHERE sales_channel = 'Online'
        AND transaction_type = 'Sale'
        AND posting_date >= '2025-02-17'
        AND posting_date < '2025-02-19'
    GROUP BY 1
),
shopify_summary AS (
    SELECT 
        name as web_order_id,
        total_discounts as shopify_total_discount,
        total_price as shopify_total_price,
        discount_codes[0].code as coupon_code
    FROM `tps-data-386515`.`shopify`.`orders`
    WHERE created_at >= '2025-02-17'
        AND created_at < '2025-02-19'
        AND total_discounts > 0
)
SELECT 
    fc.web_order_id,
    fc.fc_total_discount,
    s.shopify_total_discount,
    (s.shopify_total_discount - fc.fc_total_discount) as discount_difference,
    s.coupon_code,
    fc.fc_total_gross,
    s.shopify_total_price
FROM fc_summary fc
INNER JOIN shopify_summary s
    ON fc.web_order_id = s.web_order_id
ORDER BY ABS(s.shopify_total_discount - fc.fc_total_discount) DESC
LIMIT 10;


-- =====================================================
-- QUERY 10: Coupon Code Distribution in Shopify
-- =====================================================
-- See which coupon codes are most popular in Shopify data
SELECT 
    discount_codes[0].code as coupon_code,
    COUNT(*) as usage_count,
    SUM(total_discounts) as total_discount_given,
    AVG(total_discounts) as avg_discount_per_order,
    MIN(created_at) as first_used,
    MAX(created_at) as last_used
FROM `tps-data-386515`.`shopify`.`orders`
WHERE total_discounts > 0
    AND created_at >= '2025-01-01'
    AND created_at < '2025-03-01'
GROUP BY 1
ORDER BY usage_count DESC
LIMIT 20;
