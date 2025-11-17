-- =====================================================
-- SIMPLE QUERY: Online Order with Discount
-- Order: INV00537669 (Web Order: O30175330S)
-- =====================================================

-- Full order details with all discount columns
SELECT 
    document_no_,
    web_order_id,
    posting_date,
    sales_channel,
    online_order_channel,
    customer_name,
    std_phone_no_,
    item_no_,
    item_name,
    item_category,
    invoiced_quantity,
    
    -- DISCOUNT ANALYSIS COLUMNS
    sales_amount_gross,           -- Original price before discount
    online_discount_amount,       -- Online discount (VAT-exclusive)
    discount_amount,              -- Consolidated discount
    sales_amount__actual_,        -- Final paid amount
    online_offer_no_,             -- Coupon code (if any)
    
    -- Additional details
    paymentgateway,
    order_type
    
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE document_no_ = 'INV00537669'
    AND transaction_type = 'Sale';


-- =====================================================
-- ORDER SUMMARY
-- =====================================================
-- Order Number: INV00537669
-- Web Order ID: O30175330S
-- Date: 2025-09-30
-- Channel: Online (Website)
-- Order Type: EXPRESS
-- Payment: Adyen
-- Customer: GUEST CUSTOMER DO NOT DELETE (TECH)
-- Phone: 971551255555
--
-- FINANCIAL SUMMARY:
-- Total Items: 10 items
-- Total Gross: 2,927.58 AED
-- Total Discount: -585.52 AED (20% average)
-- Total Paid: 2,342.06 AED
--
-- ITEMS BREAKDOWN:
-- 1. Royal Canin British Shorthair 4KG - 199.05 → 159.24 (-39.81)
-- 2. Royal Canin Hair & Skin 4KG - 228.10 → 182.48 (-45.62)
-- 3. Royal Canin Hair & Skin 4KG - 228.10 → 182.48 (-45.62)
-- 4. Royal Canin Persian 10KG - 465.24 → 372.19 (-93.05)
-- 5. Royal Canin Urinary Care 4KG - 204.99 → 164.00 (-40.99)
-- 6. Royal Canin Regular Fit 4KG - 200.00 → 159.99 (-40.01)
-- 7. Royal Canin Savour Exigent 10KG - 475.24 → 380.19 (-95.05)
-- 8. Royal Canin Savour Exigent 4KG - 223.81 → 179.05 (-44.76)
-- 9. Royal Canin Hair & Skin 10KG - 376.19 → 300.95 (-75.24)
-- 10. Royal Canin Regular Fit 10KG - 326.86 → 261.49 (-65.37)
-- =====================================================


-- =====================================================
-- FIND MORE ONLINE ORDERS WITH DISCOUNTS
-- =====================================================
SELECT 
    document_no_,
    web_order_id,
    posting_date,
    sales_channel,
    online_order_channel,
    customer_name,
    item_name,
    sales_amount_gross,
    online_discount_amount,
    discount_amount,
    sales_amount__actual_,
    online_offer_no_
FROM `tps-data-386515.dbt_dev_datago_fct.fact_commercial`
WHERE sales_channel = 'Online'
    AND has_discount = 1
    AND transaction_type = 'Sale'
    AND posting_date >= '2024-01-01'
ORDER BY posting_date DESC
LIMIT 20;
