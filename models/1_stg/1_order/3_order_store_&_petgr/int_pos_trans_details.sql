-- =====================================================
-- int_pos_trans_details: POS Transaction Details with Header
-- Joins line-level details with transaction header
-- Adds calculated columns for sales channel, margins, etc.
-- =====================================================

WITH 
-- Get details (line level)
details AS (
    SELECT * FROM {{ ref('stg_pos_trans_details') }}
),

-- Get header (transaction level)
header AS (
    SELECT * FROM {{ ref('stg_pos_trans_header') }}
),

-- Get staff master data (using original source column names due to BigQuery caching)
staff AS (
    SELECT 
        id AS staff_id,
        TRIM(CONCAT(first_name, ' ', last_name)) AS staff_full_name,
        CASE 
            WHEN permission_group = 'MANAGER' THEN 'Manager'
            WHEN permission_group = 'CASHIER' THEN 'Cashier'
            WHEN permission_group = 'TPSCHR' THEN 'Team Lead'
            ELSE permission_group
        END AS staff_role,
        name_on_receipt AS staff_receipt_name,
        store_no_ AS staff_store_code
    FROM {{ source(var('erp_source'), 'petshop_staff_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
),

-- Get customer master data (for customer_identity_status)
customers AS (
    SELECT 
        no_ AS customer_no,
        customer_identity_status,
        customer_journey_segment,
        name AS customer_name,
        retail_customer_group
    FROM {{ ref('int_erp_customer') }}
),

-- Get Value Entry document numbers (to check if POS transaction was posted)
value_entry_docs AS (
    SELECT DISTINCT document_no_
    FROM {{ ref('stg_value_entry') }}
    WHERE document_no_ LIKE '%-%-%'  -- POS format: STORE-TERMINAL-TRANS
),

-- Join details with header (LEFT JOIN to preserve all detail lines)
joined AS (
    SELECT
        -- =====================================================
        -- LINE-LEVEL FIELDS (from details)
        -- =====================================================
        
        -- Primary Identifiers
        d.document_no_,
        d.store_no_,
        d.pos_terminal_no_,
        d.transaction_no_,
        d.line_no_,
        d.receipt_no_,
        
        -- Item Information
        d.item_no_,
        d.parent_item_no_,
        d.variant_code,
        d.barcode_no_,
        d.item_category_code,
        d.retail_product_code,
        d.retail_product_code_2,
        d.item_posting_group,
        d.item_disc__group,
        d.price_group_code,
        
        -- Customer (from detail line)
        d.customer_no_,
        
        -- Effective Customer (COALESCE: line first, then header fallback)
        COALESCE(
            NULLIF(d.customer_no_, ''),
            NULLIF(h.customer_no_, '')
        ) AS effective_customer_no_,
        
        -- Customer ID Source (for tracking data quality)
        CASE 
            WHEN d.customer_no_ IS NOT NULL AND d.customer_no_ != '' THEN 'Line'
            WHEN h.customer_no_ IS NOT NULL AND h.customer_no_ != '' THEN 'Header (Recovered)'
            ELSE 'Missing'
        END AS customer_id_source,
        
        -- Date & Time (line level)
        d.pos_posting_date,                    -- When transaction was finalized/posted (matches Value Entry)
        d.pos_document_date,                   -- When customer started checkout (document date)
        d.pos_posting_time,                    -- Time when finalized
        d.pos_document_time,                   -- Time when checkout started
        d.shift_date AS line_shift_date,
        d.shift_no_ AS line_shift_no_,
        d.expiration_date,
        
        -- Quantity & UOM
        d.quantity,
        d.refund_qty_,
        d.uom_quantity,
        d.unit_of_measure,
        
        -- Pricing & Amounts (line level)
        d.price,
        d.net_price,
        d.standard_net_price,
        d.uom_price,
        d.net_amount,
        d.cost_amount,
        d.vat_amount,
        
        -- Discounts (line level)
        d.discount_amount,
        d.customer_discount AS line_customer_discount,
        d.line_discount,
        d.total_discount AS line_total_discount,
        d.periodic_discount,
        d.coupon_discount,
        d.line_was_discounted,
        
        -- Promotion Fields
        d.promotion_no_,
        d.periodic_disc__group,
        d.periodic_disc__type,
        
        -- Transaction Type (line level)
        d.transaction_code AS line_transaction_code,
        d.sales_type AS line_sales_type,
        d.type_of_sale,
        d.return_no_sale,
        
        -- Staff (line level)
        d.staff_id AS line_staff_id,
        d.sales_staff,
        
        -- Staff Name (from staff master)
        s.staff_full_name AS staff_name,
        s.staff_role,
        s.staff_receipt_name,
        
        -- Customer Info (from customer master)
        c.customer_identity_status,      -- 'Anonymous' (Walk-in) or 'Identified'
        c.customer_journey_segment,      -- 'offline_customer', 'online_legacy_customer', 'online_ofs_customer'
        c.customer_name,
        c.retail_customer_group,
        
        -- Refund Reference
        d.refunded_line_no_,
        d.refunded_trans__no_,
        d.refunded_pos_no_,
        d.refunded_store_no_,
        
        -- System Fields (line)
        d._fivetran_synced AS line_fivetran_synced,
        d._fivetran_deleted AS line_fivetran_deleted,
        
        -- =====================================================
        -- HEADER-LEVEL FIELDS (from header)
        -- =====================================================
        
        -- Transaction Type & Status (header)
        h.transaction_type AS header_transaction_type,
        h.transaction_code AS header_transaction_code,
        h.entry_status,
        h.sale_is_return_sale,
        h.trans__is_mixed_sale_refund,
        h.customer_order,
        h.customer_order_id,
        
        -- Date & Time (header level)
        h.pos_posting_date AS header_pos_posting_date,
        h.original_date,
        h.pos_posting_time AS header_pos_posting_time,
        h.time_when_trans__closed,
        h.shift_date AS header_shift_date,
        h.shift_no_ AS header_shift_no_,
        
        -- Customer (header level)
        h.customer_no_ AS header_customer_no_,
        h.member_card_no_,
        h.customer_disc__group,
        
        -- Staff (header level)
        h.staff_id AS header_staff_id,
        h.manager_id,
        
        -- Transaction Totals (header level)
        h.gross_amount AS header_gross_amount,
        h.net_amount AS header_net_amount,
        h.payment AS header_payment,
        h.cost_amount AS header_cost_amount,
        h.discount_amount AS header_discount_amount,
        h.total_discount AS header_total_discount,
        
        -- Item Counts
        h.no__of_items,
        h.no__of_item_lines,
        
        -- Refund Receipt Reference
        h.refund_receipt_no_,
        
        -- Value Entry Posting Check
        CASE WHEN ve.document_no_ IS NOT NULL THEN TRUE ELSE FALSE END AS is_in_value_entry
        
    FROM details d
    LEFT JOIN header h ON d.document_no_ = h.document_no_
    LEFT JOIN staff s ON CAST(d.staff_id AS STRING) = s.staff_id
    -- Use COALESCE to match customer: try line first, then header
    LEFT JOIN customers c ON COALESCE(NULLIF(d.customer_no_, ''), NULLIF(h.customer_no_, '')) = c.customer_no
    -- Check if transaction exists in Value Entry
    LEFT JOIN value_entry_docs ve ON d.document_no_ = ve.document_no_
),

-- Add calculated columns
enriched AS (
    SELECT
        *,
        
        -- =====================================================
        -- SALES CHANNEL CLASSIFICATION
        -- =====================================================
        -- Note: "Hyperlocal" = Online orders fulfilled from store (not in POS)
        -- All third-party platforms (Instashop, Deliveroo, Amazon, etc.) = "Affiliate"
        
        -- Sales Channel (high-level)
        CASE 
            -- Service first (by store)
            WHEN store_no_ = 'MOBILE' THEN 'Service'
            WHEN store_no_ = 'GRM' THEN 'Service'
            -- Affiliate = ALL third-party platforms (quick commerce + marketplace)
            WHEN header_customer_no_ IN (
                -- Quick commerce platforms
                'BCN/2021/4059',  -- Instashop
                'BCN/2021/4067',  -- Deliveroo
                'BCN/2021/4064',  -- Talabat
                'BCN/2021/4408',  -- Noon
                'BCN/2021/4063',  -- Careem
                'BCN/2021/4060',  -- El Grocer
                -- Marketplace platforms
                'BCN/2021/0691',  -- Amazon/Souq
                'BCN/2021/4066',  -- Amazon DFS
                'BCN/2024/4066',  -- Amazon DFS (new code)
                'BCN/2021/4061'   -- Swan
            ) THEN 'Affiliate'
            -- Everything else is Shopfloor (walk-in customers)
            ELSE 'Shopfloor'
        END AS pos_sales_channel,
        
        -- Sales Channel Detail (specific partner/type)
        CASE 
            -- Service types
            WHEN store_no_ = 'MOBILE' THEN 'Mobile Grooming'
            WHEN store_no_ = 'GRM' THEN 'Shop Grooming'
            -- Quick commerce platforms (Affiliate)
            WHEN header_customer_no_ = 'BCN/2021/4059' THEN 'Instashop'
            WHEN header_customer_no_ = 'BCN/2021/4067' THEN 'Deliveroo'
            WHEN header_customer_no_ = 'BCN/2021/4064' THEN 'Talabat'
            WHEN header_customer_no_ = 'BCN/2021/4408' THEN 'Noon'
            WHEN header_customer_no_ = 'BCN/2021/4063' THEN 'Careem'
            WHEN header_customer_no_ = 'BCN/2021/4060' THEN 'El Grocer'
            -- Marketplace platforms (Affiliate)
            WHEN header_customer_no_ = 'BCN/2021/0691' THEN 'Amazon/Souq'
            WHEN header_customer_no_ IN ('BCN/2021/4066', 'BCN/2024/4066') THEN 'Amazon DFS'
            WHEN header_customer_no_ = 'BCN/2021/4061' THEN 'Swan'
            -- Shopfloor types (use customer_identity_status from int_erp_customer)
            WHEN customer_identity_status = 'Anonymous' THEN 'Walk-in'
            WHEN header_customer_no_ IS NULL OR header_customer_no_ = '' THEN 'Walk-in'
            ELSE 'Retail Customer'
        END AS pos_sales_channel_detail,
        
        -- Sales Channel Sort Order (1=Shopfloor, 2=Affiliate, 3=Service)
        CASE 
            WHEN store_no_ IN ('MOBILE', 'GRM') THEN 3  -- Service
            WHEN header_customer_no_ IN (
                'BCN/2021/4059', 'BCN/2021/4067', 'BCN/2021/4064', 
                'BCN/2021/4408', 'BCN/2021/4063', 'BCN/2021/4060',
                'BCN/2021/0691', 'BCN/2021/4066', 'BCN/2024/4066', 
                'BCN/2021/4061'
            ) THEN 2  -- Affiliate
            ELSE 1    -- Shopfloor
        END AS pos_sales_channel_sort,
        
        -- =====================================================
        -- TRANSACTION TYPE CLASSIFICATION
        -- =====================================================
        
        -- Transaction Type (Sale vs Refund)
        -- In POS: quantity < 0 = items leaving (SALE), quantity > 0 = items returning (REFUND)
        -- In POS: net_amount < 0 = revenue in (SALE), net_amount > 0 = money back (REFUND)
        CASE 
            WHEN sale_is_return_sale = 1 THEN 'Refund'
            WHEN quantity > 0 THEN 'Refund'           -- Items returning to inventory
            WHEN net_amount > 0 THEN 'Refund'         -- Money going back to customer
            ELSE 'Sale'
        END AS transaction_type,
        
        -- Is Refund Flag
        CASE 
            WHEN sale_is_return_sale = 1 OR quantity > 0 OR net_amount > 0 THEN 1 
            ELSE 0 
        END AS is_refund,
        
        -- =====================================================
        -- VALUE ENTRY POSTING STATUS
        -- Tracks if transaction exists in Value Entry (source of fact_commercial)
        -- Uses actual join to value_entry_docs to verify
        -- Service is excluded from fact_commercial (separate model) - check is N/A
        -- =====================================================
        
        -- Is Posted to Value Entry (boolean - based on actual existence in Value Entry)
        is_in_value_entry AS is_posted_to_value_entry,
        
        -- Value Entry Posting Status (descriptive)
        CASE 
            -- Service transactions go to Value Entry via int_pos_service_trans_details
            -- They are NOT in fact_commercial (excluded by filter) - so status is N/A
            WHEN store_no_ IN ('MOBILE', 'GRM') THEN 'Service (Separate Model)'
            -- Shopfloor & Affiliate should be posted
            WHEN is_in_value_entry = TRUE THEN 'Posted'
            WHEN is_in_value_entry = FALSE AND header_customer_no_ IN (
                'BCN/2021/4059', 'BCN/2021/4067', 'BCN/2021/4064', 
                'BCN/2021/4408', 'BCN/2021/4063', 'BCN/2021/4060',
                'BCN/2021/0691', 'BCN/2021/4066', 'BCN/2024/4066', 
                'BCN/2021/4061'
            ) THEN 'Affiliate - Not Posted (Issue)'
            WHEN is_in_value_entry = FALSE THEN 'Shopfloor - Not Posted (Issue)'
            ELSE 'Unknown'
        END AS value_entry_posting_status,
        
        -- =====================================================
        -- ITEM CLASSIFICATION
        -- =====================================================
        
        -- Revenue Category (for reconciliation with Value Entry/Commercial)
        -- Note: Carrier bags & Delivery fees are in POS but NOT in Value Entry
        CASE 
            WHEN item_no_ = '205619-1' THEN 'Carrier Bag'
            WHEN item_no_ IN ('300131', '300132', '300139') THEN 'Delivery Fee'
            WHEN item_category_code IN ('310', '311') THEN 'Service'
            WHEN retail_product_code IN ('31024', '31010', '31011', '31012', '31113', '31114') THEN 'Service'
            ELSE 'Merchandise'
        END AS revenue_category,
        
        -- Is Merchandise (flows to Value Entry)
        CASE 
            WHEN item_no_ = '205619-1' THEN 0  -- Carrier bag
            WHEN item_no_ IN ('300131', '300132', '300139') THEN 0  -- Delivery fees
            ELSE 1
        END AS is_merchandise,
        
        -- Is Carrier Bag
        CASE WHEN item_no_ = '205619-1' THEN 1 ELSE 0 END AS is_carrier_bag,
        
        -- Is Delivery Fee
        CASE WHEN item_no_ IN ('300131', '300132', '300139') THEN 1 ELSE 0 END AS is_delivery_fee,
        
        -- Is Service Item (grooming)
        CASE 
            WHEN retail_product_code IN ('31024', '31010', '31011', '31012', '31113', '31114') THEN 1
            WHEN item_category_code IN ('310', '311') THEN 1
            ELSE 0
        END AS is_service_item,
        
        -- Service Type
        CASE 
            WHEN retail_product_code = '31024' THEN 'Add-on'
            WHEN retail_product_code = '31010' THEN 'Bird Groom'
            WHEN retail_product_code = '31011' THEN 'Cat Groom'
            WHEN retail_product_code = '31012' THEN 'Dog Groom'
            WHEN retail_product_code = '31113' THEN 'Mobile Cat'
            WHEN retail_product_code = '31114' THEN 'Mobile Dog'
            ELSE NULL
        END AS service_type,
        
        -- =====================================================
        -- FINANCIAL CALCULATIONS
        -- =====================================================
        
        -- Gross Amount (before discount) - estimated from net + discount
        ROUND(ABS(net_amount) + ABS(COALESCE(discount_amount, 0)), 2) AS gross_amount,
        
        -- Sales Amount (positive for reporting)
        ROUND(ABS(net_amount), 2) AS sales_amount,
        
        -- Cost Amount (positive for reporting)
        ROUND(ABS(cost_amount), 2) AS cost_amount_abs,
        
        -- Gross Profit
        ROUND(ABS(net_amount) - ABS(COALESCE(cost_amount, 0)), 2) AS gross_profit,
        
        -- Gross Margin %
        CASE 
            WHEN ABS(net_amount) > 0 
            THEN ROUND((ABS(net_amount) - ABS(COALESCE(cost_amount, 0))) / ABS(net_amount) * 100, 2)
            ELSE 0
        END AS gross_margin_pct,
        
        -- Discount Amount (positive)
        ROUND(ABS(COALESCE(discount_amount, 0)), 2) AS discount_amount_abs,
        
        -- Discount %
        CASE 
            WHEN (ABS(net_amount) + ABS(COALESCE(discount_amount, 0))) > 0 
            THEN ROUND(ABS(COALESCE(discount_amount, 0)) / (ABS(net_amount) + ABS(COALESCE(discount_amount, 0))) * 100, 2)
            ELSE 0
        END AS discount_pct,
        
        -- Has Discount Flag
        CASE WHEN COALESCE(discount_amount, 0) != 0 THEN 1 ELSE 0 END AS has_discount,
        
        -- =====================================================
        -- REVENUE BY CATEGORY (for reconciliation)
        -- Merchandise revenue matches Value Entry
        -- Carrier bags & Delivery fees are POS-only (not in Value Entry)
        -- =====================================================
        
        -- Merchandise Revenue (matches Value Entry - signed for proper netting)
        CASE 
            WHEN item_no_ = '205619-1' THEN 0  -- Carrier bag
            WHEN item_no_ IN ('300131', '300132', '300139') THEN 0  -- Delivery fees
            ELSE net_amount
        END AS merchandise_net_amount,
        
        -- Carrier Bag Revenue (POS-only)
        CASE WHEN item_no_ = '205619-1' THEN net_amount ELSE 0 END AS carrier_bag_net_amount,
        
        -- Delivery Fee Revenue (POS-only)
        CASE WHEN item_no_ IN ('300131', '300132', '300139') THEN net_amount ELSE 0 END AS delivery_fee_net_amount,
        
        -- =====================================================
        -- TIME-BASED CALCULATIONS
        -- Note: Using pos_posting_date (matches value entry posting_date)
        -- =====================================================
        
        -- Hour of Transaction (from pos_document_time for operational analysis)
        EXTRACT(HOUR FROM pos_document_time) AS checkout_hour,
        
        -- Day of Week (1=Sunday, 7=Saturday)
        EXTRACT(DAYOFWEEK FROM pos_posting_date) AS day_of_week,
        
        -- Day Name
        FORMAT_DATE('%A', pos_posting_date) AS day_name,
        
        -- Is Weekend
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM pos_posting_date) IN (1, 7) THEN 1 
            ELSE 0 
        END AS is_weekend,
        
        -- Month
        FORMAT_DATE('%Y-%m', pos_posting_date) AS year_month,
        
        -- Week Number
        EXTRACT(WEEK FROM pos_posting_date) AS week_number
        
    FROM joined
)

SELECT * FROM enriched

