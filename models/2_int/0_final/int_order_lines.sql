-- =====================================================
-- INT_ORDER_LINES
-- Replacement for int_commercial using new model structure:
-- stg_value_entry → int_value_entry → int_order_lines
-- 
-- Note: int_value_entry already contains ALL joins:
-- - Sales header, Customer, Items, Inbound sales line dedup
-- =====================================================

SELECT
    -- =====================================================
    -- Primary Key (from int_value_entry)
    -- =====================================================
    ve.value_entry_id,  -- Business Central GUID - unique per value entry row
    
    -- =====================================================
    -- Core Identifiers (from int_value_entry)
    -- =====================================================
    ve.source_no_,
    ve.document_no_,
    ve.posting_date,
    ve.document_date,
    ve.invoiced_quantity,

    ve.company_source,
    ve.item_ledger_entry_no_,

    
    
    -- =====================================================
    -- Sales Channel Information (from int_value_entry)
    -- =====================================================
    ve.sales_channel,
    ve.sales_channel_sort,
    ve.transaction_type,
    ve.offline_order_channel,
    ve.source_code,
    ve.item_ledger_entry_type,
    ve.document_type,
    
    -- =====================================================
    -- Discount Analysis
    -- Using online_discount_amount already calculated in int_value_entry
    -- =====================================================
    
    -- Discount Status
    CASE 
        WHEN ve.sales_channel = 'Online' AND ve.online_discount_amount IS NOT NULL AND ve.online_discount_amount != 0 THEN 'Discounted'
        WHEN ve.sales_channel != 'Online' AND ve.offline_discount_amount IS NOT NULL AND ve.offline_discount_amount != 0 THEN 'Discounted'
        ELSE 'No Discount'
    END AS discount_status,
    
    -- Discount Indicator Flag
    CASE 
        WHEN ve.sales_channel = 'Online' AND ve.online_discount_amount IS NOT NULL AND ve.online_discount_amount != 0 THEN 1
        WHEN ve.sales_channel != 'Online' AND ve.offline_discount_amount IS NOT NULL AND ve.offline_discount_amount != 0 THEN 1
        ELSE 0
    END AS has_discount,
    
    -- Consolidated Discount Amount
    CASE 
        WHEN ve.sales_channel = 'Online' 
            THEN COALESCE(ve.online_discount_amount, 0)
        ELSE COALESCE(ve.offline_discount_amount, 0)
    END AS discount_amount,
    
    -- Separate Online/Offline Discount Tracking
    ve.offline_discount_amount,
    ve.online_discount_amount,
    
    ve.online_offer_no_,
    ve.offline_offer_no_,
    ve.offline_offer_name,

    
    -- =====================================================
    -- Gross Sales Calculation
    -- =====================================================
    ROUND(
        CASE 
            WHEN ve.transaction_type = 'Refund' THEN 
                -- For refunds: subtract discount to get original gross amount (more negative)
                ve.sales_amount__actual_ - ABS(
                    CASE 
                        WHEN ve.sales_channel = 'Online' 
                            THEN COALESCE(ve.online_discount_amount, 0)
                        ELSE COALESCE(ve.offline_discount_amount, 0)
                    END
                )
            ELSE 
                -- For sales: add discount to get original gross amount
                ve.sales_amount__actual_ + ABS(
                    CASE 
                        WHEN ve.sales_channel = 'Online' 
                            THEN COALESCE(ve.online_discount_amount, 0)
                        ELSE COALESCE(ve.offline_discount_amount, 0)
                    END
                )
        END, 2
    ) AS sales_amount_gross,
    
    -- =====================================================
    -- Financial Amounts (from int_value_entry)
    -- =====================================================
    ve.sales_amount__actual_,
    ve.cost_amount__actual_,
    
    -- =====================================================
    -- Posting Groups & Dimensions (from int_value_entry)
    -- =====================================================
    ve.gen__prod__posting_group,
    ve.gen__bus__posting_group,
    ve.source_posting_group,
    ve.inventory_posting_group,
    ve.global_dimension_1_code,
    ve.global_dimension_2_code,
    ve.dimension_code,
    ve.global_dimension_2_code_name,
    ve.clc_global_dimension_2_code_name,
    
    -- =====================================================
    -- Location Information (from int_value_entry)
    -- =====================================================
    ve.location_code,
    ve.clc_location_code,
    case 
        when ve.clc_location_code in ('REM', 'FZN') then 'Abu Dhabi'
        when ve.clc_location_code in ('RAK') then 'Ras Al Khaimah'
        else 'Dubai'
        end as location_city,
    
    -- =====================================================
    -- User & Entry Type (from int_value_entry)
    -- =====================================================
    ve.user_id,
    ve.entry_type,
    
    -- =====================================================
    -- Sales Channel Detail
    -- =====================================================
    CASE 
        WHEN ve.sales_channel IN  ('Shop','Affiliate') 
            THEN ve.location_code 
        WHEN ve.sales_channel = 'Online' 
            THEN ve.online_order_channel
        ELSE ve.clc_global_dimension_2_code_name 
    END AS sales_channel_detail,

       -- Affiliate Order Channel
    CASE 
        WHEN ve.sales_channel IN ('Affiliate') 
            THEN ve.clc_global_dimension_2_code_name 
    END AS affiliate_order_channel,
    
    -- =====================================================
    -- Unified Order, Customer & Refund IDs
    -- =====================================================
    CASE 
        WHEN ve.sales_channel = 'Online' AND ve.transaction_type = 'Sale' and ve.web_order_id is not null  THEN ve.web_order_id 
        WHEN ve.sales_channel = 'Online' AND ve.transaction_type = 'Sale' and ve.web_order_id is null then ve.document_no_   --- (76 recoud mail: SO/2025/00225)

        WHEN ve.sales_channel IN ('Shop', 'Affiliate', 'B2B', 'Service') AND ve.transaction_type = 'Sale' THEN ve.document_no_
        ELSE NULL 
    END AS unified_order_id,
    
    CASE 
        WHEN ve.transaction_type = 'Refund' 
            THEN ve.document_no_  
        ELSE NULL  
    END AS unified_refund_id,

    CASE 
      WHEN ve.std_phone_no_ = '000000000000' OR ve.std_phone_no_ IS NULL THEN ve.source_no_
      ELSE CAST(ve.std_phone_no_ AS STRING)
    END AS unified_customer_id,

    ve.loyality_member_id,
    ve.web_customer_no_,  -- Shopify customer ID for SuperApp linkage
    
    -- =====================================================
    -- Online Order Information (already in int_value_entry)
    -- =====================================================
    ve.web_order_id,
    ve.online_order_channel, -- website, Android, iOS, CRM, Unmapped
    ve.order_type,           -- EXPRESS, NORMAL, EXCHANGE
    ve.paymentgateway,       -- creditCard, cash, etc.
    ve.paymentmethodcode,    -- PREPAID, COD, creditCard
    
    -- =====================================================
    -- Customer Information (already in int_value_entry)
    -- =====================================================
    ve.customer_name,
    ve.std_phone_no_,
    ve.raw_phone_no_,
    ve.duplicate_flag,
    ve.customer_identity_status,
    
    -- =====================================================
    -- Item Information (already in int_value_entry)
    -- =====================================================
    -- Product Hierarchy (Updated Naming)
    -- =====================================================
    ve.item_no_,
    ve.item_name,
    ve.item_division,        -- Level 1: Pet (was division)
    ve.item_block,           -- Level 2: Block (was item_category)
    ve.item_category,        -- Level 3: Category (was item_subcategory)
    ve.item_subcategory,     -- Level 4: Subcategory (was item_type)
    ve.item_brand,
    -- Dynamic Sort Orders (based on revenue contribution - highest revenue = 1)
    ve.item_division_sort_order,      -- Level 1 sort
    ve.item_block_sort_order,         -- Level 2 sort
    ve.item_category_sort_order,      -- Level 3 sort (NEW)
    ve.item_subcategory_sort_order,   -- Level 4 sort (NEW)
    ve.item_brand_sort_order,         -- Level 5 sort (NEW)
    ve.brand_ownership_type,          -- Brand Ownership Type
    ve.item_purchase_type,            -- Item Purchase Type
    ve.vendor_no_,                    -- Vendor Number
    ve.vendor_posting_group,          -- Vendor Posting Group
    ve.vendor_purchase_type,          -- Vendor Purchase Type
    ve.vendor_name,                   -- Vendor Name
    
    -- =====================================================
    -- Customer Tenure Metrics (Line-Level Context)
    -- =====================================================
    
    -- Customer's first purchase date (across all transactions)
    MIN(ve.posting_date) OVER (
        PARTITION BY CASE 
            WHEN ve.std_phone_no_ = '000000000000' OR ve.std_phone_no_ IS NULL THEN ve.source_no_
            ELSE CAST(ve.std_phone_no_ AS STRING)
        END
    ) AS customer_first_purchase_date,
    
    -- Customer tenure in months at time of this transaction
    DATE_DIFF(
        ve.posting_date,
        MIN(ve.posting_date) OVER (
            PARTITION BY CASE 
                WHEN ve.std_phone_no_ = '000000000000' OR ve.std_phone_no_ IS NULL THEN ve.source_no_
                ELSE CAST(ve.std_phone_no_ AS STRING)
            END
        ),
        MONTH
    ) AS customer_tenure_months,
    
    -- =====================================================
    -- Time Period Flags
    -- =====================================================
    
    -- Current Period Flags
    CASE WHEN ve.posting_date >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND ve.posting_date <= CURRENT_DATE() THEN 1 ELSE 0 END AS is_mtd,
    CASE WHEN ve.posting_date >= DATE_TRUNC(CURRENT_DATE(), YEAR) AND ve.posting_date <= CURRENT_DATE() THEN 1 ELSE 0 END AS is_ytd,
    
    -- Last Month To Date
    CASE WHEN ve.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND ve.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AND EXTRACT(DAY FROM ve.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE()) THEN 1 ELSE 0 END AS is_lmtd,
    
    -- Last Year Month To Date
    CASE 
        WHEN ve.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
            AND ve.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
            AND EXTRACT(DAY FROM ve.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE())
            AND EXTRACT(MONTH FROM ve.posting_date) = EXTRACT(MONTH FROM CURRENT_DATE())
        THEN 1 ELSE 0 
    END AS is_lymtd,
    
    -- Last Year To Date
    CASE 
        WHEN ve.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
            AND ve.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
        THEN 1 ELSE 0 
    END AS is_lytd,
    
    -- Full Month Flags
    -- M_1: Last Month (Full)
    CASE 
        WHEN DATE_TRUNC(ve.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
        THEN 1 ELSE 0 
    END AS is_m_1,
    
    -- M_2: Two Months Ago (Full)
    CASE 
        WHEN DATE_TRUNC(ve.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
        THEN 1 ELSE 0 
    END AS is_m_2,
    
    -- M_3: Three Months Ago (Full)
    CASE 
        WHEN DATE_TRUNC(ve.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH)
        THEN 1 ELSE 0 
    END AS is_m_3,
    
    -- Year Flags
    -- Y_1: Last Year (Full)
    CASE 
        WHEN DATE_TRUNC(ve.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
        THEN 1 ELSE 0 
    END AS is_y_1,
    
    -- Y_2: Two Years Ago (Full)
    CASE 
        WHEN DATE_TRUNC(ve.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), YEAR)
        THEN 1 ELSE 0 
    END AS is_y_2


FROM {{ ref('int_value_entry') }} AS ve


WHERE 1=1
    AND ve.item_ledger_entry_type = 'Sale'
    AND ve.source_code IN ('BACKOFFICE', 'SALES')
    AND ve.document_type NOT IN ('Sales Shipment', 'Sales Return Receipt')
    AND ve.dimension_code = 'PROFITCENTER'
    
--and company_source =  'Pet Shop Services' 