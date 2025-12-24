-- =====================================================
-- CTE: Deduplicate Inbound Sales Lines
-- =====================================================
WITH inbound_sales_line_dedup AS (
    SELECT 
        a.documentno,
        a.item_no_,
        count(*),
        MAX(a.discount_amount) AS discount_amount,
        MAX(b.couponcode) AS couponcode,
        MAX(inserted_on) AS inserted_on,
    FROM {{ ref('stg_erp_inbound_sales_line') }} as a
    left join {{ ref('stg_ofs_inboundpaymentline') }} as b on a.item_id =b.itemid and b.isheader = 0 
    GROUP BY 
        a.documentno, 
        a.item_no_
) 

-- =====================================================
-- Main Query: Sales Analysis with Categorizations
-- =====================================================
SELECT
    -- Source Information
    ve.document_no_,
    ve.source_code,
    ve.company_source,
    ve.item_ledger_entry_no_,
    
    -- =====================================================
    -- Sales Channel Categorization
    -- =====================================================
    CASE 
        
        WHEN clc_global_dimension_2_code_name IN ('Online') THEN 'Online' 
        WHEN clc_global_dimension_2_code_name IN ('POS Sale') THEN 'Shop'
        WHEN clc_global_dimension_2_code_name IN ('Now Now','Amazon', 'Amazon FBA', 'Amazon DFS', 'Swan','Instashop', 'El Grocer', 'Careem', 'Noon','Deliveroo', 'Talabat', 'BlazeApp', 'Trendyol' ) THEN 'Affiliate'
        WHEN clc_global_dimension_2_code_name IN ('B2B Sales') THEN 'B2B'
        WHEN clc_global_dimension_2_code_name IN ('P&M', 'Pet Relocation','Cleaning & Potty', 'PETRELOC', 'Grooming','Mobile Grooming', 'Shop Grooming' ) THEN 'Service'
        -- Handle NULL name with PETGROOM code (legacy grooming vouchers from Jan-Mar 2022)
        WHEN clc_global_dimension_2_code_name IS NULL AND ve.global_dimension_2_code = 'PETGROOM' THEN 'Service'
        ELSE 'Check My Logic'
    END AS sales_channel,  

    CASE 
        WHEN clc_global_dimension_2_code_name IN ('Online') THEN 1 
        WHEN clc_global_dimension_2_code_name IN ('POS Sale') THEN 2
        WHEN clc_global_dimension_2_code_name IN ('Amazon', 'Amazon FBA', 'Amazon DFS', 'Swan','Instashop', 'El Grocer', 'Careem', 'Noon','Deliveroo', 'Talabat', 'BlazeApp', 'Trendyol' ) THEN 3
        WHEN clc_global_dimension_2_code_name IN ('B2B Sales') THEN 4
        WHEN clc_global_dimension_2_code_name IN ('P&M', 'Pet Relocation','Cleaning & Potty', 'PETRELOC', 'Grooming','Mobile Grooming', 'Shop Grooming' ) THEN 5
        ELSE 6
    END AS sales_channel_sort,  

    -- =====================================================
    -- Entry Type Classifications
    -- =====================================================
    
    -- Item Ledger Entry Type
    CASE 
        WHEN ve.item_ledger_entry_type = 0 THEN 'Purchase'
        WHEN ve.item_ledger_entry_type = 1 THEN 'Sale'
        WHEN ve.item_ledger_entry_type = 2 THEN 'Positive Adjmt.'
        WHEN ve.item_ledger_entry_type = 3 THEN 'Negative Adjmt.'
        WHEN ve.item_ledger_entry_type = 4 THEN 'Transfer'
        ELSE 'Check My Logic'
    END AS item_ledger_entry_type,

    -- =====================================================
    -- Item Information
    -- =====================================================
    -- Product Hierarchy (Updated Naming)
    -- =====================================================
    it.item_no_,
    it.item_name,
    it.item_division,        -- Level 1: Pet (was division)
    it.item_block,           -- Level 2: Block (was item_category)
    it.item_category,        -- Level 3: Category (was item_subcategory)
    it.item_subcategory,     -- Level 4: Subcategory (was item_type)
    it.item_brand,
    -- Dynamic Sort Orders (based on revenue contribution)
    it.item_division_sort_order,      -- Level 1 sort
    it.item_block_sort_order,         -- Level 2 sort
    it.item_category_sort_order,      -- Level 3 sort (NEW)
    it.item_subcategory_sort_order,   -- Level 4 sort (NEW)
    it.item_brand_sort_order,         -- Level 5 sort (NEW)
    -- Brand Ownership Type
    it.brand_ownership_type,

    -- =====================================================
    -- Document Classifications
    -- =====================================================
    
    -- Document Type
    CASE 
        WHEN ve.document_type = 0 THEN '----'
        WHEN ve.document_type = 1 THEN 'Sales Shipment'
        WHEN ve.document_type = 2 THEN 'Sales Invoice'
        WHEN ve.document_type = 3 THEN 'Sales Return Receipt'
        WHEN ve.document_type = 4 THEN 'Sales Credit Memo'   --Refund
        WHEN ve.document_type = 5 THEN 'Purchase Receipt'
        WHEN ve.document_type = 6 THEN 'Purchase Invoice'
        WHEN ve.document_type = 7 THEN 'Purchase Return Shipment'
        WHEN ve.document_type = 8 THEN 'Purchase Credit Memo'
        WHEN ve.document_type = 9 THEN 'Transfer Shipment'
        WHEN ve.document_type = 10 THEN 'Transfer Receipt'
        ELSE 'Check My Logic'
    END AS document_type,

    -- Transaction Type
    CASE 
        -- Handle document_type = 0 with amount check
        WHEN ve.document_type = 0 AND ve.sales_amount__actual_ >= 0 
            THEN 'Sale'
        WHEN ve.document_type = 0 AND ve.sales_amount__actual_ < 0 
            THEN 'Refund'
        -- Standard document type classifications
        WHEN ve.document_type = 2 
            THEN 'Sale'
        WHEN ve.document_type = 4 
            THEN 'Refund'
        -- Everything else
        ELSE 'Other'
    END AS transaction_type,

    -- =====================================================
    -- Location Information
    -- =====================================================
    
    -- Calculated Location Code
    CASE
        WHEN clc_global_dimension_2_code_name IN ('Amazon DFS', 'Amazon') 
            THEN 'DIP'
        WHEN clc_global_dimension_2_code_name = 'Pet Relocation' 
            THEN 'PRL'
        ELSE ve.location_code
    END AS clc_location_code,

    -- Original Location Code
    ve.location_code,

    -- Offline Order Channel
    CASE 
        WHEN clc_global_dimension_2_code_name IN ('POS Sale') 
            THEN ve.location_code 
    END AS offline_order_channel,

    -- =====================================================
    -- Additional Type Classifications
    -- =====================================================
    
    -- Entry Type
    CASE 
        WHEN ve.entry_type = 0 THEN 'Direct Cost'  
        WHEN ve.entry_type = 1 THEN 'Revaluation'
        WHEN ve.entry_type = 2 THEN 'Rounding'
        ELSE 'Check My Logic'
    END AS entry_type,  

    -- Source Type
    CASE 
        WHEN ve.source_type = 0 THEN '----'  
        WHEN ve.source_type = 1 THEN 'Customer'
        WHEN ve.source_type = 2 THEN 'Vendor'
        WHEN ve.source_type = 37 THEN '37'
        WHEN ve.source_type = 39 THEN '39'
        WHEN ve.source_type = 5741 THEN '5741'
        ELSE 'Check My Logic'
    END AS source_type,     



    -- Type
    CASE 
        WHEN ve.type = 2 THEN '----'  
        WHEN ve.type = 0 THEN 'Work Center'
        ELSE 'Check My Logic'
    END AS type,


    
    ve.source_no_,
    ve.posting_date,
    ve.document_date,
    ve.invoiced_quantity,
    ve.document_line_no_,
    ve.dimension_code,
    ve.sales_amount__actual_,
    ve.cost_amount__actual_,

    ve.gen__prod__posting_group,
    ve.gen__bus__posting_group,
    ve.source_posting_group,
    ve.inventory_posting_group,
    ve.global_dimension_1_code,
    ve.global_dimension_2_code,
    
    ve.global_dimension_2_code_name,
    ve.clc_global_dimension_2_code_name,
    ve.user_id,



ish.web_order_id,
ish.online_order_channel, --website, Android, iOS, CRM, Unmapped
ish.order_type, --EXPRESS, NORMAL, EXCHANGE
ish.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
ish.paymentmethodcode, -- PREPAID, COD, creditCard


cu.name as customer_name,
cu.raw_phone_no_,
cu.std_phone_no_,
cu.customer_identity_status,
cu.duplicate_flag,
cu.loyality_member_id,
cu.web_customer_no_,  -- Shopify customer ID for SuperApp linkage

ROUND(COALESCE(-1*isl.discount_amount,0) / (1 + 5 / 100), 2) as online_discount_amount,


ve.discount_amount as offline_discount_amount,


isl.couponcode as online_offer_no_,
isl.inserted_on,


dic.offer_no_ as offline_offer_no_,
dic.offline_offer_name,

FROM {{ ref('stg_value_entry') }} AS ve
    LEFT JOIN {{ ref('int_inbound_sales_header') }} AS ish ON ve.document_no_ = ish.documentno and ve.company_source = 'Pet Shop'
    LEFT JOIN {{ ref('int_erp_customer') }} AS cu ON ve.source_no_ = cu.no_ and ve.company_source = 'Pet Shop'
    LEFT JOIN {{ ref('int_items') }} AS it ON it.item_no_ = ve.item_no_
    LEFT JOIN inbound_sales_line_dedup AS isl ON ve.document_no_ = isl.documentno AND ve.item_no_ = isl.item_no_ and ve.company_source = 'Pet Shop'

  -- LEFT JOIN {{ ref('stg_value_entry_2') }} AS ve2 on ve.entry_no_ = ve2.entry_no_ and ve.company_source = 'Pet Shop'

   left join {{ ref('int_discount_ledger_entry') }} AS dic on ve.item_ledger_entry_no_ = dic.item_ledger_entry_no_

--where ve.entry_no_ = 22954217

--where document_no_ = 'DIP-DT08-48383'


-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse1 ON ve.dimension_set_id = dse1.dimension_set_id AND dse1.global_dimension_no_ = 1 -- <STORE>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse2 ON ve.dimension_set_id = dse2.dimension_set_id AND dse2.global_dimension_no_ = 2 -- <PROFITCENTER>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse3 ON ve.dimension_set_id = dse3.dimension_set_id AND dse3.global_dimension_no_ = 3 -- <PRODUCTGROUP>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse4 ON ve.dimension_set_id = dse4.dimension_set_id AND dse4.global_dimension_no_ = 4 -- <RESOURCE>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse5 ON ve.dimension_set_id = dse5.dimension_set_id AND dse5.global_dimension_no_ = 5 -- <VEHICLE>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse6 ON ve.dimension_set_id = dse6.dimension_set_id AND dse6.global_dimension_no_ = 6 -- <COSTCENTER>
-- LEFT JOIN {{ ref('int_dimension_set_entry') }} AS dse7 ON ve.dimension_set_id = dse7.dimension_set_id AND dse7.global_dimension_no_ = 7 -- <PROJECT>



    -- =====================================================
    -- Commented Dimension Fields (for future use)
    -- =====================================================
    -- dse1.name AS store,           -- Global Dimension 1
    -- dse2.name AS profit_center,   -- Global Dimension 2
    -- dse3.name AS product_group,   -- Global Dimension 3
    -- dse4.name AS resource,        -- Global Dimension 4
    -- dse5.name AS vehicle,         -- Global Dimension 5
    -- dse6.name AS costcenter,      -- Global Dimension 6
    -- dse7.name AS project,         -- Global Dimension 7

--where document_no_ = 'ALQ-ALQ01-147'