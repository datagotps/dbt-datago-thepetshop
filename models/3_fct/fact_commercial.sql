select

-- Core Identifiers
source_no_,                        -- dim (customer ID)
document_no_,                      -- dim (ERP document number)
posting_date,                      -- dim (date)
invoiced_quantity,                 -- fact (quantity)
item_ledger_entry_no_,

-- Sales Channel Information
company_source,                    --Petshop,pethaus
sales_channel,                     -- dim: Online, Shop, Affiliate, B2B, Service
sales_channel_sort,                -- dim: 1-6 (sort order)
transaction_type,                  -- dim: Sale, Refund, Other
offline_order_channel,             -- dim: store code when POS Sale
source_code,                       -- dim: BACKOFFICE, SALES
item_ledger_entry_type,            -- dim: Sale (filtered)
document_type,                     -- dim: Sales Invoice, Sales Credit Memo

-- Discount Information
discount_status,                   -- dim: Discounted, No Discount
has_discount,                      -- fact: 0, 1 (flag)
discount_amount,                   -- fact (AED amount)
offline_discount_amount,           -- fact (AED for offline)
online_discount_amount,            -- fact (AED for online)
online_offer_no_,                  -- dim (online promo code)
offline_offer_no_,                 -- dim (offline promo code)
offline_offer_name,

-- Financial Amounts
sales_amount_gross,                -- fact (AED before discount)
sales_amount__actual_,             -- fact (AED after discount)
cost_amount__actual_,              -- fact (AED cost)

-- Posting Groups & Dimensions
gen__prod__posting_group,          -- dim (general product posting)
gen__bus__posting_group,           -- dim (general business posting)
source_posting_group,              -- dim (source posting)
inventory_posting_group,           -- dim (inventory posting)
global_dimension_1_code,           -- dim (dimension 1 code)
global_dimension_2_code,           -- dim (dimension 2 code)
dimension_code,                    -- dim: PROFITCENTER (filtered)
global_dimension_2_code_name,     -- dim (dimension 2 name)
clc_global_dimension_2_code_name, -- dim (calculated dimension 2)

-- Location Information
location_code,                     -- dim: DIP, FZN, REM, UMSQ, WSL, etc
clc_location_code,                 -- dim (calculated location)
location_city,                     -- dim: Dubai, Abu Dhabi, Ras Al Khaimah

-- User & Entry
user_id,                           -- dim (user who created)
entry_type,                        -- dim: Direct Cost, Revaluation, Rounding

-- Sales Channel Detail
sales_channel_detail,              -- dim (store/platform/channel name)
affiliate_order_channel,

-- Unified IDs
unified_order_id,                  -- dim (web order or doc number)
unified_refund_id,                 -- dim (refund doc number)
unified_customer_id,               -- dim (phone or source_no_)
loyality_member_id,                -- dim (loyalty ID or null)

-- Online Order Information
web_order_id,                      -- dim (web order number)
online_order_channel,              -- dim: website, Android, iOS, CRM, Unmapped
order_type,                        -- dim: EXPRESS, NORMAL, EXCHANGE
paymentgateway,                    -- dim: creditCard, cash, COD, Tabby, etc
paymentmethodcode,                 -- dim: PREPAID, COD

-- Customer Information
customer_name,                     -- dim (text)
std_phone_no_,                     -- dim (standardized phone)
raw_phone_no_,                     -- dim (original phone)
duplicate_flag,                    -- dim: Yes, No
customer_identity_status,          -- dim: Verified, Unverified

-- Item Information
item_no_,                          -- dim (item code)
item_name,                         -- dim (item description)
item_category,                     -- dim (category name)
item_subcategory,                  -- dim (subcategory name)
item_type,
item_brand,                        -- dim (brand name)
division,                          -- dim (division name)
division_sort_order,
item_category_sort_order,

-- Time Period Flags
is_mtd,                            -- fact: 0, 1 (month-to-date flag)
is_ytd,                            -- fact: 0, 1 (year-to-date flag)
is_lmtd,                           -- fact: 0, 1 (last month-to-date)
is_lymtd,                          -- fact: 0, 1 (last year month-to-date)
is_lytd,                           -- fact: 0, 1 (last year-to-date)
is_m_1,                            -- fact: 0, 1 (last month full)
is_m_2,                            -- fact: 0, 1 (2 months ago full)
is_m_3,                            -- fact: 0, 1 (3 months ago full)
is_y_1,                            -- fact: 0, 1 (last year full)
is_y_2,                            -- fact: 0, 1 (2 years ago full)


DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at, 


null as test_1,
null as test_2,
null as test_3,

null as test_4,


FROM {{ ref('int_commercial') }}

WHERE document_no_ NOT IN ('PSI/2021/01307', 'PSI/2023/00937')
  AND (company_source != 'Pet Shop' 
       OR clc_global_dimension_2_code_name NOT IN ('Mobile Grooming','Shop Grooming'))
  AND document_no_ != 'INV00528612' --Future Order Date (Dec 9, 2025)

{{ dev_date_filter('posting_date') }}

--and document_no_ = 'DIP-DT08-48383'
