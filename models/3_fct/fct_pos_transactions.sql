select

-- Core Identifiers
document_no_,                      -- dim (POS document number: STORE-TERMINAL-TRANS)
line_no_,                          -- dim (line number within transaction)
effective_customer_no_ AS customer_no_, -- dim (best available customer ID: line first, then header)
customer_no_ AS line_customer_no_, -- dim (customer ID from line level)
header_customer_no_,               -- dim (customer ID from header level)
customer_id_source,                -- dim (Line, Header (Recovered), Missing)
pos_posting_date AS posting_date,  -- dim (transaction date - matches value entry posting_date)
quantity,                          -- fact (quantity sold/returned)

-- Sales Channel Information
'Pet Shop POS' AS company_source,  -- dim: source system
pos_sales_channel,                 -- dim: Shopfloor, Affiliate, Service
pos_sales_channel_sort,            -- dim: 1-3 (sort order)
pos_sales_channel_detail,          -- dim: specific partner/type (Walk-in, Retail Customer, Instashop, etc.)
transaction_type,                  -- dim: Sale, Refund
is_refund,                         -- fact: 0, 1 (flag)

-- Discount Information
CASE 
    WHEN has_discount = 1 THEN 'Discounted'
    ELSE 'No Discount'
END AS discount_status,            -- dim: Discounted, No Discount
has_discount,                      -- fact: 0, 1 (flag)
discount_amount_abs AS discount_amount, -- fact (AED amount, positive)
discount_pct,                      -- fact (discount percentage)
promotion_no_,                     -- dim (promotion number)
periodic_disc__group,              -- dim (periodic discount group)
coupon_discount,                   -- fact (coupon discount amount)

-- Financial Amounts
gross_amount,                      -- fact (AED before discount)
sales_amount,                      -- fact (AED after discount, positive)
net_amount,                        -- fact (AED original signed amount)
cost_amount_abs AS cost_amount,    -- fact (AED cost, positive)
gross_profit,                      -- fact (AED profit)
gross_margin_pct,                  -- fact (margin percentage)
vat_amount,                        -- fact (VAT amount)

-- Price Information
price,                             -- fact (unit price)
net_price,                         -- fact (net unit price)
standard_net_price,                -- fact (standard net price)

-- Location Information
store_no_,                         -- dim: DIP, FZN, REM, UMSQ, WSL, GRM, MOBILE, etc.
CASE
    WHEN store_no_ IN ('DIP', 'SZR', 'FZN', 'WSL', 'REM', 'UMSQ', 'DLM', 'DSO', 'JVC') THEN 'Dubai'
    WHEN store_no_ IN ('CRK', 'MRI', 'MAJ') THEN 'Abu Dhabi'
    WHEN store_no_ = 'RAK' THEN 'Ras Al Khaimah'
    WHEN store_no_ = 'GRM' THEN 'Dubai'
    WHEN store_no_ = 'MOBILE' THEN 'Mobile'
    ELSE 'Other'
END AS location_city,              -- dim: Dubai, Abu Dhabi, Ras Al Khaimah

-- Staff Information
line_staff_id AS staff_id,         -- dim (staff who processed line)
staff_name,                        -- dim (staff full name from master)
staff_role,                        -- dim (Manager, Cashier, Team Lead, etc.)
staff_receipt_name,                -- dim (name printed on receipt)
sales_staff,                       -- dim (sales staff)
header_staff_id,                   -- dim (staff from header)
manager_id,                        -- dim (manager ID)

-- Terminal & Shift Information
pos_terminal_no_,                  -- dim (POS terminal)
header_shift_no_ AS shift_no_,     -- dim (shift number)
header_shift_date AS shift_date,   -- dim (shift date)

-- Item Information
item_no_,                          -- dim (item code)
parent_item_no_,                   -- dim (parent item if BOM)
variant_code,                      -- dim (variant code)
barcode_no_,                       -- dim (barcode)
item_category_code,                -- dim (item category)
retail_product_code,               -- dim (retail product code)
retail_product_code_2,             -- dim (retail product description)
item_posting_group,                -- dim (item posting group)
brand_ownership_type,              -- dim: Brand Ownership Type (Own Brand, Private Label, Other Brand)
vendor_no_,                        -- dim: Vendor number (ID)
vendor_name,                       -- dim: Vendor name
vendor_posting_group,              -- dim: Vendor posting group (Local, Foreign)

-- Item & Revenue Classification
revenue_category,                  -- dim: Merchandise, Carrier Bag, Delivery Fee, Service
is_merchandise,                    -- fact: 0, 1 (1 = flows to Value Entry)
is_carrier_bag,                    -- fact: 0, 1 (POS-only, not in Value Entry)
is_delivery_fee,                   -- fact: 0, 1 (POS-only, not in Value Entry)
is_service_item,                   -- fact: 0, 1 (flag for grooming)
service_type,                      -- dim: Dog Groom, Cat Groom, Mobile Dog, etc.

-- Revenue by Category (for reconciliation with Commercial)
merchandise_net_amount,            -- fact: Revenue that matches Value Entry (signed)
carrier_bag_net_amount,            -- fact: Carrier bag revenue (POS-only, signed)
delivery_fee_net_amount,           -- fact: Delivery fee revenue (POS-only, signed)

-- Customer Information
member_card_no_,                   -- dim (loyalty card number)
customer_disc__group,              -- dim (customer discount group)
customer_identity_status,          -- dim (Anonymous/Identified - from customer master)
customer_journey_segment,          -- dim (offline_customer, online_legacy_customer, online_ofs_customer)
customer_name,                     -- dim (customer name from master)
retail_customer_group,             -- dim (customer group from master)

-- Transaction Header Totals (for reference)
header_gross_amount,               -- fact (transaction total gross)
header_net_amount,                 -- fact (transaction total net)
header_discount_amount,            -- fact (transaction total discount)
no__of_items,                      -- fact (number of items in transaction)
no__of_item_lines,                 -- fact (number of lines in transaction)

-- Refund Reference
refund_receipt_no_,                -- dim (original receipt if refund)
refunded_store_no_,                -- dim (original store if refund)
refunded_trans__no_,               -- dim (original transaction if refund)

-- Transaction Status
entry_status,                      -- dim (entry status code: 0=Not Posted, 1=Posted)
is_posted_to_value_entry,          -- fact: TRUE/FALSE (posted to Value Entry/fact_commercial)
value_entry_posting_status,        -- dim: Posted, Shopfloor - Not Posted (Issue), Affiliate - Not Posted (Issue), Service - Not in Value Entry (Expected)
sale_is_return_sale,               -- fact: 0, 1 (is return sale flag)
trans__is_mixed_sale_refund,       -- fact: 0, 1 (mixed transaction flag)

-- Time Dimensions
checkout_hour,                     -- dim: 0-23 (hour when checkout started)
day_of_week,                       -- dim: 1-7 (day of week)
day_name,                          -- dim: Monday, Tuesday, etc.
is_weekend,                        -- fact: 0, 1 (weekend flag)
year_month,                        -- dim: YYYY-MM
week_number,                       -- dim: week number

-- Date & Time Details
pos_posting_date,                  -- dim (when transaction was finalized/posted)
pos_document_date,                 -- dim (when customer started checkout - document date)
pos_posting_time,                  -- dim (time when finalized)
pos_document_time,                 -- dim (time when checkout started)
header_pos_posting_date,           -- dim (header posting date)
header_pos_posting_time,           -- dim (header posting time)
time_when_trans__closed,           -- dim (when transaction closed)

-- Receipt Information
receipt_no_,                       -- dim (receipt number)

-- Report Metadata
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,

-- System Fields
line_fivetran_synced,
line_fivetran_deleted

FROM {{ ref('int_pos_trans_details') }}

-- Exclude deleted records
WHERE COALESCE(line_fivetran_deleted, FALSE) = FALSE

{{ dev_date_filter('pos_posting_date') }}
