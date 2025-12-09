 

select 
-- Purchase Order Identifiers
document_no_,                     -- dim (PO number / key)
line_no_,                         -- dim (line number within PO)
po_active_status,                 -- dim: Archived, Active, Unknown (derived from _fivetran_deleted)
document_type,                    -- dim: Quote, Order, Invoice, Credit Memo, Blanket Order, Return Order

-- Item Information
item_no,                          -- dim (item code)
item_name,                        -- dim (item description)
item_category_code,               -- dim (item category)

-- Vendor Information
buy_from_vendor_no_,              -- dim (buy-from vendor no.)
pay_to_vendor_no_,                -- dim (pay-to vendor no.)

-- Dates
order_date,                       -- dim (order date at line level)
document_date,                    -- dim (header document date)

-- Quantities (Line-Level)
qty_ordered,                      -- fact (ordered qty)
qty_received,                     -- fact (received qty to date)
qty_invoiced,                     -- fact (invoiced qty to date)
qty_outstanding,                  -- fact (remaining qty = ordered - received)

-- Status Flags
is_fully_received,                -- dim (boolean: line completely received)

-- Pending Actions
qty_to_receive_next,              -- fact (next qty to receive)
qty_to_invoice_next,              -- fact (next qty to invoice)

-- Critical GRN / Over-Receipt Tracking
qty_grn_pending_invoice,          -- fact (qty received not yet invoiced)
qty_over_received,                -- fact (over-receipt qty)

-- Financials (Line-Level)
currency_code,                    -- dim (ISO currency)
direct_unit_cost,                 -- fact (unit cost from vendor)
line_amount,                      -- fact (extended amount = qty × cost, before discounts)
line_discount_amount,             -- fact (line discount amount)
amount,                           -- fact (net amount excl. VAT)
outstanding_amount,               -- fact (value of pending goods)

-- Posting Groups
gen__bus__posting_group,          -- dim (general business posting group)
gen__prod__posting_group,         -- dim (general product posting group)

-- Location / Logistics
location_code,                    -- dim (location / site code)

-- Quality Control
quality_status,                   -- dim (QC status text/code)
is_qc_completed,                  -- dim (boolean: QC done)

-- Header-Level Status
po_header_status,                 -- dim (header status as in ERP)


DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,

from {{ ref('int_purchase_line') }}

where 1=1
{{ dev_date_filter('order_date', [
    {'start': '2024-04-01', 'end': '2024-04-30'},
    {'start': '2025-03-01', 'end': '2025-03-31'},
    {'start': '2025-04-01', 'end': '2025-04-30'}
]) }}

--where document_no_ = 'TPS/2025/002646' and document_type = 'Order'
--where  document_no_ = 'TPS/2025/002673' -- and item_no = '100935-1'

