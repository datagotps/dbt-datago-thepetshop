-- fct_procurement.sql
-- Purchase Order Fact Table with GRN + Variant Splits as Source of Truth

select 

-- PURCHASE ORDER IDENTIFIERS
document_no_,                     -- dim (PO number / key)
line_no_,                         -- dim (line number within PO)
po_active_status,                 -- dim: Archived, Active, Unknown
document_type,                    -- dim: Quote, Order, Invoice, Credit Memo, Blanket Order, Return Order

-- ITEM INFORMATION
item_no,                          -- dim (item code)
item_name,                        -- dim (item description)
item_category_code,               -- dim (item category)

-- VENDOR INFORMATION
buy_from_vendor_no_,              -- dim (buy-from vendor no.)
buy_from_vendor_name,             -- dim (vendor display name)
pay_to_vendor_no_,                -- dim (pay-to vendor no.)

-- DATES
order_date,                       -- dim (order date at line level)
document_date,                    -- dim (header document date)
expected_receipt_date,            -- dim (expected delivery date from PO)

-- QUANTITIES - TOTAL (GRN + VARIANT SPLITS) AS SOURCE OF TRUTH
qty_ordered,                      -- fact (ordered qty from PO)
total_qty_received as qty_received,   -- fact (TOTAL received = GRN + Variants)
total_qty_invoiced as qty_invoiced,   -- fact (TOTAL invoiced = GRN + Variants)
qty_outstanding,                  -- fact (remaining qty = ordered - total_received)

-- BREAKDOWN: GRN vs VARIANT SPLITS
grn_qty_received,                 -- fact (direct GRN match)
grn_qty_invoiced,                 -- fact (direct GRN match)
variant_qty_received,             -- fact (from variant splits)
variant_qty_invoiced,             -- fact (from variant splits)

-- PO quantities for comparison (may be stale after archive)
po_qty_received,                  -- fact (PO's view of received - reference only)
po_qty_invoiced,                  -- fact (PO's view of invoiced - reference only)

-- VARIANT SPLIT TRACKING
has_variant_split,                -- dim (true if warehouse split master to variants)
variant_count,                    -- fact (number of variant codes)
variant_item_codes,               -- dim (list of variant item codes, e.g., "205448-1, 205448-2")
variant_grn_numbers,              -- dim (GRN documents for variants)

-- GRN RECEIPT TRACKING
first_receipt_date,               -- dim (first GRN posting date - direct match)
last_receipt_date,                -- dim (last GRN posting date - direct match)
combined_first_receipt_date,      -- dim (earliest receipt including variants)
combined_last_receipt_date,       -- dim (latest receipt including variants)
grn_count,                        -- fact (number of GRN documents - direct match)
grn_numbers,                      -- dim (list of GRN document numbers - direct match)

-- ON-TIME DELIVERY METRICS
is_on_time,                       -- dim (1=on time, 0=late, null=not received)
delivery_delay_days,              -- fact (days late, negative=early)
delay_days_open,                  -- fact (days overdue for open POs)

case 
    when combined_first_receipt_date is null then 'Not Received'
    when is_on_time = 1 then 'On Time'
    when is_on_time = 0 then 'Late'
    else 'Unknown'
end as delivery_status,           -- dim: Not Received, On Time, Late

-- VARIANCE FLAGS (PO vs True Received)
has_receiving_variance,           -- dim (true if PO qty != Total received)
receiving_variance_qty,           -- fact (PO received - Total received)
is_orphan_grn,                    -- dim (true if GRN has no matching PO line)
grn_discrepancy_type,             -- dim (Variant Split, Item Changed, etc.)

-- STATUS COLUMNS (Based on TOTAL received data)
is_fully_received,                -- dim (boolean from PO)

case 
    when total_qty_received = 0 then 'Not Received'
    when total_qty_received > 0 and qty_outstanding > 0 then 'Partially Received'
    when qty_outstanding <= 0 then 'Fully Received'
    else 'Unknown'
end as receiving_status,          -- dim: Not Received, Partially Received, Fully Received

case 
    when total_qty_received = 0 then 'Not Yet Received'
    when grn_qty_pending_invoice > 0 and total_qty_invoiced = 0 then 'Pending Invoice'
    when total_qty_invoiced > 0 and total_qty_invoiced < total_qty_received then 'Partially Invoiced'
    when total_qty_invoiced >= total_qty_received and total_qty_received > 0 then 'Fully Invoiced'
    else 'Unknown'
end as invoice_status,            -- dim: Not Yet Received, Pending Invoice, Partially Invoiced, Fully Invoiced

-- PENDING ACTIONS
qty_to_receive_next,              -- fact (next qty to receive from PO)
qty_to_invoice_next,              -- fact (next qty to invoice from PO)

-- GRN / OVER-RECEIPT TRACKING
grn_qty_pending_invoice as qty_grn_pending_invoice,  -- fact (qty received not yet invoiced)
qty_over_received,                -- fact (over-receipt qty)

-- FINANCIALS (Line-Level)
currency_code,                    -- dim (ISO currency)
direct_unit_cost,                 -- fact (unit cost from vendor)
line_amount,                      -- fact (extended amount = qty × cost)
line_discount_amount,             -- fact (line discount amount)
amount,                           -- fact (net amount excl. VAT)
outstanding_amount,               -- fact (value of pending goods)

-- POSTING GROUPS
gen__bus__posting_group,          -- dim (general business posting group)
gen__prod__posting_group,         -- dim (general product posting group)

-- LOCATION / LOGISTICS
location_code,                    -- dim (location / site code)

-- QUALITY CONTROL
quality_status,                   -- dim (QC status text/code)
is_qc_completed,                  -- dim (boolean: QC done)

-- HEADER-LEVEL STATUS
po_header_status,                 -- dim (header status as in ERP)

-- METADATA
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at

from {{ ref('int_purchase_line') }}

where 1=1
{{ dev_date_filter('order_date', [
    {'start': '2024-04-01', 'end': '2024-04-30'},
    {'start': '2025-03-01', 'end': '2025-03-31'},
    {'start': '2025-04-01', 'end': '2025-04-30'}
]) }}

