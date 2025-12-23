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
item_category_code,               -- dim (item category - legacy)

-- PRODUCT HIERARCHY (Pet → Block → Category → Sub-Category → Brand → Item)
item_division,                    -- dim Level 1: Pet (DOG, CAT, FISH, BIRD, REPTILE, SMALL PET)
item_block,                       -- dim Level 2: Block (FOOD, ACCESSORIES, etc.)
item_category,                    -- dim Level 3: Category (Dry Food, Wet Food, Treats, etc.)
item_subcategory,                 -- dim Level 4: Sub-Category (item type detail)
item_brand,                       -- dim Level 5: Brand (Royal Canin, Hills, etc.)

-- VENDOR INFORMATION
buy_from_vendor_no_,              -- dim (buy-from vendor no.)
buy_from_vendor_name,             -- dim (vendor display name)
pay_to_vendor_no_,                -- dim (pay-to vendor no.)

-- DATES (Simplified)
document_date as po_date,                    -- dim: PO creation date (header level)
expected_receipt_date,                       -- dim: Expected delivery date from PO
-- order_date,                               -- COMMENTED: line-level date (use po_date instead)

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

-- GRN RECEIPT TRACKING (Simplified)
combined_first_receipt_date as receipt_date,         -- dim: First goods arrival (use for lead time)
combined_last_receipt_date as receipt_complete_date, -- dim: Last goods arrival (use for fully received)
grn_count,                                           -- fact (number of GRN documents)
grn_numbers,                                         -- dim (list of GRN document numbers)
-- first_receipt_date,                               -- COMMENTED: direct match only (use receipt_date)
-- last_receipt_date,                                -- COMMENTED: direct match only (use receipt_complete_date)

-- GRN COST & VALUE (from GRN - landed cost reference)
grn_unit_cost,                    -- fact (weighted avg unit cost from GRN receipts)
grn_value_received,               -- fact (total value received from GRN - may include freight allocation)
grn_unit_cost_lcy,                -- fact (unit cost in local currency AED)
grn_value_received_lcy,           -- fact (value received in local currency AED)

-- INVOICE DATA (Source of Truth for Payment)
inv_qty_invoiced,                 -- fact (qty invoiced from Posted Invoice - source of truth)
inv_unit_cost,                    -- fact (actual invoiced unit cost)
inv_gross_value,                  -- fact (invoice gross before discount)
inv_discount_pct,                 -- fact (invoice discount %)
inv_discount_amount,              -- fact (invoice discount amount)
inv_net_value,                    -- fact (NET payment value = what we pay vendor)
inv_vat_pct,                      -- fact (VAT percentage)
inv_vat_amount,                   -- fact (VAT amount)
inv_total_value,                  -- fact (total incl. VAT)
inv_value_lcy,                                       -- fact (invoice value in local currency AED)
first_invoice_date as invoice_date,                  -- dim: First invoice posting date
last_invoice_date as invoice_complete_date,          -- dim: Last invoice posting date (fully invoiced)
invoice_count,                                       -- fact (number of invoices)
invoice_numbers,                                     -- dim (list of invoice document numbers)

-- ON-TIME DELIVERY METRICS
is_on_time,                       -- dim (1=on time, 0=late, null=not received)
delivery_delay_days,              -- fact (days late, negative=early)
delay_days_open,                  -- fact (days overdue for open POs)

case 
    when combined_first_receipt_date is null then 'Not Received'
    when is_on_time = 1 then 'On Time'
    when is_on_time = 0 then 'Late'
    else 'Unknown'
end as delivery_status,           -- dim: Not Received, On Time, Late (uses receipt_date logic)

-- VARIANCE FLAGS (PO vs True Received)
has_receiving_variance,           -- dim (true if PO qty != Total received)
receiving_variance_qty,           -- fact (PO received - Total received)
is_orphan_grn,                    -- dim (true if GRN has no matching PO line)
grn_discrepancy_type,             -- dim (Variant Split, Item Changed, etc.)

-- PO MODIFICATION FLAG
is_superseded,                    -- dim (true if line was superseded by PO modification - same item on later line)

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

-- ══════════════════════════════════════════════════════════════════════════════
-- PO LIFECYCLE STAGES (Main 4 stages for dashboard tracking)
-- Stage is based on TEAM ACTIVITY, not fill rate
-- ══════════════════════════════════════════════════════════════════════════════
case 
    -- Stage 1: DRAFT - PO in planning/approval phase
    when po_header_status in ('Open', 'Pending Approval') then '1. Draft'
    
    -- Stage 2: AWAITING SUPPLIER - Released, waiting for vendor delivery
    when po_header_status = 'Released' and total_qty_received = 0 then '2. Awaiting Supplier'
    
    -- Stage 3: IN RECEIVING - Goods arrived, team actively processing (QC not done)
    -- This is a small number of POs at any time - team working on them NOW
    when total_qty_received > 0 and coalesce(is_qc_completed, 0) = 0 then '3. In Receiving'
    
    -- Stage 4: RECEIVED - Team finished work, goods in stock (QC complete)
    -- Regardless of partial/full - once QC is done, it's in warehouse
    when total_qty_received > 0 and coalesce(is_qc_completed, 1) = 1 then '4. Received'
    
    else 'Unknown'
end as po_stage,                  -- dim: 1. Draft, 2. Awaiting Supplier, 3. In Receiving, 4. Received

-- PO LIFECYCLE DETAIL (Sub-statuses for granular tracking)
case 
    -- Stage 1: DRAFT
    when po_header_status = 'Open' then 'Draft'
    when po_header_status = 'Pending Approval' then 'Pending Approval'
    
    -- Stage 2: AWAITING SUPPLIER
    when po_header_status = 'Released' 
         and total_qty_received = 0 
         and expected_receipt_date < CURRENT_DATE() 
    then 'Overdue'
    
    when po_header_status = 'Released' and total_qty_received = 0 then 'Sent to Vendor'
    
    -- Stage 3: IN RECEIVING (team working on it - QC not done)
    when total_qty_received > 0 and coalesce(is_qc_completed, 0) = 0 then 'Processing'
    
    -- Stage 4: RECEIVED (team finished - QC done, in stock)
    when total_qty_received > 0 and coalesce(is_qc_completed, 1) = 1 and qty_outstanding <= 0 then 'Complete'
    when total_qty_received > 0 and coalesce(is_qc_completed, 1) = 1 and qty_outstanding > 0 then 'Partial - In Stock'
    
    else 'Unknown'
end as po_stage_detail,           -- dim: Draft, Pending Approval, Sent to Vendor, Overdue, Processing, Partial - In Stock, Complete

-- PENDING ACTIONS
qty_to_receive_next,              -- fact (next qty to receive from PO)
qty_to_invoice_next,              -- fact (next qty to invoice from PO)

-- GRN / OVER-RECEIPT TRACKING
grn_qty_pending_invoice as qty_grn_pending_invoice,  -- fact (qty received not yet invoiced)
qty_over_received,                -- fact (over-receipt qty)

-- FINANCIALS (Line-Level)
currency_code,                    -- dim (ISO currency)
direct_unit_cost,                 -- fact (unit cost from vendor)

-- PO VALUE BREAKDOWN (Gross → Discount → Net)
gross_value as po_gross_value,    -- fact: qty × unit cost (BEFORE discount)
line_discount_pct as po_discount_pct,      -- fact: discount percentage (e.g., 20)
line_discount_amount as po_discount_amount, -- fact: discount amount in currency
amount as po_net_value,           -- fact: net amount (AFTER discount) = gross - discount

-- Legacy columns (same as above, for backwards compatibility)
line_amount,                      -- fact (same as po_net_value)
amount,                           -- fact (same as po_net_value)
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

