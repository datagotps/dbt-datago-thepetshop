-- fct_procurement.sql
-- Purchase Order Fact Table with GRN + Variant Splits as Source of Truth
-- Clean fact table - all calculations and aliases are in int_purchase_line

select 

-- PURCHASE ORDER IDENTIFIERS
document_no_,
line_no_,
po_active_status,
document_type,

-- ITEM INFORMATION
item_no,
item_name,
item_category_code,

-- PRODUCT HIERARCHY (Pet → Block → Category → Sub-Category → Brand → Item)
item_division,
item_block,
item_category,
item_subcategory,
item_brand,
brand_ownership_type,              -- dim: Brand Ownership Type (Own Brand, Private Label, Other Brand)

-- VENDOR INFORMATION (simplified names from int)
vendor_no_,
vendor_name,
pay_to_vendor_no_,

-- VENDOR DIMENSION FIELDS (from int_vendor)
vendor_purchase_type,
vendor_type,
vendor_posting_group,
vendor_vat_posting_group,
vendor_country_code,
vendor_city,
vendor_is_active,
vendor_lead_time_days,
vendor_review_time_days,

-- DATES (simplified names from int)
po_date,
expected_receipt_date,

-- QUANTITIES - TOTAL (GRN + VARIANT SPLITS) AS SOURCE OF TRUTH
qty_ordered,
qty_received,
qty_invoiced,
qty_outstanding,

-- BREAKDOWN: GRN vs VARIANT SPLITS
grn_qty_received,
grn_qty_invoiced,
variant_qty_received,
variant_qty_invoiced,

-- PO quantities for comparison (may be stale after archive)
po_qty_received,
po_qty_invoiced,

-- VARIANT SPLIT TRACKING
has_variant_split,
variant_count,
variant_item_codes,
variant_grn_numbers,

-- GRN RECEIPT TRACKING (simplified names from int)
receipt_date,
receipt_complete_date,
grn_count,
grn_numbers,

-- GRN COST & VALUE (from GRN - landed cost reference)
grn_unit_cost,
grn_value_received,
grn_unit_cost_lcy,
grn_value_received_lcy,

-- INVOICE DATA (Source of Truth for Payment)
inv_qty_invoiced,
inv_unit_cost,
inv_gross_value,
inv_discount_pct,
inv_discount_amount,
inv_net_value,
inv_vat_pct,
inv_vat_amount,
inv_total_value,
inv_value_lcy,
invoice_date,
invoice_complete_date,
invoice_count,
invoice_numbers,

-- ON-TIME DELIVERY METRICS
is_on_time,
delivery_delay_days,
delay_days_open,
delivery_status,

-- VARIANCE FLAGS (PO vs True Received)
has_receiving_variance,
receiving_variance_qty,
is_orphan_grn,
grn_discrepancy_type,

-- PO MODIFICATION FLAG
is_superseded,

-- STATUS COLUMNS (calculated in int)
is_fully_received,
receiving_status,
invoice_status,

-- PO LIFECYCLE STAGE (calculated in int)
po_stage,
po_stage_sort,
po_stage_detail,
po_stage_detail_sort,

-- PENDING ACTIONS
qty_to_receive_next,
qty_to_invoice_next,

-- GRN / OVER-RECEIPT TRACKING
qty_grn_pending_invoice,
qty_over_received,

-- FINANCIALS (Line-Level)
currency_code,
direct_unit_cost,

-- PO VALUE BREAKDOWN (simplified names from int)
po_gross_value,
po_discount_pct,
po_discount_amount,
po_net_value,

-- Legacy columns (for backwards compatibility)
line_amount,
amount,
outstanding_amount,

-- POSTING GROUPS
gen__bus__posting_group,
gen__prod__posting_group,

-- LOCATION / LOGISTICS
location_code,

-- QUALITY CONTROL
quality_status,
is_qc_completed,

-- HEADER-LEVEL STATUS
po_header_status,

-- ══════════════════════════════════════════════════════════════════════════════
-- RETURN ORDER INFORMATION (from int_return_by_po)
-- ══════════════════════════════════════════════════════════════════════════════

-- Return Order Flags
has_return_order,                    -- dim: TRUE if this PO line has return orders
return_order_count,                  -- fact: Number of return orders for this PO line
return_order_numbers,                -- dim: List of return order numbers

-- Return Quantities
return_qty_ordered,                  -- fact: Total quantity ordered to return
return_qty_received,                 -- fact: Total quantity returned (received)
return_qty_invoiced,                 -- fact: Total quantity credited (invoiced)
return_qty_outstanding,              -- fact: Outstanding return quantity

-- Return Financials
return_amount,                       -- fact: Total return amount
return_outstanding_amount,           -- fact: Outstanding return amount

-- Return Status
return_status,                       -- dim: Not Yet Returned, Partially Returned, Fully Returned, Returned Pending Credit

-- Return Dates
first_return_order_date,             -- dim: Date of first return order
last_return_order_date,              -- dim: Date of last return order

-- Return Tracking
return_shipment_numbers,             -- dim: Return shipment tracking numbers
return_reason_codes,                 -- dim: Return reason codes (if available)

-- Return Rate (calculated)
CASE 
    WHEN qty_received > 0 THEN SAFE_DIVIDE(return_qty_ordered, qty_received) 
    ELSE NULL 
END as return_rate,                  -- fact: Return quantity / Received quantity

-- METADATA
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at

from {{ ref('int_purchase_line') }}

where 1=1
{{ dev_date_filter('order_date', [
    {'start': '2024-04-01', 'end': '2024-04-30'},
    {'start': '2025-03-01', 'end': '2025-03-31'},
    {'start': '2025-04-01', 'end': '2025-04-30'}
]) }}
