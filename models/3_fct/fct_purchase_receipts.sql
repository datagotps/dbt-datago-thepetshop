-- ══════════════════════════════════════════════════════════════════════════════
-- fct_purchase_receipts.sql
-- GRN-Level Fact Table - Detailed receipt transactions
-- One row per GRN line (Posted Purchase Receipt)
-- ══════════════════════════════════════════════════════════════════════════════

with grn_data as (
    select * from {{ ref('stg_petshop_purch_rcpt_line') }}
    where quantity > 0  -- Only actual receipts
),

po_lines as (
    select 
        document_no_,
        line_no_,
        no_ as po_item_no,
        quantity as po_qty_ordered,
        _fivetran_deleted
    from {{ ref('stg_petshop_purchase_line') }}
),

po_headers as (
    select
        no_ as document_no_,
        status as po_header_status,
        buy_from_vendor_name
    from {{ ref('stg_petshop_purchase_header') }}
)

select
-- ══════════════════════════════════════════════════════════════════════════════
-- GRN IDENTIFIERS
-- ══════════════════════════════════════════════════════════════════════════════
g.document_no_ as grn_no,                -- dim (GRN document number)
g.order_no_ as po_no,                    -- dim (linked PO number)
g.order_line_no_ as po_line_no,          -- dim (linked PO line number)
g.line_no_ as grn_line_no,               -- dim (GRN line number)

-- ══════════════════════════════════════════════════════════════════════════════
-- ITEM INFORMATION
-- ══════════════════════════════════════════════════════════════════════════════
g.no_ as item_no,                        -- dim (item code on GRN)
g.description as item_name,              -- dim (item description)
g.item_category_code,                    -- dim (item category)
g.variant_code,                          -- dim (variant code if applicable)

-- ══════════════════════════════════════════════════════════════════════════════
-- VENDOR INFORMATION
-- ══════════════════════════════════════════════════════════════════════════════
g.buy_from_vendor_no_,                   -- dim (vendor number)
h.buy_from_vendor_name,                  -- dim (vendor name from header)
g.pay_to_vendor_no_,                     -- dim (pay-to vendor)

-- ══════════════════════════════════════════════════════════════════════════════
-- DATES
-- ══════════════════════════════════════════════════════════════════════════════
g.order_date,                            -- dim (original order date)
g.expected_receipt_date,                 -- dim (expected delivery date)
g.posting_date as receipt_date,          -- dim (actual receipt posting date)
g.promised_receipt_date,                 -- dim (promised date from vendor)
g.planned_receipt_date,                  -- dim (planned receipt date)

-- ══════════════════════════════════════════════════════════════════════════════
-- QUANTITIES
-- ══════════════════════════════════════════════════════════════════════════════
g.quantity as qty_received,              -- fact (qty received on this GRN)
g.quantity_invoiced as qty_invoiced,     -- fact (qty invoiced from this GRN)
g.qty__rcd__not_invoiced as qty_pending_invoice,  -- fact (received but not invoiced)
g.over_receipt_quantity as qty_over_received,     -- fact (over-receipt qty)
p.po_qty_ordered,                        -- fact (original ordered qty on PO line)

-- ══════════════════════════════════════════════════════════════════════════════
-- ON-TIME DELIVERY METRICS
-- ══════════════════════════════════════════════════════════════════════════════
case 
    when g.posting_date <= g.expected_receipt_date then true 
    else false 
end as is_on_time,                       -- dim (receipt on or before expected)

date_diff(g.posting_date, g.expected_receipt_date, day) as delay_days,  -- fact (negative = early)

case 
    when g.posting_date <= g.expected_receipt_date then 'On Time'
    when g.posting_date > g.expected_receipt_date then 'Late'
    else 'Unknown'
end as delivery_status,                  -- dim: On Time, Late

-- ══════════════════════════════════════════════════════════════════════════════
-- FINANCIALS
-- ══════════════════════════════════════════════════════════════════════════════
g.direct_unit_cost,                      -- fact (unit cost)
g.unit_cost,                             -- fact (unit cost alternate)
g.unit_cost__lcy_ as unit_cost_lcy,      -- fact (unit cost in local currency)

-- ══════════════════════════════════════════════════════════════════════════════
-- LOCATION / LOGISTICS
-- ══════════════════════════════════════════════════════════════════════════════
g.location_code,                         -- dim (warehouse location)
g.bin_code,                              -- dim (bin location)

-- ══════════════════════════════════════════════════════════════════════════════
-- POSTING GROUPS
-- ══════════════════════════════════════════════════════════════════════════════
g.gen__bus__posting_group,               -- dim (general business posting group)
g.gen__prod__posting_group,              -- dim (general product posting group)
g.posting_group,                         -- dim (posting group)

-- ══════════════════════════════════════════════════════════════════════════════
-- VARIANT SPLIT / ORPHAN DETECTION
-- ══════════════════════════════════════════════════════════════════════════════
case 
    when p.document_no_ is null then true 
    else false 
end as is_variant_split,                 -- dim (true if no matching PO line)

case 
    when p.document_no_ is null then 'Variant Split / Orphan'
    when g.no_ != p.po_item_no then 'Item Code Changed'
    else 'Normal'
end as receipt_type,                     -- dim: Normal, Variant Split, Item Changed

case 
    when p._fivetran_deleted = true then 'Archived'
    when p._fivetran_deleted = false then 'Active'
    when p.document_no_ is null then 'No PO Line'
    else 'Unknown'
end as po_line_status,                   -- dim (status of linked PO line)

-- ══════════════════════════════════════════════════════════════════════════════
-- LINKED PO STATUS
-- ══════════════════════════════════════════════════════════════════════════════
h.po_header_status,                      -- dim (PO header status)

-- ══════════════════════════════════════════════════════════════════════════════
-- METADATA
-- ══════════════════════════════════════════════════════════════════════════════
g._fivetran_synced as grn_synced_at,     -- dim (last sync timestamp)
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at

from grn_data g

left join po_lines p
    on g.order_no_ = p.document_no_
    and g.order_line_no_ = p.line_no_

left join po_headers h
    on g.order_no_ = h.document_no_

