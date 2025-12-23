-- ══════════════════════════════════════════════════════════════════════════════
-- int_purchase_receipts.sql
-- Purpose: Aggregate GRN (Posted Purchase Receipts) data by PO + Line
-- Source of Truth for receiving metrics
-- ══════════════════════════════════════════════════════════════════════════════

with grn_lines as (
    select * from {{ ref('stg_petshop_purch_rcpt_line') }}
    where quantity > 0  -- Only actual receipts
),

-- Aggregate receipts by PO line
grn_by_po_line as (
    select
        order_no_ as document_no_,
        order_line_no_ as line_no_,
        no_ as grn_item_no,
        max(description) as grn_item_name,
        
        -- Receipt Metrics (from GRN - source of truth)
        sum(quantity) as grn_qty_received,
        sum(quantity_invoiced) as grn_qty_invoiced,
        sum(qty__rcd__not_invoiced) as grn_qty_pending_invoice,
        
        -- GRN COST & VALUE (from GRN - actual received cost)
        -- Cast to FLOAT64 in case source has STRUCT type
        safe_divide(
            sum(cast(item_charge_base_amount as float64)), 
            nullif(sum(quantity), 0)
        ) as grn_unit_cost,
        sum(cast(item_charge_base_amount as float64)) as grn_value_received,

        -- Local Currency (AED)
        safe_divide(
            sum(quantity * cast(unit_cost__lcy_ as float64)), 
            nullif(sum(quantity), 0)
        ) as grn_unit_cost_lcy,
        sum(quantity * cast(unit_cost__lcy_ as float64)) as grn_value_received_lcy,

        
        -- Receipt Dates
        min(posting_date) as first_receipt_date,
        max(posting_date) as last_receipt_date,
        min(expected_receipt_date) as grn_expected_receipt_date,
        
        -- GRN Document Tracking
        count(distinct document_no_) as grn_count,
        string_agg(distinct document_no_, ', ' order by document_no_) as grn_numbers,
        
        -- Location
        max(location_code) as grn_location_code,
        
        -- Vendor
        max(buy_from_vendor_no_) as grn_vendor_no

    from grn_lines
    group by order_no_, order_line_no_, no_
),

-- Check for orphan GRN lines (no matching PO line = variant split or post-receipt edit)
orphan_check as (
    select
        g.*,
        
        -- Check if PO line exists
        case 
            when p.document_no_ is null then true 
            else false 
        end as is_orphan_grn,
        
        -- Identify discrepancy type
        case 
            when p.document_no_ is null then 'Variant Split / No PO Line'
            when g.grn_item_no != p.no_ then 'Item Code Changed'
            else null
        end as grn_discrepancy_type,
        
        -- On-Time Delivery calculation
        case 
            when g.first_receipt_date <= g.grn_expected_receipt_date then 1
            when g.first_receipt_date > g.grn_expected_receipt_date then 0
            else null
        end as is_on_time,
        
        date_diff(g.first_receipt_date, g.grn_expected_receipt_date, day) as delivery_delay_days
        
    from grn_by_po_line g
    left join {{ ref('stg_petshop_purchase_line') }} p
        on g.document_no_ = p.document_no_
        and g.line_no_ = p.line_no_
)

select * from orphan_check

