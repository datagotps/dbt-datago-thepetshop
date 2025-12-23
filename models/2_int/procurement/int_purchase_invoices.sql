-- ══════════════════════════════════════════════════════════════════════════════
-- int_purchase_invoices.sql
-- Purpose: Aggregate Posted Purchase Invoice data by PO + Line
-- Source of Truth for invoicing/payment metrics
-- ══════════════════════════════════════════════════════════════════════════════

with inv_lines as (
    select * from {{ ref('stg_petshop_purch_inv_line') }}
    where order_no_ is not null 
      and order_no_ != ''
      and quantity > 0  -- Only actual invoice lines
),

-- Aggregate invoices by PO line
inv_by_po_line as (
    select
        order_no_ as document_no_,
        order_line_no_ as line_no_,
        item_no as inv_item_no,
        max(description) as inv_item_name,
        
        -- Invoice Metrics (source of truth for payment)
        sum(quantity) as inv_qty_invoiced,
        
        -- Invoice Cost & Value
        -- Weighted average unit cost across multiple invoices
        safe_divide(sum(inv_net_value), nullif(sum(quantity), 0)) as inv_unit_cost,
        
        -- Gross Value (before discount)
        sum(inv_gross_value) as inv_gross_value,
        
        -- Discount
        safe_divide(sum(inv_discount_amount), nullif(sum(inv_gross_value), 0)) * 100 as inv_discount_pct,
        sum(inv_discount_amount) as inv_discount_amount,
        
        -- Net Value (after discount, before VAT) - What we pay
        sum(inv_net_value) as inv_net_value,
        
        -- VAT
        safe_divide(sum(quantity * vat_pct), nullif(sum(quantity), 0)) as inv_vat_pct,
        sum(inv_total_value - inv_net_value) as inv_vat_amount,
        
        -- Total Value (incl. VAT)
        sum(inv_total_value) as inv_total_value,
        
        -- Local Currency Value (AED)
        sum(quantity * unit_cost_lcy) as inv_value_lcy,
        
        -- Invoice Dates
        min(posting_date) as first_invoice_date,
        max(posting_date) as last_invoice_date,
        
        -- Invoice Document Tracking
        count(distinct document_no_) as invoice_count,
        string_agg(distinct document_no_, ', ' order by document_no_) as invoice_numbers,
        
        -- Location
        max(location_code) as inv_location_code,
        
        -- Vendor
        max(buy_from_vendor_no_) as inv_vendor_no

    from inv_lines
    group by order_no_, order_line_no_, item_no
),

-- Check for item code changes between PO and Invoice
item_check as (
    select
        i.*,
        
        -- Check if PO line exists
        case 
            when p.document_no_ is null then true 
            else false 
        end as is_orphan_invoice,
        
        -- Identify discrepancy type
        case 
            when p.document_no_ is null then 'Invoice Only / No PO Line'
            when i.inv_item_no != p.no_ then 'Item Code Changed'
            else null
        end as inv_discrepancy_type,
        
        -- PO values for comparison
        p.no_ as po_item_no,
        p.direct_unit_cost as po_unit_cost,
        p.line_discount__ as po_discount_pct,
        p.amount as po_net_value
        
    from inv_by_po_line i
    left join {{ ref('stg_petshop_purchase_line') }} p
        on i.document_no_ = p.document_no_
        and i.line_no_ = p.line_no_
)

select * from item_check

