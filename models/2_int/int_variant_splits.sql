-- ══════════════════════════════════════════════════════════════════════════════
-- int_variant_splits.sql
-- Purpose: Track warehouse variant splits where master code was changed to variants
-- Identifies GRN lines that reference non-existent PO lines
-- ══════════════════════════════════════════════════════════════════════════════

with orphan_grn_lines as (
    -- GRN lines that reference PO lines that don't exist
    select
        g.order_no_ as document_no_,
        g.order_line_no_ as grn_line_no,
        g.no_ as variant_item_no,
        g.description as variant_item_name,
        g.quantity as variant_qty_received,
        g.quantity_invoiced as variant_qty_invoiced,
        g.posting_date as receipt_date,
        g.document_no_ as grn_no,
        g.location_code,
        g.buy_from_vendor_no_
    from {{ ref('stg_petshop_purch_rcpt_line') }} g
    left join {{ ref('stg_petshop_purchase_line') }} p
        on g.order_no_ = p.document_no_
        and g.order_line_no_ = p.line_no_
    where p.document_no_ is null
        and g.quantity > 0
),

-- Try to match orphan lines to original master item
-- by extracting base item code (e.g., 205448-1 → 205448)
variant_mapping as (
    select
        o.*,
        
        -- Extract base item code (assumes format: XXXXXX-N)
        regexp_extract(o.variant_item_no, r'^(\d+)-') as variant_base_code,
        
        -- Find potential master item in same PO
        p.line_no_ as original_po_line,
        p.no_ as original_item_no,
        p.description as original_item_name,
        p.quantity as original_qty_ordered,
        p.quantity_received as original_qty_received,
        p.outstanding_quantity as original_qty_outstanding,
        p._fivetran_deleted as original_line_deleted
        
    from orphan_grn_lines o
    left join {{ ref('stg_petshop_purchase_line') }} p
        on o.document_no_ = p.document_no_
        and regexp_extract(p.no_, r'^(\d+)') = regexp_extract(o.variant_item_no, r'^(\d+)')
        and p.no_ != o.variant_item_no  -- Different variant/item
)

select
    -- PO Info
    document_no_,
    
    -- Original PO Line (if matched)
    original_po_line,
    original_item_no,
    original_item_name,
    original_qty_ordered,
    original_qty_received,
    original_qty_outstanding,
    original_line_deleted,
    
    -- Variant Details (from GRN)
    grn_line_no,
    variant_item_no,
    variant_item_name,
    variant_base_code,
    variant_qty_received,
    variant_qty_invoiced,
    receipt_date,
    grn_no,
    location_code,
    buy_from_vendor_no_,
    
    -- Flags
    true as is_variant_split,
    case 
        when original_po_line is not null then 'Matched to Master'
        else 'Unmatched Orphan'
    end as split_status

from variant_mapping

where document_no_ = 'TPS/2025/002646'