-- ══════════════════════════════════════════════════════════════════════════════
-- int_purchase_line.sql
-- Purpose: Join Purchase Line + Header + GRN receipts + Variant Splits
-- GRN is the source of truth for receiving metrics
-- Variant splits are added back to original PO lines
-- ══════════════════════════════════════════════════════════════════════════════

with 

-- Aggregate variant splits by original PO line
variant_splits_agg as (
    select
        document_no_,
        original_po_line as line_no_,
        original_item_no,
        
        -- Sum all variant receipts for this original PO line
        sum(variant_qty_received) as variant_qty_received,
        sum(variant_qty_invoiced) as variant_qty_invoiced,
        
        -- Tracking
        count(*) as variant_count,
        string_agg(distinct variant_item_no, ', ' order by variant_item_no) as variant_item_codes,
        string_agg(distinct grn_no, ', ' order by grn_no) as variant_grn_numbers,
        min(receipt_date) as variant_first_receipt_date,
        max(receipt_date) as variant_last_receipt_date
        
    from {{ ref('int_variant_splits') }}
    where original_po_line is not null  -- Only matched variants
    group by document_no_, original_po_line, original_item_no
)

select

-- ══════════════════════════════════════════════════════════════════════════════
-- PURCHASE LINE (a.) 
-- ══════════════════════════════════════════════════════════════════════════════

    -- DOCUMENT IDENTIFIERS
    a.document_no_,
    a.line_no_,

    case 
        when a._fivetran_deleted = true then 'Archived'
        when a._fivetran_deleted = false then 'Active'
        else 'Unknown'
    end as po_active_status,

    case 
        when a.document_type = 0 then 'Quote'         --PQ/24/10003
        when a.document_type = 1 then 'Order'         --TPS/2025/002120
        when a.document_type = 2 then 'Invoice'       --PI/11142
        when a.document_type = 3 then 'Credit Memo'   --PCM/0563
        when a.document_type = 4 then 'Blanket Order' -- not used
        when a.document_type = 5 then 'Return Order'  --PR/2021/00001
        else 'check my logic'
    end as document_type,

    -- ITEM INFORMATION
    a.no_ as item_no,
    a.description as item_name,
    a.item_category_code,

    -- VENDOR INFORMATION
    a.buy_from_vendor_no_,
    a.pay_to_vendor_no_,

    -- DATES
    a.order_date,
    a.expected_receipt_date,

    -- QUANTITIES (from PO - for reference/comparison)
    a.quantity as qty_ordered,
    a.quantity_received as po_qty_received,      -- PO's view (may be stale)
    a.quantity_invoiced as po_qty_invoiced,      -- PO's view (may be stale)
    a.outstanding_quantity as po_qty_outstanding,
    
    -- Status Flags (from PO)
    a.completely_received as is_fully_received,
    
    -- Pending Actions
    a.qty__to_receive as qty_to_receive_next,
    a.qty__to_invoice as qty_to_invoice_next,
    
    -- Critical Financial Tracking (from PO)
    a.qty__rcd__not_invoiced as po_qty_grn_pending_invoice,
    a.over_receipt_quantity as qty_over_received,

    -- FINANCIAL
    a.currency_code,
    a.direct_unit_cost,
    a.line_amount,
    a.line_discount_amount,
    a.amount,
    a.outstanding_amount,

    -- POSTING GROUPS
    a.gen__bus__posting_group,
    a.gen__prod__posting_group,

    -- LOCATION
    a.location_code,

    -- QUALITY CONTROL
    a.quality_status,
    a.qc_done as is_qc_completed,

-- ══════════════════════════════════════════════════════════════════════════════
-- PURCHASE HEADER (b.) 
-- ══════════════════════════════════════════════════════════════════════════════

    b.status as po_header_status,
    b.document_date,
    b.vendor_invoice_no_,
    b.buy_from_vendor_name,

-- ══════════════════════════════════════════════════════════════════════════════
-- GRN DATA (grn.) - DIRECT MATCH RECEIPTS
-- ══════════════════════════════════════════════════════════════════════════════

    -- GRN-Based Receiving Metrics (Direct Match)
    coalesce(grn.grn_qty_received, 0) as grn_qty_received,
    coalesce(grn.grn_qty_invoiced, 0) as grn_qty_invoiced,
    coalesce(grn.grn_qty_pending_invoice, 0) as grn_qty_pending_invoice,
    
    -- Receipt Dates (from GRN)
    grn.first_receipt_date,
    grn.last_receipt_date,
    grn.grn_expected_receipt_date,
    
    -- GRN Document Tracking
    coalesce(grn.grn_count, 0) as grn_count,
    grn.grn_numbers,
    
    -- On-Time Delivery Metrics (from GRN)
    grn.is_on_time,
    grn.delivery_delay_days,
    
    -- Variance Detection: PO vs GRN
    grn.is_orphan_grn,
    grn.grn_discrepancy_type,

-- ══════════════════════════════════════════════════════════════════════════════
-- VARIANT SPLIT DATA (vs.) - RECEIPTS FROM SPLIT VARIANTS
-- ══════════════════════════════════════════════════════════════════════════════

    -- Variant Split Receipts
    coalesce(vs.variant_qty_received, 0) as variant_qty_received,
    coalesce(vs.variant_qty_invoiced, 0) as variant_qty_invoiced,
    coalesce(vs.variant_count, 0) as variant_count,
    vs.variant_item_codes,
    vs.variant_grn_numbers,
    vs.variant_first_receipt_date,
    vs.variant_last_receipt_date,
    
    -- Variant Split Flag
    case when vs.variant_qty_received > 0 then true else false end as has_variant_split,

-- ══════════════════════════════════════════════════════════════════════════════
-- TOTAL RECEIVING (GRN + VARIANT SPLITS)
-- ══════════════════════════════════════════════════════════════════════════════

    -- TOTAL Received = Direct GRN + Variant Splits
    coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0) as total_qty_received,
    coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0) as total_qty_invoiced,

-- ══════════════════════════════════════════════════════════════════════════════
-- CALCULATED METRICS (Using TOTAL)
-- ══════════════════════════════════════════════════════════════════════════════

    -- Outstanding Qty using TOTAL received (GRN + Variants)
    a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) as qty_outstanding,
    
    -- Variance: PO view vs True received
    case 
        when a.quantity_received != (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))
        then true else false 
    end as has_receiving_variance,
    
    a.quantity_received - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) as receiving_variance_qty,
    
    -- Delay Days (for OPEN POs only)
    case 
        when a.expected_receipt_date is not null 
            and CURRENT_DATE() > a.expected_receipt_date 
            and (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) < a.quantity
        then DATE_DIFF(CURRENT_DATE(), a.expected_receipt_date, DAY)
    end as delay_days_open,
    
    -- First/Last Receipt Date (considering both GRN and Variants)
    least(grn.first_receipt_date, vs.variant_first_receipt_date) as combined_first_receipt_date,
    greatest(grn.last_receipt_date, vs.variant_last_receipt_date) as combined_last_receipt_date

from {{ ref('stg_petshop_purchase_line') }} as a

left join {{ ref('stg_petshop_purchase_header') }} as b 
    on a.document_no_ = b.no_

left join {{ ref('int_purchase_receipts') }} as grn
    on a.document_no_ = grn.document_no_
    and a.line_no_ = grn.line_no_

left join variant_splits_agg as vs
    on a.document_no_ = vs.document_no_
    and a.line_no_ = vs.line_no_
