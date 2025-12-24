-- ══════════════════════════════════════════════════════════════════════════════
-- int_return_by_po.sql
-- Purpose: Aggregate return orders by original Purchase Order
-- Links return orders back to their original PO lines for PO-level analysis
-- Handles both direct links (applies_to_doc__no_) and item-based matching
-- ══════════════════════════════════════════════════════════════════════════════

with 

-- Get return orders with their original PO references
return_orders_raw as (
    select
        -- Return Order Identifiers
        a.document_no_ as return_order_no,
        a.line_no_ as return_line_no,
        a.document_type,
        
        -- Link to Original PO (from line table) - handle empty strings
        nullif(trim(a.order_no_), '') as original_po_no_from_line,
        a.order_line_no_ as original_po_line_no_from_line,
        
        -- Link to Original PO (from header - applies_to_doc__no_) - handle empty strings
        nullif(trim(b.applies_to_doc__no_), '') as original_po_no_from_header,
        b.applies_to_doc__type as original_po_type_from_header,
        
        -- Return Order Details
        a.no_ as item_no,
        a.description as item_name,
        a.buy_from_vendor_no_ as vendor_no_,
        
        -- Return Quantities
        a.quantity as return_qty_ordered,
        a.quantity_received as return_qty_received,
        a.quantity_invoiced as return_qty_invoiced,
        a.outstanding_quantity as return_qty_outstanding,
        
        -- Return Financials
        a.amount as return_amount,
        a.outstanding_amount as return_outstanding_amount,
        
        -- Return Dates
        b.document_date as return_order_date,
        a.order_date as return_line_date,
        
        -- Return Shipment Tracking
        a.return_shipment_no_,
        a.return_reason_code,
        
        -- Return Status Flags
        a.return_qty__to_ship,
        a.return_qty__shipped,
        a.return_qty__shipped_not_invd_,
        
        -- Metadata
        b.status as return_order_status,
        b._fivetran_deleted
        
    from {{ ref('stg_petshop_purchase_line') }} as a
    left join {{ ref('stg_petshop_purchase_header') }} as b
        on a.document_no_ = b.no_
    where a.document_type = 5  -- Return Order
        and b._fivetran_deleted = false
),

-- Find matching POs for returns without direct links (item + vendor + date match)
-- When matching by item only, pick the PO line with highest qty_received to avoid duplicates
po_matches as (
    select
        ro.return_order_no,
        ro.return_line_no,
        po.document_no_ as matched_po_no,
        po.line_no_ as matched_po_line_no,
        row_number() over (
            partition by ro.return_order_no, ro.return_line_no 
            order by po.quantity_received desc, po_h.document_date desc, po.line_no_
        ) as match_rank
    from return_orders_raw ro
    inner join {{ ref('stg_petshop_purchase_line') }} po
        on po.document_type = 1  -- Order
        and po.no_ = ro.item_no
        and po.buy_from_vendor_no_ = ro.vendor_no_
        and po.quantity_received > 0  -- Only match to received items
    inner join {{ ref('stg_petshop_purchase_header') }} po_h
        on po.document_no_ = po_h.no_
        and po_h.document_date <= coalesce(ro.return_order_date, ro.return_line_date)
    where ro.original_po_no_from_header is null
        and ro.original_po_no_from_line is null
),

-- Combine direct links and matched links
return_orders as (
    select
        ro.*,
        -- Use direct link if available, otherwise use matched PO
        coalesce(
            ro.original_po_no_from_header,
            ro.original_po_no_from_line,
            pm.matched_po_no
        ) as original_po_no_final,
        -- Use direct line number if available, otherwise use matched line number
        -- This ensures we always have a specific line_no (even if originally 0)
        coalesce(
            case when ro.original_po_line_no_from_line > 0 then ro.original_po_line_no_from_line else null end,
            pm.matched_po_line_no
        ) as original_po_line_no_final
    from return_orders_raw ro
    left join po_matches pm
        on ro.return_order_no = pm.return_order_no
        and ro.return_line_no = pm.return_line_no
        and pm.match_rank = 1  -- Only best match
    where coalesce(
        ro.original_po_no_from_header,
        ro.original_po_no_from_line,
        pm.matched_po_no
    ) is not null  -- Must have some link to original PO
),

-- Aggregate returns by original PO line
returns_by_po_line as (
    select
        -- Original PO Identifiers
        original_po_no_final as original_po_no,
        original_po_line_no_final as original_po_line_no,
        item_no,
        vendor_no_,
        
        -- Return Order Count
        count(distinct return_order_no) as return_order_count,
        string_agg(distinct return_order_no, ', ' order by return_order_no) as return_order_numbers,
        
        -- Return Quantities (aggregated)
        sum(return_qty_ordered) as total_return_qty_ordered,
        sum(return_qty_received) as total_return_qty_received,
        sum(return_qty_invoiced) as total_return_qty_invoiced,
        sum(return_qty_outstanding) as total_return_qty_outstanding,
        
        -- Return Financials (aggregated)
        sum(return_amount) as total_return_amount,
        sum(return_outstanding_amount) as total_return_outstanding_amount,
        
        -- Return Dates
        min(return_order_date) as first_return_order_date,
        max(return_order_date) as last_return_order_date,
        
        -- Return Status
        -- Logic: Check invoiced status first (credit issued), then physical receipt status
        case 
            -- Fully credited (invoiced = ordered) - regardless of receipt status
            when sum(return_qty_invoiced) = sum(return_qty_ordered) 
                and sum(return_qty_ordered) > 0
            then 'Fully Returned'
            
            -- Nothing received and nothing invoiced
            when sum(return_qty_received) = 0 
                and sum(return_qty_invoiced) = 0
            then 'Not Yet Returned'
            
            -- Credit issued without physical receipt (credit memo scenario)
            when sum(return_qty_received) = 0 
                and sum(return_qty_invoiced) > 0
            then 'Credited, Pending Return'
            
            -- Partially received
            when sum(return_qty_received) > 0 
                and sum(return_qty_received) < sum(return_qty_ordered)
            then 'Partially Returned'
            
            -- Fully received but not fully credited
            when sum(return_qty_received) = sum(return_qty_ordered) 
                and sum(return_qty_invoiced) < sum(return_qty_ordered)
            then 'Returned, Pending Credit'
            
            -- Default fallback
            else 'In Progress'
        end as return_status,
        
        -- Return Shipment Tracking
        string_agg(distinct return_shipment_no_, ', ' order by return_shipment_no_) as return_shipment_numbers,
        
        -- Return Reasons (if available)
        string_agg(distinct return_reason_code, ', ' order by return_reason_code) as return_reason_codes,
        
        -- Flags
        case when count(distinct return_order_no) > 0 then true else false end as has_return_order
        
    from return_orders
    group by 
        original_po_no_final,
        original_po_line_no_final,
        item_no,
        vendor_no_
)

select * from returns_by_po_line
