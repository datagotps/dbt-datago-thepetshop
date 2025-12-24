-- ══════════════════════════════════════════════════════════════════════════════
-- int_purchase_line.sql
-- Purpose: Join Purchase Line + Header + GRN receipts + Variant Splits
-- GRN is the source of truth for receiving metrics
-- Variant splits are added back to original PO lines
-- ══════════════════════════════════════════════════════════════════════════════

with 

-- Identify PO Modification pattern: same item on same PO, earlier line with no receipts (superseded by later line)
line_item_counts as (
    select
        document_no_,
        no_ as item_no,
        line_no_,
        quantity_received,
        ROW_NUMBER() OVER (PARTITION BY document_no_, no_ ORDER BY line_no_) as item_seq,
        COUNT(*) OVER (PARTITION BY document_no_, no_) as item_line_count
    from {{ ref('stg_petshop_purchase_line') }}
),

superseded_lines as (
    select
        document_no_,
        line_no_,
        item_no,
        -- Line is superseded if: same item appears multiple times AND this is an earlier line AND no qty received
        case 
            when item_line_count > 1 
                and item_seq < item_line_count 
                and quantity_received = 0 
            then true 
            else false 
        end as is_superseded
    from line_item_counts
),

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

    -- PRODUCT HIERARCHY (Pet → Block → Category → Sub-Category → Brand)
    item.item_division,                      -- Level 1: Pet (DOG, CAT, FISH, BIRD, etc.)
    item.item_block,                         -- Level 2: Block (FOOD, ACCESSORIES, etc.)
    item.item_category,                      -- Level 3: Category (Dry Food, Wet Food, Treats, etc.)
    item.item_subcategory,                   -- Level 4: Sub-Category (item type detail)
    item.item_brand,                         -- Level 5: Brand (Royal Canin, Hills, etc.)
    item.brand_ownership_type,               -- Brand Ownership Type (Own Brand, Private Label, Other Brand)

    -- VENDOR INFORMATION (with simplified aliases)
    a.buy_from_vendor_no_ as vendor_no_,
    b.buy_from_vendor_name as vendor_name,
    a.pay_to_vendor_no_,
    
    -- VENDOR DIMENSION FIELDS (from int_vendor)
    vendor.purchase_type as vendor_purchase_type,
    vendor.vendor_type as vendor_type,
    vendor.business_posting_group as vendor_business_posting_group,
    vendor.vendor_posting_group as vendor_posting_group,
    vendor.vat_posting_group as vendor_vat_posting_group,
    vendor.country_code as vendor_country_code,
    vendor.city as vendor_city,
    vendor.currency_code as vendor_currency_code,
    vendor.is_active as vendor_is_active,
    vendor.lead_time__days_ as vendor_lead_time_days,
    vendor.review_time__days_ as vendor_review_time_days,

-- ══════════════════════════════════════════════════════════════════════════════
-- RETURN ORDER INFORMATION (aggregated by original PO)
-- ══════════════════════════════════════════════════════════════════════════════

    -- Return Order Flags
    coalesce(returns.has_return_order, false) as has_return_order,
    
    -- Return Order Count
    coalesce(returns.return_order_count, 0) as return_order_count,
    returns.return_order_numbers,
    
    -- Return Quantities
    coalesce(returns.total_return_qty_ordered, 0) as return_qty_ordered,
    coalesce(returns.total_return_qty_received, 0) as return_qty_received,
    coalesce(returns.total_return_qty_invoiced, 0) as return_qty_invoiced,
    coalesce(returns.total_return_qty_outstanding, 0) as return_qty_outstanding,
    
    -- Return Financials
    coalesce(returns.total_return_amount, 0) as return_amount,
    coalesce(returns.total_return_outstanding_amount, 0) as return_outstanding_amount,
    
    -- Return Status
    returns.return_status,
    
    -- Return Dates
    returns.first_return_order_date,
    returns.last_return_order_date,
    
    -- Return Shipment Tracking
    returns.return_shipment_numbers,
    returns.return_reason_codes,

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
    a.line_discount__ as line_discount_pct,  -- Discount percentage
    a.line_discount_amount,
    a.amount,
    a.outstanding_amount,
    
    -- GROSS/NET CALCULATION (with simplified aliases)
    a.quantity * a.direct_unit_cost as gross_value,  -- Value before discount
    a.quantity * a.direct_unit_cost as po_gross_value,  -- Value before discount (alias)
    a.line_discount__ as po_discount_pct,  -- Discount percentage (alias)
    a.line_discount_amount as po_discount_amount,  -- Discount amount (alias)
    a.amount as po_net_value,  -- Net amount after discount (alias)

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
    b.document_date as po_date,
    b.vendor_invoice_no_,
    b.buy_from_vendor_name,

-- ══════════════════════════════════════════════════════════════════════════════
-- GRN DATA (grn.) - DIRECT MATCH RECEIPTS
-- ══════════════════════════════════════════════════════════════════════════════

    -- GRN-Based Receiving Metrics (Direct Match - with simplified aliases)
    coalesce(grn.grn_qty_received, 0) as grn_qty_received,
    coalesce(grn.grn_qty_invoiced, 0) as grn_qty_invoiced,
    coalesce(grn.grn_qty_pending_invoice, 0) as grn_qty_pending_invoice,
    coalesce(grn.grn_qty_pending_invoice, 0) as qty_grn_pending_invoice,
    
    -- GRN COST & VALUE (from GRN - actual received cost)
    grn.grn_unit_cost,                           -- Weighted avg unit cost from GRN
    coalesce(grn.grn_value_received, 0) as grn_value_received,  -- Direct value from GRN
    grn.grn_unit_cost_lcy,                       -- Unit cost in local currency (AED)
    coalesce(grn.grn_value_received_lcy, 0) as grn_value_received_lcy,  -- Value in local currency
    
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

    -- TOTAL Received = Direct GRN + Variant Splits (with simplified aliases)
    coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0) as total_qty_received,
    coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0) as qty_received,
    coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0) as total_qty_invoiced,
    coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0) as qty_invoiced,

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
    
    -- First/Last Receipt Date (considering both GRN and Variants - with simplified aliases)
    least(grn.first_receipt_date, vs.variant_first_receipt_date) as combined_first_receipt_date,
    least(grn.first_receipt_date, vs.variant_first_receipt_date) as receipt_date,
    greatest(grn.last_receipt_date, vs.variant_last_receipt_date) as combined_last_receipt_date,
    greatest(grn.last_receipt_date, vs.variant_last_receipt_date) as receipt_complete_date,

-- ══════════════════════════════════════════════════════════════════════════════
-- INVOICE DATA (inv.) - POSTED PURCHASE INVOICES
-- ══════════════════════════════════════════════════════════════════════════════

    -- Invoice Metrics (source of truth for payment)
    coalesce(inv.inv_qty_invoiced, 0) as inv_qty_invoiced,
    
    -- Invoice Cost & Value
    inv.inv_unit_cost,                              -- Actual invoiced unit cost
    coalesce(inv.inv_gross_value, 0) as inv_gross_value,  -- Invoice gross (before discount)
    inv.inv_discount_pct,                           -- Invoice discount percentage
    coalesce(inv.inv_discount_amount, 0) as inv_discount_amount,  -- Invoice discount amount
    coalesce(inv.inv_net_value, 0) as inv_net_value,  -- Net payment value (after discount)
    inv.inv_vat_pct,                                -- VAT percentage
    coalesce(inv.inv_vat_amount, 0) as inv_vat_amount,  -- VAT amount
    coalesce(inv.inv_total_value, 0) as inv_total_value,  -- Total incl. VAT
    coalesce(inv.inv_value_lcy, 0) as inv_value_lcy,  -- Local currency value (AED)
    
    -- Invoice Dates (with simplified aliases)
    inv.first_invoice_date,
    inv.first_invoice_date as invoice_date,
    inv.last_invoice_date,
    inv.last_invoice_date as invoice_complete_date,
    
    -- Invoice Document Tracking
    coalesce(inv.invoice_count, 0) as invoice_count,
    inv.invoice_numbers,
    
    -- Invoice Discrepancy Detection
    inv.is_orphan_invoice,
    inv.inv_discrepancy_type,

-- ══════════════════════════════════════════════════════════════════════════════
-- PO MODIFICATION DETECTION
-- ══════════════════════════════════════════════════════════════════════════════

    -- Flag for lines superseded by PO modification (same item added on later line)
    coalesce(sup.is_superseded, false) as is_superseded,

-- ══════════════════════════════════════════════════════════════════════════════
-- CALCULATED STATUS FIELDS
-- ══════════════════════════════════════════════════════════════════════════════

    -- Delivery Status
    case 
        when least(grn.first_receipt_date, vs.variant_first_receipt_date) is null then 'Not Received'
        when grn.is_on_time = 1 then 'On Time'
        when grn.is_on_time = 0 then 'Late'
        else 'Unknown'
    end as delivery_status,

    -- Receiving Status
    case 
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) = 0 then 'Not Received'
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 
            and (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) > 0 
        then 'Partially Received'
        when (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) <= 0 
        then 'Fully Received'
        else 'Unknown'
    end as receiving_status,

    -- Invoice Status
    case 
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) = 0 then 'Not Yet Received'
        when grn.grn_qty_pending_invoice > 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) = 0 
        then 'Pending Invoice'
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) > 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) < (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
        then 'Partially Invoiced'
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) >= (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
            and (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 
        then 'Fully Invoiced'
        else 'Unknown'
    end as invoice_status,

    -- PO Stage
    case 
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) > 0 then 'Invoiced'
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 then 'Received'
        else 'Ordered'
    end as po_stage,

    -- PO Stage Sort
    case 
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) > 0 then 3
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 then 2
        else 1
    end as po_stage_sort,

    -- PO Stage Detail
    case 
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) = 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) = 0 then
            case 
                when b.status in ('Open', 'Pending Approval') then 'Draft'
                when a.expected_receipt_date < CURRENT_DATE() then 'Overdue'
                else 'Sent to Vendor'
            end
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) = 0 then
            case 
                when coalesce(a.qc_done, 0) = 0 then 'In QC'
                else 'Pending Invoice'
            end
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) > 0 then
            case 
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) >= (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                    and (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) <= 0 
                then 'Fully Invoiced'
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) >= (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                    and (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) > 0 
                then 'Partial Delivery'
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) < (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                then 'Partially Invoiced'
                else 'Invoiced'
            end
        else 'Unknown'
    end as po_stage_detail,

    -- PO Stage Detail Sort
    case 
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) = 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) = 0 then
            case 
                when b.status in ('Open', 'Pending Approval') then 1
                when a.expected_receipt_date < CURRENT_DATE() then 3
                else 2
            end
        when (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) > 0 
            and (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) = 0 then
            case 
                when coalesce(a.qc_done, 0) = 0 then 4
                else 5
            end
        when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) > 0 then
            case 
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) >= (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                    and (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) <= 0 
                then 8
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) >= (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                    and (a.quantity - (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0))) > 0 
                then 7
                when (coalesce(grn.grn_qty_invoiced, 0) + coalesce(vs.variant_qty_invoiced, 0)) < (coalesce(grn.grn_qty_received, 0) + coalesce(vs.variant_qty_received, 0)) 
                then 6
                else 6
            end
        else 99
    end as po_stage_detail_sort

from {{ ref('stg_petshop_purchase_line') }} as a

left join {{ ref('stg_petshop_purchase_header') }} as b 
    on a.document_no_ = b.no_

left join {{ ref('int_purchase_receipts') }} as grn
    on a.document_no_ = grn.document_no_
    and a.line_no_ = grn.line_no_

left join variant_splits_agg as vs
    on a.document_no_ = vs.document_no_
    and a.line_no_ = vs.line_no_

left join superseded_lines as sup
    on a.document_no_ = sup.document_no_
    and a.line_no_ = sup.line_no_

left join {{ ref('int_purchase_invoices') }} as inv
    on a.document_no_ = inv.document_no_
    and a.line_no_ = inv.line_no_

-- Product Hierarchy (Pet → Block → Category → Sub-Category → Brand → Item)
left join {{ ref('int_items') }} as item
    on a.no_ = item.item_no_

-- Vendor Dimension
left join {{ ref('int_vendor') }} as vendor
    on a.buy_from_vendor_no_ = vendor.vendor_code

-- Return Orders (aggregated by original PO)
-- Only join returns to regular Orders (document_type = 1), not to Return Orders themselves
-- Join by PO + Line if line_no > 0, or PO + Item + Line if line_no = 0 (to avoid duplicate matches)
-- When line_no = 0, match only to the first line with that item (highest qty_received)
left join {{ ref('int_return_by_po') }} as returns
    on a.document_type = 1  -- Only for regular Orders
    and a.document_no_ = returns.original_po_no
    and a.line_no_ = returns.original_po_line_no  -- Always match by specific line number (int_return_by_po assigns line_no even when originally 0)
