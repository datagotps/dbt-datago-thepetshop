select

-- PURCHASE LINE (a.) 
    -- DOCUMENT IDENTIFIERS
    a.document_no_,

        case 
        when a._fivetran_deleted = true then 'Archived'
        when a._fivetran_deleted = false then 'Active'
        else 'Unknown'
    end as po_active_status,

   --a.document_type,
    a.line_no_,

            case 
            when a.document_type = 0 then 'Quote'         --PQ/24/10003
            when a.document_type = 1 then 'Order'         --TPS/2025/002120
            when a.document_type = 2 then 'Invoice'       --PI/11142
            when a.document_type = 3 then 'Credit Memo'   --PCM/0563
            when a.document_type = 4 then 'Blanket Order' -- not used
            when a.document_type = 5 then 'Return Order'  --PR/2021/00001
            else 'cheack my logic'
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
    --a.expected_receipt_date,
    --a.promised_receipt_date,
    --a.planned_receipt_date,

    --QUANTITIES
    a.quantity as qty_ordered,
    a.quantity_received as qty_received,
    a.quantity_invoiced as qty_invoiced,
    a.outstanding_quantity as qty_outstanding,
    
    -- Status Flags
    a.completely_received as is_fully_received,
    
    -- Pending Actions
    a.qty__to_receive as qty_to_receive_next,
    a.qty__to_invoice as qty_to_invoice_next,
    
    -- Critical Financial Tracking
    a.qty__rcd__not_invoiced as qty_grn_pending_invoice,
    a.over_receipt_quantity as qty_over_received,

    -- FINANCIAL
    a.currency_code,
    a.direct_unit_cost,                 -- Unit cost from vendor
    a.line_amount,                      -- Extended amount (qty × cost)
    a.line_discount_amount,             -- Discount received
    a.amount,                           -- Net amount (excl. VAT)
    --a.amount_including_vat as amount_incl_vat,             -- Gross amount (incl. VAT)
    a.outstanding_amount,               -- Value of pending goods
    --a.a__rcd__not_inv__ex__vat__lcy_ as amount_rcd_not_inv,  -- GRN accrual amount

    -- POSTING GROUPS
    a.gen__bus__posting_group,
    a.gen__prod__posting_group,

    --LOCATION
    a.location_code,

    -- QUALITY CONTROL
    a.quality_status,
    a.qc_done as is_qc_completed,



-- PURCHASE HEADER (b.) 
    -- DOCUMENT INFO
    b.status as po_header_status,
    b.document_date,


from {{ ref('stg_petshop_purchase_line') }} as a
left join {{ ref('stg_petshop_purchase_header') }} as b 
    on a.document_no_ = b.no_

--where a.document_no_ = 'TPS/2025/000812'