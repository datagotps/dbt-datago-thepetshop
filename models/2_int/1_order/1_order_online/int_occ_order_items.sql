-- 1. SOURCE DATA CTEs
with 
-- Order line data 
inboundpaymentline as (
    select
        itemid,
        mrpprice as gross_subtotal,
        COALESCE(mrpprice,0) / (1 + TaxPercentage / 100) as gross_subtotal_exclu_tax,
        COALESCE(discount,0) / (1 + TaxPercentage / 100) as discount_exclu_tax,
        amountincltax as net_subtotal,
        amount as net_subtotal_exclu_tax,
        discount,
        insertedon
    from {{ ref('stg_ofs_inboundpaymentline') }}
    where isheader = 0
),

-- Order header data
order_head as (
    select
        weborderno,
        sum(case when isheader = 1 then shippingcharges else 0 end) as shipping,
        sum(case when isheader = 1 then amountincltax else 0 end) as order_value
    from {{ ref('stg_ofs_inboundpaymentline') }} 
    group by 1
),

-- Order status and fulfillment data
orderdataanalysis as (
    select
        itemid,
        orderdate,
        deliverydate,
        paymentmethodcode,
        batchdatetime,
        pickeddatetime,
        boxid,
        boxdatetime,
        ordertype,
        case when batchid is null or batchid = 0 then 0 else 1 end as batchcreated,
        allocated,
        picked,
        case when awbno is null or awbno = '' then 0 else 1 end as packed,
        isdelivered,
        case when returnticket is null or returnticket = '' then 0 else 1 end as returned,
        iscancelled,
        awbno
    from {{ ref('stg_ofs_orderdataanalysis') }}
),

-- Box and packing data
boxstatus as (
    select 
        boxid,
        insertedon as packdatetime
    from {{ ref('stg_ofs_boxstatus') }}
),

-- CRM status data
ofs_crmlinestatus as (
    select 
        itemid,
        statusname
    from {{ ref('stg_ofs_crmlinestatus') }}
),

-- Product data (deduplicated)
ofs_itemdetail as (
    with deduplicated as (
        select 
            *,
            ROW_NUMBER() over (PARTITION BY itemno ORDER BY itemno) AS rn
        from {{ ref('stg_ofs_itemdetail') }}
    )
    select * except(rn) from deduplicated where rn = 1
),

-- ERP invoice data
erp_posted_sales_invoice_line as (
    select
        document_no_,
        sum(amount) as posted_amount,
        sum(amount_including_vat) as posted_amount_including_vat,
        count(*) as invoice_item_count,
        sum(case when gen__prod__posting_group = 'SHIPPING' then amount else 0 end) as shipping_amount_exlu_tax,
        sum(case when gen__prod__posting_group = 'SHIPPING' then amount_including_vat else 0 end) as shipping_amount_inclu_tax
    from {{ ref('stg_erp_sales_invoice_line') }} 
    group by 1
)

-- 2. FINAL RESULT - COMBINING DATA
select
    -- Order identifiers
    a.weborderno,
    a.itemid,
    a.itemno,
    a.sku,
    a.quantity,
    
    -- Product details
    e.description as item_name,
    e.divisioncodedescription as item_division,      -- Level 1: Pet (DOG, CAT, FISH, etc.)
    e.itemcategorycode as item_block,                -- Level 2: Block (FOOD, ACCESSORIES, etc.)
    e.retailproductcode as item_category,            -- Level 3: Category (Dry Food, Wet Food, etc.)
    e.brand as item_brand,                           -- Level 5: Brand
    
    -- Order details
    h.orderplatform,
    h.ordersource,
    h.online_order_channel,
    h.customerid,
    g.location,
    c.ordertype,
    f.paymentmethodcode,
    i.statusname as crm_order_line_status,
    
    -- Revenue data
    b.gross_subtotal,
    b.gross_subtotal_exclu_tax,
    b.net_subtotal,
    b.net_subtotal_exclu_tax,
    b.discount,
    b.discount_exclu_tax,
    
    
    -- Fulfillment flags
    f.iscancelled,
    f.isdelivered,
    f.returned,
    f.batchcreated,
    f.picked,
    f.packed,
    f.awbno,
    
    -- Fulfillment dates
    b.insertedon, -- to be removed later
    h.order_date, -- to be removed later

    h.order_date as shopify_order_datetime, -- Original Order Date

    b.insertedon as ofs_order_datetime,
    date(b.insertedon) as ofs_order_date,
    f.batchdatetime,
    f.pickeddatetime,
    m.packdatetime,
    f.deliverydate,
    DATETIME_DIFF(b.insertedon, h.order_date, MINUTE) as order_sync_minutes,

    CASE 
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, MINUTE) < 10 THEN '< 10 mins'
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, MINUTE) <= 60 THEN '10-60 mins'
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, HOUR) <= 10 THEN '1-10 hours'
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, HOUR) <= 24 THEN '10-24 hours'
    ELSE '> 24 hours'
    END as order_sync_category,

    CASE 
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, MINUTE) < 10 THEN 1
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, MINUTE) <= 60 THEN 2
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, HOUR) <= 10 THEN 3
    WHEN DATETIME_DIFF(b.insertedon, h.order_date, HOUR) <= 24 THEN 4
    ELSE 5
END as order_sync_category_sort,

    
    -- Revenue calculations and classifications
    -- Fixed: Added k.item_id IS NULL to prevent double counting with recognized_revenue
    -- Items that are Posted in ERP should NOT be counted as unfulfilled, even if OFS isdelivered=false
    case 
        when f.isdelivered is not true AND k.item_id IS NULL then net_subtotal_exclu_tax 
        when f.returned = 1 and f.iscancelled is true AND k.item_id IS NULL then net_subtotal_exclu_tax
        else 0 
    end as unfulfilled_revenue,
    
    k.invoice_value_excl__tax_excl_ship as recognized_revenue,
    k.rec_rev_in_period,
    k.rec_rev_deferred,
    k.revenue_classification,
    k.nav_customer_id,

    case 
        when k.item_id is not null then 'Posted' 
        else 'Not Posted' 
    end as erp_posting_status,
    
    round(COALESCE(b.net_subtotal_exclu_tax, 0) - COALESCE(k.invoice_value_excl__tax_excl_ship, 0), 2) as unfulfilled_sales,
    
    -- Shipping (allocated to first item in order only)
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY l.weborderno) = 1 THEN l.shipping 
        ELSE 0 
    END as shipping,
    
    -- Delivery mode classification
    CASE
        WHEN c.ordertype = 'EXPRESS' AND h.order_date < DATE('2025-01-16') THEN '4-Hour Express'
        WHEN c.ordertype = 'EXPRESS' AND h.order_date >= DATE('2025-01-16') THEN '60-min Express'
        WHEN c.ordertype = 'NORMAL' THEN 'Regular'
        ELSE 'Regular'
    END AS delivery_mode,
    
    -- PNA (Product Not Available) flags
    case 
        when q.item_id is not null then 'pna' 
        else null 
    end as pna_flag,
    
    case 
        when q.item_id is not null and i.statusname in ('CLOSE','ReturnClose')  then 'resolved_pna' 
        when q.item_id is not null and i.statusname != 'CLOSE' then 'permanent_pna'
        when q.item_id is null then 'no_pna'
        else 'ask anmar' 
    end as pna_flag_detail,

    
    case 
    when q.item_id is null then 'no_pna'
    when q.inserted_by = 'SYSTEM' then 'system_pna'
    else 'manual_pna' end as pna_reason,

q.insert_date_time as pna_date

-- Main joins
from {{ ref('stg_ofs_inboundsalesline') }} as a 
left join inboundpaymentline as b on a.itemid = b.itemid 
left join {{ ref('stg_ofs_orderdetail') }} as c on c.itemid = a.itemid
left join ofs_itemdetail as e on e.itemno = a.itemno 
left join orderdataanalysis as f on f.itemid = a.itemid
left join {{ ref('stg_ofs_locationmaster') }} as g on g.id = SAFE_CAST(a.packaginglocation AS INT64)
left join {{ ref('stg_ofs_inboundsalesheader') }} as h on h.weborderno = a.weborderno
left join ofs_crmlinestatus as i on i.itemid = a.itemid
left join {{ ref('fct_erp_occ_invoice_items') }} as k on k.item_id = a.itemid
left join order_head as l on l.weborderno = a.weborderno
left join boxstatus as m on m.boxid = f.boxid
left join {{ ref('stg_petshop_pick_detail') }} as p on p.itemid = a.itemid
left join {{ ref('stg_petshop_pna_details') }} as q on q.item_id = a.itemid


where b.insertedon is not null

--AND a.itemid  = 3866448