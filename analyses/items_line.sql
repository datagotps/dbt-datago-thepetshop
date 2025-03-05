with orderstatusline as 
(

    WITH RankedStatus AS (
        SELECT
            weborderno,
            itemid,
            statusname,
            insertedon,
            ROW_NUMBER() OVER (
                PARTITION BY weborderno, itemid
                ORDER BY insertedon DESC, statusname
                              ) AS rn
        FROM {{ ref('stg_ofs_orderstatusline') }}
        where statusname != 'CLOSE'
    )
    SELECT
        weborderno,
        itemid,
        statusname AS order_line_status,
        insertedon AS order_line_status_date,
    FROM RankedStatus
    WHERE rn = 1 
)
,
order_shipping as 

    (
        select
        weborderno,
        sum(case when isheader = 1 then shippingcharges else 0 end ) as shipping,
        from {{ ref('stg_ofs_inboundpaymentline') }} group by 1 --where weborderno = 'O3070167S' 

    )

,

ofs_crmlinestatus as 
(
    SELECT 
    itemid,
    statusname,

    FROM {{ ref('stg_ofs_crmlinestatus') }} 
)

,

ofs_itemdetail as 

    (
     with deduplicated as 
      (
           select *,
           ROW_NUMBER() over (PARTITION BY itemno ORDER BY itemno) AS rn
            from  {{ ref('stg_ofs_itemdetail') }}
      )
     select * from deduplicated WHERE rn = 1
    )
 
,
erp_posted_sales_invoice_line as (
    select
        document_no_,
        sum(amount) as posted_amount,
        sum(amount_including_vat) as posted_amount_including_vat,
        count(*) as invoice_item_count,
        sum(case when gen__prod__posting_group = 'SHIPPING' then amount else 0 end) as shipping_amount_exlu_tax,
        sum(case when gen__prod__posting_group = 'SHIPPING' then amount_including_vat else 0 end) as shipping_amount_inclu_tax,
       
    from {{ ref('stg_erp_sales_invoice_line') }} 
    group by 
        1     
)



select
a.weborderno,
a.itemid,

--b.amountincltax as items_net_sales,
--b.mrpprice as items_gross_sales,


b.mrpprice as unit_mrpprice, --_incl_tax,
b.discount as unit_discount, --_incl_tax,
b.amountincltax as unit_final_price,  --_incl_tax,


-- sum(unit_final_price) as items_net_sales (NMV)
-- sum(unit_mrpprice) as items_gross_sales (GMV)
-- sum(unit_discount) as items_discount



b.tax as unit_tax,

--b.amount as unit_final_price_exclu_tax,
--b.mrpprice / (1 + b.TaxPercentage / 100) as unit_mrpprice_exclu_tax,
--(b.amountincltax  - b.mrpprice)  / (1 + b.TaxPercentage / 100) as unit_discount_exclu_tax,

--(b.amountincltax  - b.mrpprice) = b.discount
--(unit_final_price - unit_mrpprice) = unit_discount
a.itemno,
a.sku,
a.quantity,

g.location,





c.ordertype,


d.order_line_status,
i.statusname as crm_order_line_status,

e.description as item_name,
e.divisioncodedescription as division,
e.itemcategorycode as item_category,
e.retailproductcode as item_subcategory,
e.brand as item_brand,

h.order_date,
h.orderplatform,

f.awbno,
f.iscancelled,

(k.invoice_value_incl__tax - k.shipping_charges) as posted_merchandise_value ,

case when k.item_id is not null then 'Posted' else 'Not Posted' end as erp_posting_status,

round(  COALESCE(b.amountincltax, 0) - COALESCE(k.invoice_value_incl__tax - k.shipping_charges, 0),2) as unfulfilled_sales,




CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY l.weborderno ) = 1 THEN l.shipping 
    ELSE 0 
  END as shipping,



from  {{ ref('stg_ofs_inboundsalesline') }} as a --5409155
left join {{ ref('stg_ofs_inboundpaymentline') }} as b on a.itemid = b.itemid and b.isheader =0
left join {{ ref('stg_ofs_orderdetail') }} as c on c.itemid = a.itemid

left join orderstatusline as d on d.itemid = a.itemid 

left join ofs_itemdetail as e on e.itemno = a.itemno 
 
left join {{ ref('stg_ofs_orderdataanalysis') }} as f on f.itemid = a.itemid

left join  {{ ref('stg_ofs_locationmaster') }} as g on g.id = SAFE_CAST(a.packaginglocation AS INT64)

left join  {{ ref('stg_ofs_inboundsalesheader') }} as h on h.weborderno = a.weborderno
left join ofs_crmlinestatus as i on i.itemid = a.itemid


left join {{ ref('stg_erp_inbound_sales_line') }}  as k on k.item_id = a.itemid and k.type = 1

left join order_shipping as l on l.weborderno = a.weborderno
--where a.weborderno = 'O3070167S'


