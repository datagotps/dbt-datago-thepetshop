with 
inboundpaymentline as 

    (
        select
        itemid,
         
        mrpprice as gross_subtotal,
        amountincltax as net_subtotal,
        discount,
        insertedon,


        from  {{ ref('stg_ofs_inboundpaymentline') }}
        where isheader =0 
        --and weborderno = 'O3070096S'
        
         
        

    )

,
order_head as 

    (
        select
        weborderno,
        sum(case when isheader = 1 then shippingcharges else 0 end ) as shipping,
        sum(case when isheader = 1 then amountincltax else 0 end ) as order_value,
        from {{ ref('stg_ofs_inboundpaymentline') }} group by 1 --where weborderno = 'O3070167S' 

    )

,
  

orderdataanalysis as 

    (
        select
        itemid,
        orderdate,
        deliverydate,

        batchdatetime,
        pickeddatetime,
        boxdatetime,


        ordertype,
        case when batchid is null or batchid = 0 then 0  else 1  end as batchcreated,
        allocated,
        picked,
        case when awbno is null or awbno = '' then 0 else 1 end as packed,
        isdelivered,
        case when returnticket is null or returnticket = '' then 0 else 1 end as returned,
        iscancelled,

        awbno,

        from  {{ ref('stg_ofs_orderdataanalysis') }} 
        where weborderno = 'O3070096S' 
        

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

b.gross_subtotal,
b.net_subtotal,
b.discount,
b.insertedon,

--b.mrpprice as unit_mrpprice, --_incl_tax,
--b.discount as unit_discount, --_incl_tax,
--b.amountincltax as unit_final_price,  --_incl_tax,
--b.tax as unit_tax,




-- sum(unit_final_price) as items_net_sales (NMV)
-- sum(unit_mrpprice) as items_gross_sales (GMV)
-- sum(unit_discount) as items_discount




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
i.statusname as crm_order_line_status,

e.description as item_name,
e.divisioncodedescription as division,
e.itemcategorycode as item_category,
e.retailproductcode as item_subcategory,
e.brand as item_brand,

h.order_date,
h.orderplatform,
h.customerid,

f.awbno,
f.iscancelled,

f.isdelivered,
f.batchcreated,
f.packed,
f.returned,



case 
when f.isdelivered is not true then net_subtotal 
when f.returned = 1 and f.iscancelled is true then net_subtotal
else 0 end as unfulfilled_revenue,


k.invoice_value_incl__tax as recognized_revenue,

k.rec_rev_in_period,
k.rec_rev_deferred,
k.revenue_classification,

(k.invoice_value_incl__tax - k.shipping_charges) as recognized_merchandise_value ,

case when k.item_id is not null then 'Posted' else 'Not Posted' end as erp_posting_status,

round(  COALESCE(b.net_subtotal, 0) - COALESCE(k.invoice_value_incl__tax - k.shipping_charges , 0),2) as unfulfilled_sales,


CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY l.weborderno ) = 1 THEN l.shipping 
    ELSE 0 
  END as shipping,



from  {{ ref('stg_ofs_inboundsalesline') }} as a --5409155
left join inboundpaymentline as b on a.itemid = b.itemid 

left join {{ ref('stg_ofs_orderdetail') }} as c on c.itemid = a.itemid


left join ofs_itemdetail as e on e.itemno = a.itemno 

left join orderdataanalysis as f on f.itemid = a.itemid


left join  {{ ref('stg_ofs_locationmaster') }} as g on g.id = SAFE_CAST(a.packaginglocation AS INT64)

left join  {{ ref('stg_ofs_inboundsalesheader') }} as h on h.weborderno = a.weborderno
left join ofs_crmlinestatus as i on i.itemid = a.itemid


left join {{ ref('fct_erp_occ_invoice_items') }}  as k on k.item_id = a.itemid --and k.type = 1

left join order_head as l on l.weborderno = a.weborderno
--where a.weborderno = 'O3077296S'


