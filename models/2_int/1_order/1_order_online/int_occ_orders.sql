
with 
order_payment_data as 

    (
        select
        weborderno,
        
        sum(case when isheader = 1 then amountincltax else 0 end ) as order_value, 
        sum(case when isheader = 1 then invoicediscountamount else 0 end ) as order_discount,
        sum(case when isheader = 1 then shippingcharges else 0 end ) as shipping,

        sum(case when isheader = 1 then amountincltax + invoicediscountamount else 0 end ) as gross_order_value,
        sum(case when isheader = 1 then  amountincltax - shippingcharges  else 0 end ) as net_subtotal,
        sum(case when isheader = 1 then  (amountincltax - shippingcharges) + invoicediscountamount  else 0 end ) as gross_subtotal,


        sum(case when isheader = 1 then tax else 0 end ) as order_tax,
        --sum(case when isheader = 0 then tax else 0 end ) as net_subtotal_tax,
        sum(case when isheader = 1 then tax else 0 end - case when isheader = 0 then tax else 0 end) as shipping_tax,

        round(sum(case when isheader = 0 then mrpprice / (1 + TaxPercentage / 100) else 0 end ),2) as gross_subtotal_exclu_tax,
        round(sum(case when isheader = 0 then COALESCE(discount,0)  / (1 + TaxPercentage / 100) else 0 end ),2) as order_discount_exclu_tax,

        count(case when isheader = 0 then itemid else null end ) as items_sold,

        
        sum(case when isheader = 0 then mrpprice else 0 end ) as gross_subtotal_2,
        --sum(case when isheader = 0 then amountincltax else 0 end ) as net_subtotal,
        max(insertedon) as insertedon,

        --sum(case when isheader = 0 then mrpprice - discount else 0 end ) as net_subtotal, --mthod2
        --sum(case when isheader = 0 then discount else 0 end ) as discount,

        from {{ ref('stg_ofs_inboundpaymentline') }} group by 1 
        --where weborderno = 'O3070167S' 

    )
,
orderdataanalysis  as 
    (

        select
        weborderno,
        max(awbno) as awbno,
   
        from  {{ ref('stg_ofs_orderdataanalysis') }}
        group by 1
        
    )
,

driverdeliverystage  as 
    (

        select
        awb,
        max(lattitude) lattitude,
        max(longitude) longitude,

        
   
        from  {{ ref('stg_ofs_driverdeliverystage') }}
        group by 1
        
    )

,
ofs_address  as 
    (

        select
        weborderno,
        Latitude,
        Longitude,
        state,
        region,
        city,
        street,
        from  {{ ref('stg_ofs_inboundorderaddress') }}
        where addressdetailtype = 'Ship'
        

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


line_items  as 
    (

        select
        a.weborderno,
        sum(a.amountincltax) as order_value_2,
        sum(a.mrpprice) as subtotal,
        round(sum(a.mrpprice / (1 + a.TaxPercentage / 100)),2) AS subtotal_exclu_tax,
        sum(a.discount) as discount,
        round(sum (COALESCE(a.discount,0)  / (1 + a.TaxPercentage / 100)),2) as discount_exclu_tax,

        sum(a.tax) as subtotal_taxes,

        count(a.itemid) as items,
        

        from {{ ref('stg_ofs_inboundpaymentline') }}  as a
        left join {{ ref('stg_ofs_crmlinestatus') }} as b on a.itemid = b.itemid
        
        where a.isheader = 0  group by 1

    )

,

ofs_location_level  as 
    (

select 
    weborderno,
    STRING_AGG(location, ', ') AS location,
    count(*) as location_count,
    from {{ ref('int_ofs_location_level') }}
    --WHERE  weborderno= 'O3076606S'
    group by 1

    )
,    
crm_orders as 

    (
        select
        a.weborderno,
        concat (max(firstname), ' ', max(lastname)) as customer,
        max(b.statusname) as crm_delivery_status,

        from  {{ ref('stg_ofs_crmorders') }} as a
        left join {{ ref('stg_ofs_crmorderstatus') }} as b on b.weborderno = a.weborderno
   
        
        group by 1
    )
,

ofs_inbound_sales_header as 

    (
        select
        weborderno,
        max(customerid) as customerid ,
        max(order_date) as order_date,
        
        max(orderplatform) as orderplatform,
        max(ordertype) as ordertype,

        from {{ ref('stg_ofs_inboundsalesheader') }}   group by 1
    )
,

ofs_inbound_sales_line as 

    (
        select
        weborderno,
        count(distinct sku) as sku_count,
        count(itemid) as item_count,
        
        from {{ ref('stg_ofs_inboundsalesline') }}   group by 1
    )
,




erp_inbound_sales_header as (
    select
        web_order_id,
        STRING_AGG(invoice_no_, ', ') AS combined_invoice_no,
        case when COUNT(invoice_no_) = 1 then 'Single Shipment' else concat('Split Shipment - ', count(invoice_no_)) end as split_shipment_indicator,
        
       
    from {{ ref('stg_erp_inbound_sales_header') }}
    where web_order_id !='' 
    group by  web_order_id
)

,


erp_inbound_sales_line as (
    select
        web_order_no_,
        COUNT(item_id) AS fulfilled_items,
        sum(quantity) as quantity,
       
    from {{ ref('stg_erp_inbound_sales_line') }} group by  1
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

,

erp_posted_sales_invoice_head as (
    select
        b.web_order_id,
        STRING_AGG(no_, ', ') AS combined_posted_invoice_no,
        sum(c.posted_amount_including_vat) as posted_amount_including_vat,
        sum(c.posted_amount) as posted_total_exlu_tax,
        count(*),
    from {{ ref('stg_erp_sales_invoice_header') }} as a
    left join {{ ref('stg_erp_inbound_sales_header') }} as b on a.no_ =b.documentno
    left join erp_posted_sales_invoice_line as c on c.document_no_ = a.no_

    where b.web_order_id !=''
    group by 
            b.web_order_id
        order by 3 desc 
)

,

recognized_revenue as (

select 
web_order_no_,

sum(invoice_value_incl__tax) as invoice_value_incl__tax,
sum(invoice_value_excl__tax_excl_ship) as invoice_value_excl__tax_excl_ship,

sum(rec_rev_in_period) as rec_rev_in_period,
sum(rec_rev_deferred) as rec_rev_deferred,
from {{ ref('fct_erp_occ_invoice_items') }}  
group by  1

)




select
a.weborderno,

a.gross_order_value,
a.order_discount,
a.order_value, --ofs_total
b.order_value_2,


a.gross_subtotal, --b.subtotal,
a.gross_subtotal_2,
case when a.gross_subtotal = a.gross_subtotal_2 then 'march' else 'not match' end as gross_subtotal_cheack,

round(a.gross_subtotal - a.gross_subtotal_exclu_tax ,2) as gross_subtotal_tax ,
--b.subtotal_taxes,

a.items_sold,

a.order_discount - a.order_discount_exclu_tax as discount_tax,
a.shipping,  -- shipping_inclu_tax
a.shipping_tax,

a.order_tax, --Taxes



c.split_shipment_indicator, 
f.orderplatform,
g.crm_delivery_status,

g.Customer,


h.location,
h.location_count,



d.fulfilled_items,
d.quantity,


b.items - COALESCE(d.fulfilled_items, 0) as unfulfilled_items,
 
e.posted_total_exlu_tax,

f.ordertype,


case 
when f.ordertype = 'EXPRESS' and h.location_count = 1 and h.location = 'HQW' then 'Express Delivery From Fulfillment Center' --Single shipment from HQW (4-Hour Express) express before 2025-01-16
when f.ordertype = 'EXPRESS' and h.location_count = 1 and h.location != 'HQW' then 'Express Delivery From Store' --Single shipment from store (60-min Express) express orders after  2025-01-16
when f.ordertype = 'EXPRESS' and h.location_count > 1 then 'Mixed Fulfilment (Express & Standard)' --Split shipment (Mixed Fulfilment)
when f.ordertype != 'EXPRESS' then 'Standard Delivery' --normal orders 
else 'Ask anmar@8020datago.ai'
end as fulfillment_type,


i.sku_count,
i.item_count,


f.order_date,
a.insertedon,


--e.posted_amount_including_vat as recognized_revenue,  --posted_total
k.invoice_value_incl__tax as recognized_revenue,
k.rec_rev_in_period,
k.rec_rev_deferred,

round( a.order_value - COALESCE(e.posted_amount_including_vat, 0),0) as unfulfilled_revenue,
--case when e.posted_amount_including_vat is null then a.order_value  else 0 end as unfulfilled_revenue,

case when e.combined_posted_invoice_no is not null then 'Posted' else 'Not Posted' end as erp_posting_status,


l.Latitude as order_Latitude,
l.Longitude as order_Longitude,
l.state,
l.region,
l.city,
l.street,


case 
    when l.state in ('Dubai','DU','FU') Then 'Dubai'
    when l.state in ('Abu Dhabi') Then 'Abu Dhabi'
    when l.state in ('Sharjah','SH') Then 'Sharjah'
    when l.state in ('Ajman','AJ') Then 'Ajman'
    when l.state in ('Ras al-Khaimah','RK') Then 'Ras Al-Khaimah'
    when l.state in ('Umm al-Quwain','UQ') Then 'Umm al-Quwain'
else l.state end as c_city,


CASE
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.1300046  AND 25.1482725  AND CAST(n.longitude AS DECIMAL) BETWEEN 55.2325182 AND  55.2790647 THEN 'Dubai Downtown'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.1300046 AND 25.1798962 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.2325182 AND 55.2685525 THEN 'Business Bay'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.1289409 AND 25.1300046 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.211463 AND 55.2325182 THEN 'Al Quoz'

    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.2002125 AND 25.2066955 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.3164398 AND 55.3477911 THEN 'Dubai Creek'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.1902684 AND 25.2027912 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.2357605 AND 55.2413195 THEN 'Jumeirah'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.0806744 AND 25.0812972 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.1288754 AND 55.1397921 THEN 'Dubai Marina'

        WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.0765153 AND 25.0692834 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.1417222 AND 55.1734594 THEN 'JLT'


    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.2300 AND 25.2900 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.1500 AND 55.2000 THEN 'Tecom'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.2500 AND 25.3700 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.2700 AND 55.4000 THEN 'Barsha'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.4100 AND 25.4500 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.3000 AND 55.4500 THEN 'Jebel Ali'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.0475534 AND 25.0523684 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.2529684 AND 55.2673602 THEN 'Arabian Ranches'
    WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 25.1000 AND 25.1500 AND CAST(n.longitude AS DECIMAL) BETWEEN 55.1200 AND 55.1500 THEN 'Palm Jumeirah'


    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.4700 AND 24.4900 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.3600 AND 54.3800 THEN 'Abu Dhabi Downtown'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.4600 AND 24.4800 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.3700 AND 54.4200 THEN 'Corniche, Abu Dhabi'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.4520 AND 24.4750 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.5920 AND 54.6160 THEN 'Yas Island'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.4180 AND 24.4500 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.5100 AND 54.5300 THEN 'Khalifa City'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.4510 AND 24.4700 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.6160 AND 54.6400 THEN 'Al Raha Beach'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.3300 AND 24.3500 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.4700 AND 54.4900 THEN 'Mussafah'
    --WHEN CAST(n.lattitude AS DECIMAL) BETWEEN 24.5180 AND 24.5400 AND CAST(n.longitude AS DECIMAL) BETWEEN 54.4270 AND 54.4600 THEN 'Saadiyat Island'

    ELSE 'Other Area'
END AS location_area,


m.awbno,

n.lattitude as delivery_Latitude,
n.longitude as delivery_longitude,

case when l.Latitude = '25.010022863963258' and l.Longitude = '55.15683832090804' then 1 else 0 end as is_default_coords, --Flags orders with default coordinates ("Default Coordinates") if they match predefined values; otherwise, returns NULL.

case when l.Latitude = n.lattitude and l.Longitude = n.longitude then 'Match' else 'Not Match' end as delivery_location_match, --Indicates if the order location matches the actual delivery location. "Match" if coordinates align, otherwise "Not Match" 

f.customerid,
from order_payment_data as a
left join line_items as b on a.weborderno = b.weborderno
left join erp_inbound_sales_header as c on c.web_order_id = a.weborderno
left join erp_inbound_sales_line as d on d.web_order_no_ = a.weborderno
left join erp_posted_sales_invoice_head as e on e.web_order_id = a.weborderno

left join ofs_inbound_sales_header as f on f.weborderno = a.weborderno
left join crm_orders as g on g.weborderno = a.weborderno

left join ofs_location_level as h on h.weborderno = a.weborderno

left join ofs_inbound_sales_line as i on i.weborderno = a.weborderno

left join recognized_revenue as k on k.web_order_no_ = a.weborderno


left join ofs_address as l on l.weborderno = a.weborderno
--where a.weborderno = 'O3070167S'

left join orderdataanalysis as m on m.weborderno = a.weborderno

left join driverdeliverystage n on n.awb = m.awbno

--where a.weborderno= 'O3066151S'