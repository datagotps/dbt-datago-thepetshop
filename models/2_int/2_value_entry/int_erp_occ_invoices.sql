
with

cte_inbound_sales_header AS (
    SELECT 
        web_order_id,
        max(order_date) as order_date,
        max(order_type) as order_type,
        --max(type) as type,
        STRING_AGG(invoice_no_, ', ') AS combined_invoice_no,
        COUNT(invoice_no_) AS invoice_no_count,

    FROM 
         {{ ref('stg_erp_inbound_sales_header') }}
         where transaction_type = 'SALE'
    GROUP BY 
        web_order_id

)
,

inbound_sales_line as (
    select
        web_order_no_,
        sum(collectable_amount) as collectable_amount,
        sum(invoice_value_excl__tax) as invoice_value_excl__tax,
        sum(invoice_value_incl__tax) as invoice_value_incl__tax,
        sum(shipping_charges) as shipping_charges,
        sum(shipping_charge_base_amount) as shipping_charge_base_amount,
        sum(tax_amount) as tax_amount,
        sum(tax_base_value) as tax_base_value,
        sum(discount_amount) as discount_amount,
        sum(coupon_discount) as coupon_discount,
        sum(mrp_price) as mrp_price,

        count(*) as invoice_item_count,
       
    from {{ ref('stg_erp_inbound_sales_line') }}
    where type = 1
    group by 
        web_order_no_    

),


ofs_inboundpaymentline as (
    select
        weborderno,
        sum(mrpprice) as mrpprice, --withvat

    from {{ ref('stg_ofs_inboundpaymentline') }}
    where
         isheader = 0
    group by 
        weborderno

)

select

--count(*)

a.web_order_id,
--a.order_date,
a.order_type ,
--a.type,
a.combined_invoice_no,
a.invoice_no_count,


b.invoice_item_count,

b.collectable_amount,

b.invoice_value_incl__tax,
b.tax_amount,
b.invoice_value_excl__tax,
b.tax_base_value,



b.shipping_charges as shipping_inclu_tax,
b.shipping_charge_base_amount as shipping_exclu_tax,


b.discount_amount as discount_inclu_tax,
--discount_exclu_tax

b.mrp_price as net_item_price_inclu_tax, --Price after applying discounts, excluding shipping.


--c.ordersource,
--c.orderplatform,
case 
    when c.orderplatform ='' and c.referrer = 'CRM' then 'CRM'
    else c.orderplatform
    end as orderplatform,


d.mrpprice as mrp_item_price_inclu_tax,



c.order_date,

from  cte_inbound_sales_header as  a

left join inbound_sales_line as  b  on a.web_order_id = b.web_order_no_

left join {{ ref('stg_ofs_inboundsalesheader') }} as c on  c.weborderno = a.web_order_id 

left join ofs_inboundpaymentline as d on d.weborderno =  a.web_order_id


--where a.web_order_id = 'O3075972S'

