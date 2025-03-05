
with 
ofs_inboundpaymentline as (
    select
        itemid,
        mrpprice, --withvat

    from {{ ref('stg_ofs_inboundpaymentline') }}
    where
         isheader = 0 
         --and weborderno =  'O3075972S'
   
)

select
a.item_id,
a.web_order_no_,
a.invoice_date,
c.order_date,


CASE WHEN FORMAT_DATE('%Y-%m', c.order_date) = FORMAT_DATE('%Y-%m', a.invoice_date) THEN 1 ELSE 0 END AS is_cpr, --current_period_revenue

CASE 
        WHEN FORMAT_DATE('%Y-%m', order_date) = FORMAT_DATE('%Y-%m', invoice_date) THEN 'Rec Rev In-Period'
        WHEN FORMAT_DATE('%Y-%m', order_date) < FORMAT_DATE('%Y-%m', invoice_date) THEN 'Rec Rev Deferred'
        ELSE 'Other'
    END AS revenue_classification,

a.invoice_value_incl__tax,
a.shipping_charges,

--a.mrp_price, --not correct
b.mrpprice,

a.discount_amount,

from {{ ref('stg_erp_inbound_sales_line') }} as a
left join ofs_inboundpaymentline as b on b.itemid = a.item_id


left join  {{ ref('stg_ofs_inboundsalesheader') }} as c on c.weborderno = a.web_order_no_

where a.type = 1 
--and  web_order_no_ = 'O3075972S'