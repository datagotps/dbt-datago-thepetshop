
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
,
erp_sales_invoice_line as (
    select
        document_no_,
        no_,
        max(posting_date) as posting_date,
        count(*)

    from {{ ref('stg_erp_sales_invoice_line') }} 
    group by 1,2
)


select
a.type,
a.item_id,
a.web_order_no_,
a.invoice_date,
c.order_date,

CASE WHEN FORMAT_DATE('%Y-%m', c.order_date) = FORMAT_DATE('%Y-%m', d.posting_date) THEN 1 ELSE 0 END AS is_cpr, --current_period_revenue

CASE 
        WHEN FORMAT_DATE('%Y-%m', c.order_date) = FORMAT_DATE('%Y-%m', d.posting_date) THEN 'Rec Rev In-Period'
        WHEN FORMAT_DATE('%Y-%m', c.order_date) < FORMAT_DATE('%Y-%m', d.posting_date) THEN 'Rec Rev Deferred'
        ELSE 'Other'
    END AS revenue_classification,




a.invoice_value_incl__tax,
a.shipping_charges,


--excl__tax
    a.shipping_charge_base_amount,
    a.invoice_value_excl__tax,
    a.invoice_value_excl__tax - COALESCE(a.shipping_charge_base_amount,0)  as invoice_value_excl__tax_excl_ship,

--a.mrp_price, --not correct
b.mrpprice,

a.discount_amount,
 
d.posting_date,
a.documentno,

a.item_no_,

header.nav_customer_id,
--d.line_no_,
from {{ ref('stg_erp_inbound_sales_line') }} as a
left join ofs_inboundpaymentline as b on b.itemid = a.item_id


left join  {{ ref('stg_ofs_inboundsalesheader') }} as c on c.weborderno = a.web_order_no_

left join erp_sales_invoice_line as d on d.document_no_ = a.documentno and d.no_ = a.item_no_
--left join value_entry as e on e.document_no_ = a.documentno


    LEFT JOIN {{ ref('stg_erp_inbound_sales_header') }} AS header
        ON a.documentno = header.documentno
        AND a.invoice_no_ = header.invoice_no_
        AND a.type = 1


where a.type = 1
--where a.documentno = 'INV00431517' and  a.type = 1 
--where  a.web_order_no_='O3077302S'