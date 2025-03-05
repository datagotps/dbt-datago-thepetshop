--Each record represents an order, with additional dimensions like whether it is a new or repeat purchase.


with 

cte_ofs_inbound_items as (
    select
        weborderno,
        count(itemid) as line_item_count,
        sum(mrpprice) as mrpprice, --withvat
        --
        
        SUM(couponamount) as couponamount,
        

        STRING_AGG(distinct discounttype, ',') as discounttype,
        sum(tax) as items_tax,
        SUM(amount) as items_amount,
        sum(discount) as items_discount,
        round(sum(mrpprice / (1 + TaxPercentage / 100)),2) AS mrpprice_exclu_tax,

        round(sum (amount  - mrpprice  / (1 + TaxPercentage / 100)),2) as discount_exclu_tax,


    from {{ ref('stg_ofs_inboundpaymentline') }}
    where
         isheader = 0
    group by 
        weborderno

),

cte_erp_inbound_sales_header as (
    select
        web_order_id,
        max(invoice_no_) as invoice_no_,
        count(*),
        count(invoice_no_) as invoice_no_count,
        STRING_AGG(invoice_no_, ', ') AS combined_invoice_no,
       
    from {{ ref('stg_erp_inbound_sales_header') }}
    where web_order_id !='' and type = 'Sales'
    group by 
        web_order_id
    order by 3 desc     
),

cte_erp_posted_head_invoice as (
select
b.web_order_id,
max(no_) as no_,
count(*),
from {{ ref('stg_erp_sales_invoice_header') }} as a
left join {{ ref('stg_erp_inbound_sales_header') }} as b on a.no_ =b.documentno
where web_order_id !=''
group by 
        web_order_id
    order by 3 desc 
),


cte_erp_posted_line_invoice as (
    select
        document_no_,
        sum(amount) as amount,
        sum(amount_including_vat) as amount_including_vat,
        count(*) as invoice_item_count,
       
    from {{ ref('stg_erp_sales_invoice_line') }}
    group by 
        1     
)






select

--Order

    sh.weborderno,
    --sh.orderplatform,  -- OCC, shopify, CRM, null

    case 
    when sh.orderplatform ='' and sh.referrer = 'CRM' then 'CRM'
    else sh.orderplatform
    end as orderplatform,

    sh.referrer,


    sh.ordercategory,  -- NORMAL


    sh.orderdatetime as order_date,




    sh.ordersource,  -- D, I, A, CRM, '', CRM Exchange, FOC
    sh.ordertype,  -- NORMAL, EXPRESS, EXCHANGE
    sh.paymentmethodcode,  -- PREPAID, COD, creditCard
    sh.paymentgateway,  -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
    sh.expecteddispatchdate,
    
    sh.reservedfield1,
    sh.reservedfield2,
    sh.reservedfield3,
    sh.reservedfield4,
    sh.reservedfield5,



    --c.orderamount, --orderamountincltax,


    c.currency,
    --c.packaginglocation, --4, 10, 8, 6, 20, null review: locationmaster table
    orderdeliverytype, --clc

    

--Order Address
    oa.city,
    oa.State,
    oa.Region,
    oa.Street,
    oa.Latitude,
    oa.Longitude,


--Customer
    sh.customercomment,
    
    c.firstname,
    c.lastname,
    c.customeremail,
    c.customerphone,

    






  --Discount amount applicable at the invoice level.


pl.loyalitypointamount,
pl.paymentgatewayamount,

pl.walletname,

pl.othercharges, 
pl.customizedcharges, 



--pl.shippingcharges * 0.05 as shippingcharges_taxes,
cte_ofs_inbound_items.items_tax,
cte_ofs_inbound_items.items_amount,
cte_ofs_inbound_items.line_item_count,
cte_ofs_inbound_items.mrpprice, --Maximum Retail Price (MRP)

cte_ofs_inbound_items.discounttype,
cte_ofs_inbound_items.items_discount,






pl.codcharges, 
pl.giftcharges, 


--cte_ofs_inbound_items.discount,





pl.insertedon as insertedon,


os.statusname as order_status_2, --not reflected old
cs.statusname as delivery_status,  --

--case when cs.statusname = 'Delivered' then 'Posted' else 'Not Posted' end as erp_posting_status,

cte_erp_inbound_sales_header.invoice_no_,

case when cte_erp_inbound_sales_header.invoice_no_ is not null then 'Synced' else 'Not Synced' end as erp_Syncing_status,

case when cte_erp_posted_head_invoice.no_ is not null then 'Posted' else 'Not Posted' end as erp_posting_status,




pl.amount,  -- Net sales Sales revenue, with discounts and returns factored in
pl.amountincltax,
pl.tax,

ROUND(cte_ofs_inbound_items.mrpprice_exclu_tax, 2) as mrpprice_exclu_tax ,  ---Gross sales: Sales revenue, before discounts and returns are factored in
ROUND(cte_ofs_inbound_items.discount_exclu_tax, 2) as discount_exclu_tax, --Discounts: Amount discounted from sales revenue


pl.shippingcharges as shipping_inclu_tax,
ROUND(pl.tax - cte_ofs_inbound_items.items_tax ,2) as shipping_tax, --ShippingTax

ROUND(pl.shippingcharges - (pl.tax - cte_ofs_inbound_items.items_tax),2)  as shipping_exclu_tax,  --Shipping charges Amount customers spent on shipping





pl.invoicediscountamount as discount_inclu_tax ,

pl.invoicediscountamount - cte_ofs_inbound_items.discount_exclu_tax as discount_tax,

pl.invoicediscounttax,
pl.invoicediscountwithtax,


ii.amount_including_vat as posted_amount_including_vat,


from {{ ref('stg_ofs_inboundsalesheader') }} as sh
left join  {{ ref('stg_ofs_crmorders') }} as c on c.weborderno = sh.weborderno
left join {{ ref('stg_ofs_orderstatusmaster') }} as os on c.orderstatus = os.id

left join {{ ref('stg_ofs_crmorderstatus') }} as cs on sh.weborderno = cs.weborderno


left join {{ ref('stg_ofs_inboundorderaddress') }} AS oa ON sh.weborderno = oa.weborderno and oa.AddressDetailType = 'Ship'
left join {{ ref('stg_ofs_inboundpaymentline') }} pl on pl.weborderno = sh.weborderno and pl.isheader = 1
left join cte_ofs_inbound_items on sh.weborderno = cte_ofs_inbound_items.weborderno

left join cte_erp_inbound_sales_header on cte_erp_inbound_sales_header.web_order_id = sh.weborderno
left join cte_erp_posted_head_invoice on cte_erp_posted_head_invoice.web_order_id = sh.weborderno

left join cte_erp_posted_line_invoice as ii on ii.document_no_ = cte_erp_posted_head_invoice.no_


--where cte_erp_inbound_sales_header.invoice_no_ is not null and cte_erp_posted_invoice.no_ is  null

--where sh.orderplatform = ''

--where sh.weborderno= 'O3072896S'
 
