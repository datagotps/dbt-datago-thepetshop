--Each record represents an order, with additional dimensions like whether it is a new or repeat purchase.


with cte_coupon as (
    select
        weborderno,
        STRING_AGG(distinct couponcode, ',') as couponcode,
        SUM(couponamount) as couponamount
    from {{ ref('stg_inboundpaymentline') }}
    where
        couponcode != ''
        and isheader = 0
    group by 
        weborderno
),

cte_items as (
    select
        weborderno,
        count(itemid) as line_item_count,
        sum(mrpprice) as mrpprice,
    from {{ ref('stg_inboundpaymentline') }}
    where
         isheader = 0
    group by 
        weborderno

)

select

--Order

    sh.weborderno,
    sh.orderplatform,  -- OCC, shopify, CRM, null
    sh.ordercategory,  -- NORMAL


    sh.orderdatetime,




    sh.ordersource,  -- D, I, A, CRM, '', CRM Exchange, FOC
    sh.ordertype,  -- NORMAL, EXPRESS, EXCHANGE
    sh.paymentmethodcode,  -- PREPAID, COD, creditCard
    sh.paymentgateway,  -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
    sh.expecteddispatchdate,
    sh.referrer,
    sh.reservedfield1,
    sh.reservedfield2,
    sh.reservedfield3,
    sh.reservedfield4,
    sh.reservedfield5,

    --c.orderamount, --orderamountincltax,


    c.currency,
    --c.packaginglocation, --4, 10, 8, 6, 20, null review: locationmaster table
    orderdeliverytype, --clc

    os.statusname as order_status,

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

    



pl.discount,
pl.amount,
pl.amountincltax,
pl.tax,

pl.othercharges, 
pl.customizedcharges, 
pl.shippingcharges,
pl.codcharges, 
pl.giftcharges, 

cte_coupon.couponcode,
cte_coupon.couponamount,

cte_items.line_item_count,
cte_items.mrpprice, --Maximum Retail Price (MRP)


pl.insertedon as order_date,



from {{ ref('stg_inboundsalesheader') }} as sh
left join  {{ ref('stg_crmorders') }} as c on c.weborderno = sh.weborderno
left join {{ ref('stg_orderstatusmaster') }} as os on c.orderstatus = os.id
left join {{ ref('stg_inboundorderaddress') }} AS oa ON sh.weborderno = oa.weborderno and oa.AddressDetailType = 'Ship'
left join {{ ref('stg_inboundpaymentline') }} pl on pl.weborderno = sh.weborderno and pl.isheader = 1
left join cte_coupon on sh.weborderno = cte_coupon.weborderno
left join cte_items on sh.weborderno = cte_items.weborderno


--where  date(sh.orderdatetime) = '2025-01-06' and sh.weborderno = 'O3070217S'
--where sh.weborderno = 'O3585711'

--where sh.orderplatform = 'shopify'
--where orderchargesprocessed != 0