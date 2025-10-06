WITH shipment_summary AS (
    -- First, count shipments per order
    SELECT 
        web_order_id,
        COUNT(DISTINCT invoice_no_) as total_shipments,

        --COUNT(DISTINCT documentno) as total_shipments,
        COUNT(DISTINCT box_id) as total_boxes,
        COUNT(DISTINCT invoice_no_) as total_invoices,
        COUNT(DISTINCT delivery_date) as unique_delivery_dates,
        MIN(delivery_date) as first_delivery_date,
        MAX(delivery_date) as last_delivery_date,
        STRING_AGG(DISTINCT documentno, ', ' ORDER BY documentno) as document_numbers,
        STRING_AGG(DISTINCT invoice_no_, ', ' ORDER BY invoice_no_) as invoice_numbers,
        STRING_AGG(DISTINCT box_id, ', ' ORDER BY box_id) as box_ids,

        COUNT(DISTINCT CASE WHEN type = 2 THEN documentno END) as total_returns,
        STRING_AGG(DISTINCT CASE WHEN type = 2 THEN documentno END, ', ' ) as return_documents,

    FROM {{ ref('stg_erp_inbound_sales_header') }}
    WHERE web_order_id IS NOT NULL
    GROUP BY web_order_id
)


select
a.* EXCEPT(online_order_channel),

CASE  
WHEN a.online_order_channel = 'Unmapped' AND a.order_date >= DATE '2024-10-01' AND a.order_date <  DATE '2024-11-01' THEN 'Website'
WHEN a.online_order_channel = 'Unmapped' AND b.referrer LIKE '%.%' THEN 'iOS'
ELSE a.online_order_channel end as online_order_channel,

--b.online_order_channel as online_order_channel_2,

CASE WHEN ss.total_shipments > 1 THEN 1 ELSE 0 END as is_split_order,
CASE WHEN ss.total_returns > 0 THEN 1 ELSE 0 END as is_return_order,

--a.*,



    -- Fulfillment Type
    CASE 
        WHEN a.type = 2 THEN 'Return Order'
        WHEN ss.total_shipments = 1 THEN 'Single Fulfillment'
        WHEN ss.total_shipments > 1 AND ss.unique_delivery_dates = 1 THEN 'Multi-Box Same Day'
        WHEN ss.total_shipments > 1 AND ss.unique_delivery_dates > 1 THEN 'Split Fulfillment'
        ELSE 'Unknown'
    END as fulfillment_type,


--from ofs_inboundsalesheader
        b.ordersource, -- D, I, A, CRM, '', CRM Exchange, FOC
        b.ordertype, -- NORMAL, EXPRESS, EXCHANGE
        b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
        b.paymentmethodcode, -- PREPAID, COD, creditCard
        b.weborderno,
        b.order_date as order_date_time,
        b.orderplatform,
        b.referrer,


FROM {{ ref('stg_erp_inbound_sales_header') }} as a
LEFT JOIN {{ ref('stg_ofs_inboundsalesheader') }} as b ON a.web_order_id = b.weborderno
LEFT JOIN shipment_summary ss ON a.web_order_id = ss.web_order_id

--where a.web_order_id= 'O3035089S'
--where unique_delivery_dates >1