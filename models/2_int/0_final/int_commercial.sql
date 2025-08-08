--recorder 25938372
select

a.source_no_,
a.document_no_,
a.posting_date,

a.sales_amount__actual_,
a.cost_amount__actual_,
a.discount_amount,

a.sales_channel, 
a.sales_channel_sort,

a.offline_order_channel, --store location
a.source_code,
a.item_ledger_entry_type,

        -- Posting Groups
        a.gen__prod__posting_group,
        a.gen__bus__posting_group,
        a.source_posting_group,
        a.inventory_posting_group,
a.global_dimension_1_code,
a.global_dimension_2_code,
a.location_code,
a.clc_location_code,
a.user_id,
a.entry_type_2,
a.document_type,
a.dimension_code,
a.global_dimension_2_code_name,
a.clc_global_dimension_2_code_name,
a.transaction_type,
--case when transaction_type = 'Sale' then a.sales_amount__actual_ else 0 end as posted_revenue,
--case when transaction_type = 'Refund' then a.sales_amount__actual_ else 0 end as refund,


case 
when a.sales_channel = 'Shop' then a.location_code 
when a.sales_channel = 'Online' then b.online_order_channel
else a.global_dimension_2_code_name end as sales_channel_detail,


CASE 
    WHEN a.sales_channel = 'Online' THEN b.web_order_id 
    WHEN a.sales_channel IN ('Shop', 'Affiliate', 'B2B', 'Service') AND a.transaction_type = 'Sale' THEN a.document_no_
    ELSE NULL 
END AS unified_order_id,

CASE WHEN a.transaction_type = 'Refund' THEN a.document_no_  ELSE null  END AS unified_refund_id,


b.web_order_id,
b.online_order_channel, --website, Android, iOS, CRM, Unmapped
b.order_type, --EXPRESS, NORMAL, EXCHANGE
b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
b.paymentmethodcode, -- PREPAID, COD, creditCard


c.name,
c.raw_phone_no_,
c.customer_identity_status,


e.item_no_,
e.item_name,
e.item_category,
e.item_subcategory,
e.item_brand,
e.division,

--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount else 0 end as online_line_amount_excl_vat,
--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount_including_vat else 0 end as online_line_amount_enclu_vat,

--d.name,
--count(*)

    -- Current Period Flags (based on posting_date)
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE(CURRENT_DATETIME()), MONTH) 
         AND a.posting_date <= CURRENT_DATE() 
         THEN 1 ELSE 0 END as is_mtd,
    
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE(CURRENT_DATETIME()), YEAR) 
         AND a.posting_date <= CURRENT_DATE() 
         THEN 1 ELSE 0 END as is_ytd,
    
    -- Last Month To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
         AND EXTRACT(DAY FROM a.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE())
         THEN 1 ELSE 0 END as is_lmtd,
    
    -- Last Year Month To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), MONTH)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
         AND EXTRACT(DAY FROM a.posting_date) <= EXTRACT(DAY FROM CURRENT_DATE())
         AND EXTRACT(MONTH FROM a.posting_date) = EXTRACT(MONTH FROM CURRENT_DATE())
         THEN 1 ELSE 0 END as is_lymtd,
    
    -- Last Year To Date Flag
    CASE WHEN a.posting_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
         AND a.posting_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
         THEN 1 ELSE 0 END as is_lytd,


-- M_1: Last Month (Full Month) - If current = August, M_1 = July (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_1,

-- M_2: Two Months Ago (Full Month) - If current = August, M_2 = June (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_2,

-- M_3: Three Months Ago (Full Month) - If current = August, M_3 = May (full month)
CASE WHEN DATE_TRUNC(a.posting_date, MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), MONTH)
     THEN 1 ELSE 0 END as is_m_3,

CASE WHEN DATE_TRUNC(a.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
     THEN 1 ELSE 0 END as is_y_1,

CASE WHEN DATE_TRUNC(a.posting_date, YEAR) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), YEAR)
     THEN 1 ELSE 0 END as is_y_2,


FROM {{ ref('stg_erp_value_entry') }} as a
LEFT JOIN  {{ ref('stg_erp_inbound_sales_header') }}  as b on a.document_no_ = b.documentno
LEFT JOIN {{ ref('int_erp_customer') }} AS c ON a.source_no_ = c.no_

--left join {{ ref('stg_erp_sales_invoice_line') }} as c on a.document_no_ = c.document_no_ and source_code = 'SALES' and a.document_line_no_ = c.line_no_

--left join {{ ref('stg_dimension_value') }} as d on  a.global_dimension_2_code = d.code
left join  {{ ref('int_items') }} as e on  e.item_no_ = a.item_no_




where a.item_ledger_entry_type = 'Sale' 
and a.source_code  IN ('BACKOFFICE', 'SALES')
and a.document_type NOT IN ('Sales Shipment', 'Sales Return Receipt')
and a.dimension_code = 'PROFITCENTER'

--and a.posting_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH) AND CURRENT_DATE()

--and   b.web_order_id = 'O3087438S'