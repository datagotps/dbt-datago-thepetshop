--recorder 25938372
select

a.source_no_,
a.document_no_,
a.posting_date,
a.sales_amount__actual_,
a.sales_channel, -- Online or Shop
a.offline_order_channel, --store location

a.source_code,
a.item_ledger_entry_type,

b.web_order_id,
b.online_order_channel, --website, Android, iOS, CRM, Unmapped
b.order_type, --EXPRESS, NORMAL, EXCHANGE
b.paymentgateway, -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
b.paymentmethodcode, -- PREPAID, COD, creditCard


c.name,
c.raw_phone_no_,
c.customer_identity_status




--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount else 0 end as online_line_amount_excl_vat,
--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount_including_vat else 0 end as online_line_amount_enclu_vat,

--d.name,
--count(*)

FROM {{ ref('stg_erp_value_entry') }} as a
LEFT JOIN  {{ ref('stg_erp_inbound_sales_header') }}  as b on a.document_no_ = b.documentno
LEFT JOIN {{ ref('int_erp_customer') }} AS c ON a.source_no_ = c.no_

--left join {{ ref('stg_erp_sales_invoice_line') }} as c on a.document_no_ = c.document_no_ and source_code = 'SALES' and a.document_line_no_ = c.line_no_

--left join {{ ref('stg_petshop_dimension_value') }} as d on  a.global_dimension_2_code = d.code
left join  {{ ref('int_items') }} as e on  e.item_no_ = a.item_no_