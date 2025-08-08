select

a.document_no_,
a.line_no_,
a.gen__bus__posting_group,
a.gen__prod__posting_group,
a.amount,
a.a__rcd__not_inv__ex__vat__lcy_ as amount_rcd__not_inv,

a.quantity,
a.quantity_received,
a.quantity_invoiced,

--a.quantity__base_,



b.document_type,
b.status,
b.buy_from_vendor_no_,
b.buy_from_vendor_name,
b.vendor_authorization_no_,
b.location_code,
b.purchase_type,
b.drop_ship_order,
b.quality_status,
b.vendor_order_no_,
b.vendor_invoice_no_,
b.expected_receipt_date,
b.assigned_user_id,
b.document_date,
b.created_by_user,

from {{ ref('stg_petshop_purchase_line') }} as a
left join {{ ref('stg_petshop_purchase_header') }} as b on a.document_no_ = b.no_
--where a.document_no_ = 'TPS/2025/000809'