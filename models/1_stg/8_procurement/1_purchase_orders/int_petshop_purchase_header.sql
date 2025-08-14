
select
no_,
document_type,
status,
buy_from_vendor_no_,
buy_from_vendor_name,
vendor_authorization_no_,
location_code,
purchase_type,
drop_ship_order,
quality_status,
vendor_order_no_,
vendor_invoice_no_,
expected_receipt_date,
assigned_user_id,
document_date,
--Amount Calculated field Sum from Purchase Line table
--Amount Including VAT Calculated field Sum including tax from Purchase Line
created_by_user,


from {{ ref('stg_petshop_purchase_header') }}


--where no_ = 'TPS/2025/002043'



