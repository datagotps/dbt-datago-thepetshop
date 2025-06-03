
-- Just online orders, copme from OFS ssytem to ERP.

with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_inbound_sales_header_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
    where _fivetran_deleted is false

),

renamed as (

    select

        documentno,
        invoice_no_,
        web_order_id,
       -- order_type, -- 0,1,2,4
       -- type, --1,2
        
case 
 when type = 2 then  'Sales Return'
 when type = 1 then  'Sales'
 else 'Ask DataGo'
 end as type,

case 
 when order_type = 2 then  'EXPRESS'
 when order_type = 1 then  'NORMAL'
  when order_type = 4 then  '???'

 else 'Ask: anmar@8020datago.ai'
 end as order_type,
        
        

        order_delivered,

        order_date,
        shipped_date,
       
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        box_id,
        currency_code,
        currency_factor,
        customer_id,
        delivery_date,
        docket_no,
        dsp_code,
        error_message,
        global_dimension_2_code,
        nav_customer_id,
        order_category,
        order_created,
        
        packaging_location,
        payment_method_code,
        retry_count,
        return_before_delivered,
        shipped,
        shipped_gl,
        shipped_gl_doc_no_,
        ticket_type,
        ticketno,
        timestamp,
        synctoloyalitydetails,

        CASE
        WHEN global_dimension_2_code = 'D' THEN 'Website'
        WHEN global_dimension_2_code IN ('CRM', 'CRM Exchange', 'FOC') THEN 'CRM'
        WHEN global_dimension_2_code IN ('A', 'I') THEN 'Mobile App'
        ELSE global_dimension_2_code
        END AS online_order_channel,

    from source

)

select * from renamed
--where web_order_id= 'O30102245S'

--DIP-DT01-135461, DIP-DT01-145717, INV00442971, INV00460667, INV00461422, INV00461423, INV00461426
