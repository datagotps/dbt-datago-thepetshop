with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_inbound_sales_header_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
    where _fivetran_deleted is false


),

renamed as (

    select
        -- Document/Order Identifiers
        documentno,
        invoice_no_,
        web_order_id,
        ticketno,
        docket_no,
        
        -- Customer Information
        customer_id,
        nav_customer_id,
        
        -- Order Classification
        type,
        --order_type,
        order_category,
        ticket_type,
        businessoperationtype,
        
        -- Location & Delivery
        packaging_location,
        dsp_code,
        global_dimension_2_code,
        
        -- Payment & Currency
        payment_method_code,
        currency_code,
        currency_factor,
        
        -- Order Status & Flags
        shipped,
        return_before_delivered,
        synctoloyalitydetails,
        
        -- Dates & Timestamps
        order_date,
        order_created,
        delivery_date,
        order_delivered,
        shipped_date,
        timestamp,
        
        -- Shipping/GL Details
        shipped_gl,
        shipped_gl_doc_no_,
        
        -- Package Tracking
        box_id,
        
        -- Error Handling
        error_message,
        retry_count,
        
        -- System/Metadata Fields
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,



        CASE
        WHEN global_dimension_2_code = 'D' THEN 'Website'
        WHEN global_dimension_2_code IN ('CRM', 'CRM Exchange', 'FOC') THEN 'CRM'
        WHEN global_dimension_2_code = 'A' THEN 'Android'
        WHEN global_dimension_2_code = 'I' THEN 'iOS'
        WHEN global_dimension_2_code = '' then 'Unmapped'
        ELSE global_dimension_2_code
        END AS online_order_channel,


        CASE 
             WHEN type = 2 THEN 'RETURN'
             WHEN type = 1 THEN 'SALE'
              ELSE 'OTHER'
        END AS transaction_type,


    case 
        when order_type = 2 then  'EXPRESS'
        when order_type = 1 then  'NORMAL'
        when order_type = 4 then  'EXCHANGE'
        else 'OTHER'
    end as order_type,

    from source

)

select * from renamed

--where web_order_id = 'O30163245S'