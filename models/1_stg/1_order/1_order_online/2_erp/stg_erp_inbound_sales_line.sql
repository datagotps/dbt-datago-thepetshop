with 

source as (

    select 
    a.* ,
    b.nav_customer_id,
    from {{ source('sql_erp_prod_dbo', 'petshop_inbound_sales_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }} as a
    LEFT JOIN  {{ ref('stg_erp_inbound_sales_header') }}  as b on a.documentno = b.documentno and a.type = 1 
   

),

renamed as (

    select
        web_order_no_,
        documentno,
        invoice_no_,
        type, --1,2
        case 
        when type = 1 then 'Sales'
        when type = 2 then 'Return Sales'
        else 'cheack my logic'
        end as type_2,

        nav_customer_id,

        item_id,
        invoice_date,
        item_no_,
        customer_no_,

        
        
        serial_no_,

        discount_amount, -- incluvat
    
        coupon_discount,  -- repereted
        invoice_discount, -- not work string.
        


        
        lot_no_,

        
        invoice_value_excl__tax,
        invoice_value_incl__tax,
        shipping_charge_base_amount,
        shipping_charges,
        tax__,
        tax_amount,
        tax_base_value,

        unit_price_excl__vat,

        mrp_price, -- not correct.  =  invoice_value_incl__tax



        collectable_amount,
        
        
        
        
        
        
        
        
        
        

        
        

        _systemid,
        actual_price,
        batch_id,
        bin_code,
        box_creation_date_time,
        box_id,
        campaign_id,
        cod_charge_base_amount,
        cod_charges,






        
        customization_base_amount,
        customization_charge,
        

        docket_no,
        gift_base_amount,
        gift_charges,
        inserted_on,
        inventory_type,
        
        
        other_charge_base_amount,
        other_charges,
        packaging_location,
        pnr,
        quantity,
        sales_location,
        sales_order_created,
        
        ticketno,
        timestamp,
        user_id,
        vat_on_cod_charges,
        vat_on_customization,
        vat_on_gift_charges,
        vat_on_other_charges,
        vat_on_shipping_charges,
        loyality_amount,
        loyality_point


        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed


--where  web_order_no_ = 'O3067781S'
--where documentno = 'INV00426682'
--WHERE item_no_ ='21220000'