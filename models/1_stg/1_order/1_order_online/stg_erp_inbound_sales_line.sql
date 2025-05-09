with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_inbound_sales_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        web_order_no_,
        item_id,
        invoice_date,
        item_no_,
        customer_no_,

        documentno,
        invoice_no_,
        serial_no_,

        discount_amount, 
    
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

        mrp_price, -- not correct.



        collectable_amount,
        
        
        
        
        
        
        
        
        
        

        
        type,
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

--where web_order_no_ = 'O3075972S' --and item_id = 5356629