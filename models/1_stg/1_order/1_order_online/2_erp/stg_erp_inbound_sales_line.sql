with 
source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_inbound_sales_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),
renamed as (
    select
        -- Document/Order Identifiers
        documentno,
        invoice_no_,
        web_order_no_,
        ticketno,
        pnr,
        docket_no,
        
        -- Customer & Sales Info
        customer_no_,
        sales_location,
        packaging_location,
        user_id,
        businessoperationtype,
        
        -- Product/Item Details
        item_no_,
        item_id,
        type,
        inventory_type,
        
        -- Inventory/Tracking
        batch_id,
        bin_code,
        lot_no_,
        serial_no_,
        box_id,
        
        -- Quantities
        quantity,
        
        -- Pricing (Base)
        mrp_price,
        actual_price,
        unit_price_excl__vat,
        
        -- Discounts
        discount_amount,
        coupon_discount,
        invoice_discount,
        
        -- Additional Charges
        cod_charges,
        cod_charge_base_amount,
        shipping_charges,
        shipping_charge_base_amount,
        customization_charge,
        customization_base_amount,
        gift_charges,
        gift_base_amount,
        other_charges,
        other_charge_base_amount,
        collectable_amount,
        
        -- Tax/VAT
        tax__,
        tax_amount,
        tax_base_value,
        vat_on_cod_charges,
        vat_on_customization,
        vat_on_gift_charges,
        vat_on_other_charges,
        vat_on_shipping_charges,
        
        -- Invoice Totals
        invoice_value_excl__tax,
        invoice_value_incl__tax,
        
        -- Loyalty
        loyality_amount,
        loyality_point,
        
        -- Campaign
        campaign_id,
        
        -- Timestamps
        invoice_date,
        sales_order_created,
        box_creation_date_time,
        inserted_on,
        timestamp,
        
        -- System/Metadata Fields
        _fivetran_deleted,
        _fivetran_synced,
        _systemid
        
    from source
)
select * from renamed