with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_sales_invoice_header_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select

    no_, --(Invoice Number)
    order_no_, --(Order Number)
    sell_to_customer_no_,

--date
    order_date,
    posting_date,
    document_date,
    due_date,
    shipment_date,

    customer_posting_group,

    location_code,




        
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        allow_line_disc_,
        applies_to_doc__no_,
        applies_to_doc__type,
        area,
        bal__account_no_,
        bal__account_type,
        bill_to_address,
        bill_to_address_2,
        bill_to_city,
        bill_to_contact,
        bill_to_contact_no_,
        bill_to_country_region_code,
        bill_to_county,
        bill_to_customer_no_,
        bill_to_name,
        bill_to_name_2,
        bill_to_post_code,
        campaign_no_,
        correction,
        coupled_to_crm,
        currency_code,
        currency_factor,
        cust__ledger_entry_no_,
        customer_disc__group,
        
        customer_price_group,
        dimension_set_id,
        direct_debit_mandate_id,
        doc__exch__original_identifier,
        
        document_exchange_identifier,
        document_exchange_status,
        draft_invoice_systemid,
        
        eu_3_party_trade,
        exit_point,
        external_document_no_,
        gen__bus__posting_group,
        get_shipment_used,
        id,
        invoice_disc__code,
        invoice_discount_calculation,
        invoice_discount_value,
        language_code,
        
        no__printed,
        no__series,
        on_hold,
        opportunity_no_,
        
        
        order_no__series,
        package_tracking_no_,
        payment_discount__,
        payment_instructions,
        payment_instructions_name,
        payment_method_code,
        payment_reference,
        payment_service_set_id,
        payment_terms_code,
        pmt__discount_date,
        
        posting_description,


        pre_assigned_no_,
        pre_assigned_no__series,
        prepayment_invoice,
        prepayment_no__series,
        prepayment_order_no_,
        price_calculation_method,
        prices_including_vat, --1.0


        quote_no_,
        reason_code,
        responsibility_center,
        salesperson_code,
        sell_to_address,
        sell_to_address_2,
        sell_to_city,
        sell_to_contact,
        sell_to_contact_no_,
        sell_to_country_region_code,
        sell_to_county,
        sell_to_customer_name,
        sell_to_customer_name_2,
        
        sell_to_e_mail,
        sell_to_phone_no_,
        sell_to_post_code,
        ship_to_address,
        ship_to_address_2,
        ship_to_city,
        ship_to_code,
        ship_to_contact,
        ship_to_country_region_code,
        ship_to_county,
        ship_to_name,
        ship_to_name_2,
        ship_to_post_code,
        
        shipment_method_code,
        shipping_agent_code,
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,


        source_code,
        tax_area_code,
        tax_liable,
        timestamp,
        transaction_specification,
        transaction_type,
        transport_method, 
        user_id,
        vat_base_discount__,
        vat_bus__posting_group,
        vat_country_region_code,
        vat_registration_no_,
        work_description,
        your_reference

    from source

)

select * from renamed

--where no_= 'PSI/2025/00281' 

--where no_ = 'STMTCRK/001106'
--where sell_to_customer_no_ = 'C000008816'