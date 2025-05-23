with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_purchase_header_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        document_type,
        no_,
        status,
        last_receiving_no_,

        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        applies_to_doc__no_,
        applies_to_doc__type,
        applies_to_id,
        area,
        assigned_user_id,
        bal__account_no_,
        bal__account_type,
        buy_from_address,
        buy_from_address_2,
        buy_from_city,
        buy_from_contact,
        buy_from_contact_no_,
        buy_from_country_region_code,
        buy_from_county,
        buy_from_ic_partner_code,
        buy_from_post_code,
        buy_from_vendor_name,
        buy_from_vendor_name_2,
        buy_from_vendor_no_,
        campaign_no_,
        compress_prepayment,
        correction,
        creditor_no_,
        currency_code,
        currency_factor,
        dimension_set_id,
        doc__no__occurrence,
        document_date,
        due_date,
        entry_point,
        expected_receipt_date,
        gen__bus__posting_group,
        ic_direction,
        ic_status,
        id,
        inbound_whse__handling_time,
        incoming_document_entry_no_,
        invoice,
        invoice_disc__code,
        invoice_discount_calculation,
        invoice_discount_value,
        job_queue_entry_id,
        job_queue_status,
        language_code,
        last_posting_no_,
        last_prepayment_no_,
        last_prepmt__cr__memo_no_,
        
        last_return_shipment_no_,
        lead_time_calculation,
        location_code,
        no__printed,
        no__series,
        on_hold,
        order_address_code,
        order_class,
        order_date,
        pay_to_address,
        pay_to_address_2,
        pay_to_city,
        pay_to_contact,
        pay_to_contact_no_,
        pay_to_country_region_code,
        pay_to_county,
        pay_to_ic_partner_code,
        pay_to_name,
        pay_to_name_2,
        pay_to_post_code,
        pay_to_vendor_no_,
        payment_discount__,
        payment_method_code,
        payment_reference,
        payment_terms_code,
        pmt__discount_date,
        posting_date,
        posting_description,
        posting_from_whse__ref_,
        posting_no_,
        posting_no__series,
        prepayment__,
        prepayment_due_date,
        prepayment_no_,
        prepayment_no__series,
        prepmt__cr__memo_no_,
        prepmt__cr__memo_no__series,
        prepmt__payment_discount__,
        prepmt__payment_terms_code,
        prepmt__pmt__discount_date,
        prepmt__posting_description,
        price_calculation_method,
        prices_including_vat,
        print_posted_documents,
        promised_receipt_date,
        purchaser_code,
        quote_no_,
        reason_code,
        receive,
        receiving_no_,
        receiving_no__series,
        requested_receipt_date,
        responsibility_center,
        return_shipment_no_,
        return_shipment_no__series,
        sell_to_customer_no_,
        send_ic_document,
        ship,
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
        shortcut_dimension_1_code,
        shortcut_dimension_2_code,
        
        tax_area_code,
        tax_liable,
        timestamp,
        transaction_specification,
        transaction_type,
        transport_method,
        vat_base_discount__,
        vat_bus__posting_group,
        vat_country_region_code,
        vat_registration_no_,
        vendor_authorization_no_,
        vendor_cr__memo_no_,
        vendor_invoice_no_,
        vendor_order_no_,
        vendor_posting_group,
        vendor_shipment_no_,
        your_reference

    from source

)

select * from renamed
--where no_ = 'TPS/2025/001217'
--where no_ = 'HQWGP/21101'

-- Purchase Order
--Posted Purchase Receipt
--https://erp.thepetstore.com/petshop/?node=0000233e-df25-0000-0c6c-5800836bd2d2&page=9307&company=PetShop&dc=0


---sql_erp_prod_dbo.petshop_posted_whse__receipt_header_437dbf0e_84ff_417a_965d_ed2bb9650972
