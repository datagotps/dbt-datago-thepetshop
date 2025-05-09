with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        no_,
        phone_no_,
   


        name,
        name_2,
        


        e_mail,
        contact,

        

        gen__bus__posting_group, --DOMESTIC, RETAIL, PET-REL, P&M-SER, DOM-SER, B2B, AFFILIATES, LVC FOREIGN PETHAUS PVS ONLINE_CUSTOMER
        customer_disc__group, --DPFEMPDISC B2B FAMIYNFRND TPS100 STAFF DISC CRM20
        customer_price_group, --B2B STAFF AED AMMKT AMB2B AMFBA
        no__series, --CUST, CUSTOMER D, CUST-PRJ, CUST-PRE, B2B CUSTOMER, AF-CU

        country_region_code,


        _systemid,
        address,
        address_2,
        allow_line_disc_,
        amount,
        application_method,
        base_calendar_code,
        bill_to_customer_no_,
        block_payment_tolerance,
        blocked,
        budgeted_amount,
        cash_flow_payment_terms_code,
        chain_name,
        city,
        collection_method,
        combine_shipments,
        
        contact_graph_id,
        contact_id,
        contact_type,
        copy_sell_to_addr__to_qte_from,
        
        county,
        credit_limit__lcy_,
        currency_code,
        currency_id,

        disable_search_by_name,
        document_sending_profile,
        
        fax_no_,
        fin__charge_terms_code,
        
        gln,
        global_dimension_1_code,
        global_dimension_2_code,
        home_page,
        ic_partner_code,
        id,
        image,
        invoice_copies,
        invoice_disc__code,
        language_code,
        last_date_modified,
        last_modified_date_time,
        last_statement_no_,
        location_code,
        
        
        our_account_no_,
        partner_type,
        payment_method_code,
        payment_method_id,
        payment_terms_code,
        payment_terms_id,
        
        picture,
        place_of_export,
        post_code,
        preferred_bank_account_code,
        prepayment__,
        price_calculation_method,
        prices_including_vat,
        primary_contact_no_,
        print_statements,
        priority,
        privacy_blocked,
        reminder_terms_code,
        reserve,
        responsibility_center,
        salesperson_code,
        search_name,
        service_zone_code,
        ship_to_code,
        shipment_method_code,
        shipment_method_id,
        shipping_advice,
        shipping_agent_code,
        shipping_agent_service_code,
        shipping_time,
        statistics_group,
        tax_area_code,
        tax_area_id,
        tax_liable,
        telex_answer_back,
        telex_no_,
        territory_code,
        timestamp,
        use_gln_in_electronic_document,
        validate_eu_vat_reg__no_,
        vat_bus__posting_group,
        vat_registration_no_
        _fivetran_deleted,
        _fivetran_synced,


    from source

)

select * from renamed


--where no_ = 'BCN/2025/000070'

--where e_mail= 'marylougoebl@hotmail.com'