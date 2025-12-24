with 

source_1 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_vendor_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

source_2 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_vendor_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

renamed_source_1 as (
    select
        no_,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        address,
        address_2,
        application_method,
        base_calendar_code,
        block_payment_tolerance,
        blocked,
        budgeted_amount,
        cash_flow_payment_terms_code,
        city,
        contact,
        country_region_code,
        county,
        creditor_no_,
        currency_code,
        currency_id,
        disable_search_by_name,
        document_sending_profile,
        e_mail,
        fax_no_,
        fin__charge_terms_code,
        gen__bus__posting_group,
        gln,
        global_dimension_1_code,
        global_dimension_2_code,
        home_page,
        ic_partner_code,
        id,
        image,
        invoice_disc__code,
        language_code,
        last_date_modified,
        last_modified_date_time,
        lead_time_calculation,
        location_code,
        name,
        name_2,
        no__series,
        our_account_no_,
        over_receipt_code,
        partner_type,
        pay_to_vendor_no_,
        payment_method_code,
        payment_method_id,
        payment_terms_code,
        payment_terms_id,
        phone_no_,
        picture,
        post_code,
        preferred_bank_account_code,
        prepayment__,
        price_calculation_method,
        prices_including_vat,
        primary_contact_no_,
        priority,
        privacy_blocked,
        purchaser_code,
        responsibility_center,
        search_name,
        shipment_method_code,
        shipping_agent_code,
        statistics_group,
        tax_area_code,
        tax_liable,
        telex_answer_back,
        telex_no_,
        territory_code,
        timestamp,
        validate_eu_vat_reg__no_,
        vat_bus__posting_group,
        vat_registration_no_,
        vendor_posting_group
    from source_1
),

renamed_source_2 as (
    select
        no_,
        vendor_type,
        lead_time__days_,
        review_time__days_,
        purchase_type,
        skip_for_slim4_integration,
        isslim4sync,
        timestamp as timestamp_source_2,
        _fivetran_deleted as _fivetran_deleted_source_2,
        _fivetran_synced as _fivetran_synced_source_2
    from source_2
),

joined as (
    select
        -- From source_1 (main vendor table)
        s1.no_,
        s1._fivetran_deleted,
        s1._fivetran_synced,
        s1._systemid,
        s1.address,
        s1.address_2,
        s1.application_method,
        s1.base_calendar_code,
        s1.block_payment_tolerance,
        s1.blocked,
        s1.budgeted_amount,
        s1.cash_flow_payment_terms_code,
        s1.city,
        s1.contact,
        s1.country_region_code,
        s1.county,
        s1.creditor_no_,
        s1.currency_code,
        s1.currency_id,
        s1.disable_search_by_name,
        s1.document_sending_profile,
        s1.e_mail,
        s1.fax_no_,
        s1.fin__charge_terms_code,
        s1.gen__bus__posting_group,
        s1.gln,
        s1.global_dimension_1_code,
        s1.global_dimension_2_code,
        s1.home_page,
        s1.ic_partner_code,
        s1.id,
        s1.image,
        s1.invoice_disc__code,
        s1.language_code,
        s1.last_date_modified,
        s1.last_modified_date_time,
        s1.lead_time_calculation,
        s1.location_code,
        s1.name,
        s1.name_2,
        s1.no__series,
        s1.our_account_no_,
        s1.over_receipt_code,
        s1.partner_type,
        s1.pay_to_vendor_no_,
        s1.payment_method_code,
        s1.payment_method_id,
        s1.payment_terms_code,
        s1.payment_terms_id,
        s1.phone_no_,
        s1.picture,
        s1.post_code,
        s1.preferred_bank_account_code,
        s1.prepayment__,
        s1.price_calculation_method,
        s1.prices_including_vat,
        s1.primary_contact_no_,
        s1.priority,
        s1.privacy_blocked,
        s1.purchaser_code,
        s1.responsibility_center,
        s1.search_name,
        s1.shipment_method_code,
        s1.shipping_agent_code,
        s1.statistics_group,
        s1.tax_area_code,
        s1.tax_liable,
        s1.telex_answer_back,
        s1.telex_no_,
        s1.territory_code,
        s1.timestamp,
        s1.validate_eu_vat_reg__no_,
        s1.vat_bus__posting_group,
        s1.vat_registration_no_,
        s1.vendor_posting_group,
        
        -- From source_2 (extended vendor fields)
        s2.lead_time__days_,
        s2.review_time__days_,
        case 
            when s2.purchase_type = 0 then 'Local'
            when s2.purchase_type = 1 then 'International'
            else null
        end as purchase_type,
        case 
            when s2.vendor_type = 0 then 'Trade'
            when s2.vendor_type = 1 then 'Service'
            when s2.vendor_type = 2 then 'Consignment'
            when s2.vendor_type = 3 then 'Other'
            else null
        end as vendor_type,
        s2.skip_for_slim4_integration,
        s2.isslim4sync
        
    from renamed_source_1 s1
    left join renamed_source_2 s2
        on s1.no_ = s2.no_
)

select * from joined
