{{
  config(
    materialized = 'view'
  )
}}

with customer_972 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
),

customer_18d as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
),

customer_f9d as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

customer_531 as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_230a02db_21eb_4476_8cfe_fef4ff006531') }}
),

final as (
    select
        -- Primary key
        a.no_,
        
        -- Customer base info
        a.name,
        a.name_2,
        a.e_mail,
        a.contact,
        a.phone_no_ as raw_phone_no_,
        
        -- Standardized phone number
        CASE
            WHEN REGEXP_CONTAINS(a.no_, r'^(CUST|CDN|BCN)') THEN '000000000000'
            WHEN a.phone_no_ IS NULL OR a.phone_no_ = '' THEN '000000000000'
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^5[0-9]{8}$') THEN CONCAT('971', a.phone_no_)  -- Add 971 prefix to valid standard numbers
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^971[0-9]{9}$') THEN a.phone_no_  -- Already in correct format
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^05[0-9]{8}$') THEN CONCAT('971', SUBSTR(a.phone_no_, 2))  -- Remove 0 and add 971 prefix
            WHEN c.phone_country_code IS NOT NULL AND c.phone_country_code NOT IN ('971', '+971') THEN '000000000000'
            ELSE '000000000000'  -- Invalid pattern cases
        END AS std_phone_no_,
        
        -- Phone status for analysis
        CASE
            WHEN REGEXP_CONTAINS(a.no_, r'^(CUST|CDN|BCN)') THEN 'Deferred (CUST|CDN|BCN)'
            WHEN a.phone_no_ IS NULL OR a.phone_no_ = '' THEN 'Missing Phone Number'
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^5[0-9]{8}$') THEN 'Valid - Standard (9 digits starting with 5)'
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^971[0-9]{9}$') THEN 'Valid - Needs Trim (12 digits With 971 Prefix)'
            WHEN REGEXP_CONTAINS(a.phone_no_, r'^05[0-9]{8}$') THEN 'Valid - Needs Trim (10 digits starting with 05)'
            WHEN c.phone_country_code IS NOT NULL AND c.phone_country_code NOT IN ('971', '+971') THEN 'Non-UAE Country'
            ELSE 'Invalid - Pattern Error'
        END AS phone_no_status,
        
        -- Address information
        a.address,
        a.address_2,
        a.city,
        a.post_code,
        a.county,
        a.country_region_code,
        
        -- Customer classification
        a.gen__bus__posting_group,
        a.customer_disc__group,
        a.customer_price_group,
        a.no__series,
        b.retail_customer_group,
        
        -- Loyalty information
        c.membership_card_no_,
        c.store_credit_gift_no_,
        --c.merged_phone_no,
        c.loyality_member_id,
        
        -- Phone related fields from all tables
        c.phone_country_code,
        c.secondary_phone_country_code,
        c.secondary_phone_no_,
        b.mobile_phone_no_,
        b.daytime_phone_no_,
        
        -- Web/Digital related fields
        c.source_application,
        c.source_type,
        c.user_auth__type,
        c.web_customer_no_,
        c.old_web_customer_no_,
        d.webid,
        d.applicationid,
        
        -- Customer management fields
        a.blocked,
        a.privacy_blocked,
        a.credit_limit__lcy_,
        a.currency_code,
        
        -- House/apartment information
        b.house_apartment_no_,
        
        -- Payment and financial information
        a.payment_method_code,
        a.payment_terms_code,
        a.vat_registration_no_,
        a.vat_bus__posting_group,
        a.tax_area_code,
        a.tax_liable,
        a.prices_including_vat,
        
        -- Shipping and delivery information
        a.shipment_method_code,
        a.shipping_agent_code,
        a.shipping_agent_service_code,
        a.shipping_time,
        a.location_code,
        
        -- Business related fields
        c.business_type,
        a.salesperson_code,
        a.territory_code,
        
        -- System fields
        a._systemid,
        b.customer_id,
        a.contact_id,
        a.id,
        
        -- Creation and modification info
        b.created_by_user,
        b.date_created,
        a.last_date_modified,
        a.last_modified_date_time,
        
        -- Original timestamps from each table
        a.timestamp as base_timestamp,
        b.timestamp as retail_timestamp,
        c.timestamp as loyalty_timestamp,
        d.timestamp as web_timestamp,
        
        -- Fivetran metadata
        a._fivetran_deleted,
        a._fivetran_synced,
        
        -- Additional fields from all tables (included for completeness)
        a.allow_line_disc_,
        a.amount,
        a.application_method,
        a.base_calendar_code,
        a.bill_to_customer_no_,
        a.block_payment_tolerance,
        a.budgeted_amount,
        a.cash_flow_payment_terms_code,
        a.chain_name,
        a.combine_shipments,
        a.contact_graph_id,
        a.contact_type,
        a.copy_sell_to_addr__to_qte_from,
        a.currency_id,
        a.disable_search_by_name,
        a.document_sending_profile,
        a.fax_no_,
        a.fin__charge_terms_code,
        a.gln,
        a.global_dimension_1_code,
        a.global_dimension_2_code,
        a.home_page,
        a.ic_partner_code,
        a.image,
        a.invoice_copies,
        a.invoice_disc__code,
        a.language_code,
        a.last_statement_no_,
        a.our_account_no_,
        a.partner_type,
        a.payment_method_id,
        a.payment_terms_id,
        a.picture,
        a.place_of_export,
        a.preferred_bank_account_code,
        a.prepayment__,
        a.price_calculation_method,
        a.primary_contact_no_,
        a.print_statements,
        a.priority,
        a.reminder_terms_code,
        a.reserve,
        a.responsibility_center,
        a.search_name,
        a.service_zone_code,
        a.ship_to_code,
        a.shipment_method_id,
        a.shipping_advice,
        a.statistics_group,
        a.tax_area_id,
        a.telex_answer_back,
        a.telex_no_,
        a.use_gln_in_electronic_document,
        a.validate_eu_vat_reg__no_,
        
        -- Additional retail fields
        b.amtchargedonposint,
        b.amtchargedpostedint,
        b.balancelcyint,
        b.default_weight,
        b.other_tender_in_finalizing,
        b.post_as_shipment,
        b.print_document_invoice,
        b.reason_code,
        b.restriction_functionality,
        b.transaction_limit,
        
        -- Additional loyalty fields
        c.customer_type,
        c.error_msg,
        c.modified_sync_with_web,
        c.retry_for_crm,
        c.retry_for_web,
        c.source_version,
        c.sync_with_crm,
        c.sync_with_web,
        c.occ_customer_address,
        c.occ_customer_address_2
    from 
        customer_972 as a
        left join customer_18d as b on a.no_ = b.no_
        left join customer_f9d as c on a.no_ = c.no_
        left join customer_531 as d on a.no_ = d.no_
)

select * from final

--where e_mail = 'amazontest@yopmail.com'