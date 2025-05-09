with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        no_,

        membership_card_no_,
        store_credit_gift_no_,

        case when membership_card_no_ = '' then store_credit_gift_no_ else membership_card_no_ end as merged_phone_no,

        --merged_phone_no
        --std_phone_no



        source_application, --OCC, SHOPIFY, CRM, NAV-POS, LOYALTYAPP, null
        source_type, --ios, android, web
        user_auth__type, --email, organic, facebook, google

        web_customer_no_,
        old_web_customer_no_,
        loyality_member_id,

        secondary_phone_country_code,
        secondary_phone_no_,
        
        customer_type, --all 0 
        error_msg,
        
        modified_sync_with_web,
        phone_country_code,
        retry_for_crm,
        retry_for_web,
        
        
        source_version,
        sync_with_crm,
        sync_with_web,
        timestamp,
        occ_customer_address_2,
        occ_customer_address,
        
        business_type
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed

--where membership_card_no_ != store_credit_gift_no_
--where no_ = 'C000066928'
