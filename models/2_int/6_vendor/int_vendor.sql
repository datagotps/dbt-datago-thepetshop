-- ══════════════════════════════════════════════════════════════════════════════
-- int_vendor.sql
-- Purpose: Intermediate vendor model with Purchase Type and Vendor Type from ERP source
-- Prepares vendor data for dim_vendor dimension table
-- ══════════════════════════════════════════════════════════════════════════════

select
    -- CORE IDENTIFIERS
    no_ as vendor_code,
    name as vendor_name,
    search_name as vendor_search_name,
    
    -- STATUS
    case when blocked = 0 then true else false end as is_active,
    case 
        when blocked = 0 then 'Active'
        else 'Blocked'
    end as vendor_status,
    
    -- PURCHASE TYPE (International/Local) - Direct from ERP source
    purchase_type,
    
    -- VENDOR TYPE (Trade/Service/Consignment/Other) - Direct from ERP source
    vendor_type,
    
    -- CATEGORIZATION
    gen__bus__posting_group as business_posting_group,
    vendor_posting_group,
    vat_bus__posting_group as vat_posting_group,
    
    -- GEOGRAPHY
    country_region_code as country_code,
    city,
    post_code as postal_code,
    concat(
        coalesce(address, ''),
        case when address_2 is not null and address_2 != '' then ', ' || address_2 else '' end
    ) as full_address,
    
    -- FINANCIAL
    currency_code,
    payment_terms_code,
    
    -- CONTACT
    e_mail as email,
    phone_no_ as phone,
    
    -- COMPLIANCE
    vat_registration_no_ as vat_registration_no,
    
    -- HIERARCHY
    pay_to_vendor_no_ as pay_to_vendor_code,
    
    -- ADDITIONAL FIELDS FROM SOURCE
    lead_time__days_,
    review_time__days_,
    
    -- METADATA
    last_date_modified,
    last_modified_date_time,
    _fivetran_synced

from {{ ref('stg_petshop_vendor') }}
where _fivetran_deleted = false

