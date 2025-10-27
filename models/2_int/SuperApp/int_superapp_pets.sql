-- models/marts/core/fact_pet_snapshot.sql
with pets as (
    select * from {{ ref('stg_pets') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

pet_types as (
    select * from {{ ref('stg_pet_type') }}
),

pet_subtypes as (
    select * from {{ ref('stg_pet_subtype') }}
),

activity_levels as (
    select * from {{ ref('stg_activity_level') }}
),

health_conditions_agg as (
    select 
        petid,
        count(*) as total_health_conditions
    from {{ ref('stg_pet_health_conditions') }}
    
    group by petid
),

vaccinations_agg as (
    select 
        petid,
        max(vaccinatedon) as last_vaccination_date,
        min(case when nextvaccinedate > current_date then nextvaccinedate end) as next_vaccination_due
    from {{ ref('stg_pet_vaccination') }}
    
    group by petid
),

allergies_agg as (
    select 
        petid,
        count(*) as total_allergies
    from {{ ref('stg_pet_allergies') }}
    
    group by petid
),

personality_traits_agg as (
    select 
        petid,
        count(*) as total_personality_traits
    from {{ ref('stg_pet_personality_traits') }}
    
    group by petid
),

documents_agg as (
    select 
        petid,
        count(*) as total_documents
    from {{ ref('stg_pet_document') }}
    
    group by petid
),

images_agg as (
    select 
        petid,
        count(*) as total_images,
        max(case when isprimary = true then 1 else 0 end) as has_primary_image
    from {{ ref('stg_pet_image') }}
    
    group by petid
)

select 
    -- Pet core attributes
    p.id as pet_id,
    p.userid as user_id,
    p.name as pet_name,
    p.age as pet_age,
    p.weight as pet_weight,
    p.gender as pet_gender,
    p.birthdate as pet_birthdate,
    p.size as pet_size,
    p.isvaccinated as is_vaccinated,
    p.isneutered as is_neutered,
    p.hasmicrochip as has_microchip,
    p.microchip as microchip_number,
    p.isprofilecomplete as profile_completion_status,
    p.profilecompletedat as profile_completed_date,
    p.notes as pet_notes,
    
    -- Owner information
    u.firstname as owner_first_name,
    u.lastname as owner_last_name,
    u.email as owner_email,
    u.country as owner_country,
    u.phone as owner_phone,
    u.isactive as owner_is_active,
    u.isprofilecomplete as owner_profile_complete,
    u.openloyaltymemberid as owner_loyalty_member_id,
    u.moengageid as owner_moengage_id,
    u.gender as owner_gender,
    u.birthdate as owner_birthdate,
    u.nationality as owner_nationality,
    u.preferredlanguage as owner_preferred_language,
    
    -- Pet type information
    pt.label as pet_type_name,
    ps.label as pet_subtype_name,
    p.customsubtype is not null as is_custom_subtype,
    p.customsubtype as custom_subtype_value,
    
    -- Activity level
    al.label as activity_level_name,
    
    -- Custom preferences (text fields)
    p.customdietarypreferences as custom_dietary_preferences,
    p.customallergies as custom_allergies,
    p.customhealthconditions as custom_health_conditions,
    
    -- Aggregated health metrics
    coalesce(hc.total_health_conditions, 0) as total_health_conditions,
    coalesce(alg.total_allergies, 0) as total_allergies,
    coalesce(pt_agg.total_personality_traits, 0) as total_personality_traits,
    
    -- Vaccination metrics
    v.last_vaccination_date,
    v.next_vaccination_due,
    DATE_DIFF(v.next_vaccination_due, CURRENT_DATE(), DAY) as days_until_next_vaccination,
    
    -- Document and image metrics
    coalesce(d.total_documents, 0) as total_documents,
    coalesce(i.total_images, 0) as total_images,
    coalesce(i.has_primary_image, 0) as has_primary_image,
    
    -- Calculated fields with proper type casting
    DATE_DIFF(CURRENT_DATE(), p.birthdate, YEAR) as calculated_age_years,
    DATE_DIFF(CURRENT_DATE(), p.birthdate, MONTH) as calculated_age_months,
    DATE_DIFF(CURRENT_DATE(), CAST(p.createdat AS DATE), DAY) as pet_account_age_days,
    DATE_DIFF(CURRENT_DATE(), CAST(u.createdat AS DATE), DAY) as owner_account_age_days,
    
    -- Status flags
    case 
        when v.next_vaccination_due < CURRENT_DATE() then 'Overdue'
        when DATE_DIFF(v.next_vaccination_due, CURRENT_DATE(), DAY) <= 30 then 'Due Soon'
        when v.next_vaccination_due is null then 'No Schedule'
        else 'Up to Date'
    end as vaccination_status,
    
    case 
        when p.deletedat is not null then 'Deleted'
        when p.isprofilecomplete = false then 'Incomplete'
        else 'Active'
    end as pet_status,
    
    -- Metadata with proper type casting
    p.createdat as pet_created_date,
    p.updatedat as pet_updated_date,
    p.deletedat as pet_deleted_date,
    u.createdat as user_created_date,
    u.updatedat as user_updated_date,
    CURRENT_DATE() as snapshot_date,
    CURRENT_TIMESTAMP() as snapshot_timestamp

from pets p
left join users u 
    on p.userid = u.id
left join pet_types pt 
    on p.typeid = pt.id
left join pet_subtypes ps 
    on p.subtypeid = ps.id
left join activity_levels al 
    on p.activitylevelid = al.id
left join health_conditions_agg hc 
    on p.id = hc.petid
left join allergies_agg alg 
    on p.id = alg.petid
left join personality_traits_agg pt_agg 
    on p.id = pt_agg.petid
left join vaccinations_agg v 
    on p.id = v.petid
left join documents_agg d 
    on p.id = d.petid
left join images_agg i 
    on p.id = i.petid

