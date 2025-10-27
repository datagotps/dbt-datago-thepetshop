-- models/marts/core/fact_user_engagement.sql
with user_base as (
    select * from {{ ref('stg_users') }}
   
),

pet_stats as (
    select 
        userid,
        count(*) as total_pets,
        count(case when  deletedat is null then 1 end) as active_pets,
        count(case when isvaccinated = true then 1 end) as vaccinated_pets,
        count(case when isneutered = true then 1 end) as neutered_pets,
        count(case when isprofilecomplete = true then 1 end) as completed_pet_profiles,
        -- Cast string columns to numeric for averaging
        avg(SAFE_CAST(age AS FLOAT64)) as avg_pet_age,
        avg(SAFE_CAST(weight AS FLOAT64)) as avg_pet_weight,
        min(CAST(createdat AS DATE)) as first_pet_added_date,
        max(CAST(createdat AS DATE)) as last_pet_added_date,
        max(CAST(updatedat AS DATE)) as last_pet_update_date
    from {{ ref('stg_pets') }}
    group by userid
),

vaccination_stats as (
    select 
        p.userid,
        count(v.id) as total_vaccinations,
        count(case when v.nextvaccinedate >= CURRENT_DATE() then 1 end) as upcoming_vaccinations,
        count(case when v.nextvaccinedate < CURRENT_DATE() then 1 end) as overdue_vaccinations,
        max(v.vaccinatedon) as last_vaccination_date
    from {{ ref('stg_pet_vaccination') }} v
    join {{ ref('stg_pets') }} p on v.petid = p.id
    
    group by p.userid
),

document_stats as (
    select 
        p.userid,
        count(d.id) as total_documents,
        count(distinct d.type) as document_types_used,
        max(CAST(d.createdat AS DATE)) as last_document_upload_date
    from {{ ref('stg_pet_document') }} d
    join {{ ref('stg_pets') }} p on d.petid = p.id
    
    group by p.userid
)

select 
    -- User identifiers
    u.id as user_id,
    u.email as user_email,
    concat(u.firstname, ' ', u.lastname) as user_full_name,
    
    -- User profile
    u.country,
    u.preferredlanguage as preferred_language,
    u.gender as user_gender,
    u.nationality,
    DATE_DIFF(CURRENT_DATE(), u.birthdate, YEAR) as user_age,
    
    -- Account status
    u.isactive as is_active,
    u.isguest as is_guest,
    u.isemailverified as is_email_verified,
    u.isphoneverified as is_phone_verified,
    u.isprofilecomplete as is_profile_complete,
    u.isnotificationsenabled as notifications_enabled,
    
    -- Engagement metrics
    DATE_DIFF(CURRENT_DATE(), CAST(u.createdat AS DATE), DAY) as account_age_days,
    DATE_DIFF(CURRENT_DATE(), CAST(u.updatedat AS DATE), DAY) as days_since_last_update,
    coalesce(ps.total_pets, 0) as total_pets,
    coalesce(ps.active_pets, 0) as active_pets,
    coalesce(ps.completed_pet_profiles, 0) as completed_pet_profiles,
    
    -- Pet metrics (rounded for cleaner display)
    ROUND(ps.avg_pet_age, 1) as avg_pet_age,
    ROUND(ps.avg_pet_weight, 2) as avg_pet_weight,
    coalesce(ps.vaccinated_pets, 0) as vaccinated_pets,
    coalesce(ps.neutered_pets, 0) as neutered_pets,
    
    -- Vaccination engagement
    coalesce(vs.total_vaccinations, 0) as total_vaccinations,
    coalesce(vs.upcoming_vaccinations, 0) as upcoming_vaccinations,
    coalesce(vs.overdue_vaccinations, 0) as overdue_vaccinations,
    
    -- Document engagement
    coalesce(ds.total_documents, 0) as total_documents,
    coalesce(ds.document_types_used, 0) as document_types_used,
    
    -- Loyalty program
    u.openloyaltymemberid as loyalty_member_id,
    u.openloyaltyprofilerewarded as profile_reward_claimed,
    u.openloyaltyfirstpetrewarded as first_pet_reward_claimed,
    
    -- Important dates
    CAST(u.createdat AS DATE) as user_created_date,
    CAST(u.profilecompletedat AS DATE) as profile_completed_date,
    ps.first_pet_added_date,
    ps.last_pet_added_date,
    ps.last_pet_update_date,
    vs.last_vaccination_date,
    ds.last_document_upload_date,
    
    -- Engagement scoring
    case 
        when ps.active_pets > 0 and vs.overdue_vaccinations = 0 and DATE_DIFF(CURRENT_DATE(), CAST(u.updatedat AS DATE), DAY) <= 30 then 'Highly Engaged'
        when ps.active_pets > 0 and DATE_DIFF(CURRENT_DATE(), CAST(u.updatedat AS DATE), DAY) <= 90 then 'Moderately Engaged'
        when ps.active_pets > 0 then 'Low Engagement'
        else 'Inactive'
    end as engagement_level,
    
    -- User segmentation
    case 
        when ps.total_pets = 0 then 'No Pets'
        when ps.total_pets = 1 then 'Single Pet Owner'
        when ps.total_pets between 2 and 3 then 'Multi Pet Owner'
        else 'Pet Enthusiast'
    end as user_segment,
    
    -- Vaccination compliance
    case 
        when vs.total_vaccinations = 0 then 'No Vaccinations'
        when vs.overdue_vaccinations > 0 then 'Non-Compliant'
        when vs.upcoming_vaccinations > 0 then 'Compliant'
        else 'Fully Vaccinated'
    end as vaccination_compliance,
    
    CURRENT_DATE() as snapshot_date

from user_base u
left join pet_stats ps on u.id = ps.userid
left join vaccination_stats vs on u.id = vs.userid
left join document_stats ds on u.id = ds.userid


--where u.email= 'lmtyimjn@gmail.com'