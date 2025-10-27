
select
-- User Identifiers & Demographics
user_id,                           -- dim (unique user identifier)
user_email,                        -- dim (user email address)
user_full_name,                    -- dim (concatenated first + last name)
loyalty_member_id,                 -- dim (OpenLoyalty member ID or null)

-- User Profile Attributes
country,                           -- dim (user country location)
nationality,                       -- dim (user nationality)
preferred_language,                -- dim (user language preference)
user_gender,                       -- dim (user gender)
user_age,                          -- fact (calculated age in years)

-- Account Status Flags
is_active,                        -- dim (true/false - account active)
is_guest,                          -- dim (true/false - guest account)
is_email_verified,                -- dim (true/false - email verified)
is_phone_verified,                -- dim (true/false - phone verified)
is_profile_complete,               -- dim (true/false - profile completed)
notifications_enabled,             -- dim (true/false - notifications enabled)

-- Pet Ownership Metrics
total_pets,                       -- fact (count of all pets owned)
active_pets,                      -- fact (count of non-deleted pets)
completed_pet_profiles,            -- fact (count of pets with complete profiles)
vaccinated_pets,                  -- fact (count of vaccinated pets)
neutered_pets,                    -- fact (count of neutered pets)
avg_pet_age,                      -- fact (average age of pets)
avg_pet_weight,                   -- fact (average weight of pets in kg)

-- Vaccination Management Metrics
total_vaccinations,               -- fact (count of all vaccinations recorded)
upcoming_vaccinations,            -- fact (count of future scheduled vaccinations)
overdue_vaccinations,             -- fact (count of past due vaccinations)
vaccination_compliance,           -- dim (No Vaccinations/Non-Compliant/Compliant/Fully Vaccinated)

-- Document Management Metrics
total_documents,                  -- fact (count of uploaded documents)
document_types_used,              -- fact (count of distinct document types)

-- Loyalty Program Status
profile_reward_claimed,           -- dim (true/false - profile completion reward claimed)
first_pet_reward_claimed,         -- dim (true/false - first pet addition reward claimed)

-- Engagement & Activity Dates
account_age_days,                -- fact (days since account creation)
days_since_last_update,          -- fact (days since last user activity)
user_created_date,               -- dim (account creation date)
profile_completed_date,          -- dim (profile completion date or null)
first_pet_added_date,            -- dim (date of first pet addition)
last_pet_added_date,             -- dim (date of most recent pet addition)
last_pet_update_date,            -- dim (date of most recent pet update)
last_vaccination_date,           -- dim (date of most recent vaccination)
last_document_upload_date,       -- dim (date of most recent document upload)

-- Segmentation & Scoring
engagement_level,                -- dim (Highly Engaged/Moderately Engaged/Low Engagement/Inactive)
user_segment,                    -- dim (No Pets/Single Pet Owner/Multi Pet Owner/Pet Enthusiast)

-- System Metadata
snapshot_date,                    -- dim (current date when snapshot was taken)


DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at, 


FROM {{ ref('int_superapp_users') }}
