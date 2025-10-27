select
-- Pet Identifiers
pet_id,                            -- dim (unique pet identifier)
user_id,                           -- dim (owner user ID - foreign key)
pet_name,                          -- dim (pet's name)

-- Pet Demographics & Characteristics
pet_type_name,                    -- dim (dog/cat/bird etc.)
pet_subtype_name,                 -- dim (breed/specific subtype)
is_custom_subtype,                -- dim (true/false - using custom subtype)
custom_subtype_value,              -- dim (custom breed if specified)
pet_gender,                       -- dim (male/female/unknown)
pet_birthdate,                     -- dim (date of birth)
pet_age,                          -- dim (age value from system)
calculated_age_years,              -- fact (calculated age in years)
calculated_age_months,             -- fact (calculated age in months)
pet_size,                         -- dim (small/medium/large)
pet_weight,                       -- fact (weight in kg)
activity_level_name,               -- dim (sedentary/moderate/active/very active)

-- Pet Health Status
is_vaccinated,                    -- dim (true/false - vaccination status)
is_neutered,                      -- dim (true/false - neutered/spayed)
has_microchip,                    -- dim (true/false - microchipped)
microchip_number,                 -- dim (microchip ID if exists)
profile_completion_status,        -- dim (true/false - profile complete)
pet_status,                       -- dim (Active/Incomplete/Deleted)
pet_notes,                        -- dim (free text notes about pet)

-- Custom Health Information
custom_dietary_preferences,       -- dim (custom dietary text)
custom_allergies,                 -- dim (custom allergies text)
custom_health_conditions,         -- dim (custom health conditions text)

-- Health Metrics Aggregated
total_health_conditions,          -- fact (count of health conditions)
total_allergies,                  -- fact (count of allergies)
total_personality_traits,         -- fact (count of personality traits)

-- Vaccination Management
last_vaccination_date,            -- dim (most recent vaccination date)
next_vaccination_due,             -- dim (next scheduled vaccination)
days_until_next_vaccination,      -- fact (days remaining to next vaccine)
vaccination_status,               -- dim (Overdue/Due Soon/No Schedule/Up to Date)

-- Media & Documentation
total_documents,                  -- fact (count of documents uploaded)
total_images,                     -- fact (count of images uploaded)
has_primary_image,                -- dim (0/1 - has profile image)

-- Owner Information
owner_first_name,                 -- dim (owner's first name)
owner_last_name,                  -- dim (owner's last name)
owner_email,                      -- dim (owner's email)
owner_country,                    -- dim (owner's country)
owner_phone,                      -- dim (owner's phone)
owner_gender,                     -- dim (owner's gender)
owner_birthdate,                  -- dim (owner's birthdate)
owner_nationality,                -- dim (owner's nationality)
owner_preferred_language,         -- dim (owner's language preference)

-- Owner Account Status
owner_is_active,                  -- dim (true/false - owner account active)
owner_profile_complete,           -- dim (true/false - owner profile complete)
owner_loyalty_member_id,          -- dim (loyalty program member ID)
owner_moengage_id,                -- dim (MoEngage tracking ID)

-- Activity & Lifecycle Dates
pet_created_date,                 -- dim (timestamp - pet record created)
pet_updated_date,                 -- dim (timestamp - pet last updated)
pet_deleted_date,                 -- dim (timestamp - pet deleted or null)
profile_completed_date,           -- dim (date profile was completed)
user_created_date,                -- dim (timestamp - owner account created)
user_updated_date,                -- dim (timestamp - owner last activity)

-- Calculated Time Metrics
pet_account_age_days,             -- fact (days since pet added to system)
owner_account_age_days,           -- fact (days since owner registered)

-- System Metadata
snapshot_date,                    -- dim (date of this snapshot)
snapshot_timestamp,                -- dim (timestamp of this snapshot)

DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at, 

FROM {{ ref('int_superapp_pets') }}
