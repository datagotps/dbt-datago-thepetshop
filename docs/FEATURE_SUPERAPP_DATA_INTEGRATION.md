# Feature Documentation: Super App Data Integration
## Mobile Pet Management Platform Analytics

---

## Executive Summary

The **Super App Data Integration** feature introduces comprehensive analytics capabilities for The Pet Shop's mobile application platform. This feature enables customer engagement tracking, pet profile management analytics, and user behavior insights through a complete data modeling layer built on top of the Super App's mobile backend data.

The implementation spans **21 dbt models** across staging, intermediate, and fact layers, processing data from **17 source tables** to deliver rich analytics on pet ownership, user engagement, vaccination management, and customer loyalty metrics.

---

## Commit History

### Primary Implementation: October 25, 2025

| Field | Details |
|-------|---------|
| **Commit Hash** | `7b86fa3505af23d0381ff0a5611b657cdc7cdf11` |
| **Date** | Saturday, October 25, 2025 at 11:29:34 UTC |
| **Author** | Anmar Abbas DataGo (anmar@8020datago.ai) |
| **Message** | "yes" |
| **SuperApp Files** | 17 staging models created |
| **Total Changes** | 31 files changed, +662 lines, -28 lines |

**SuperApp Components Added**:
- 2 core staging models (users, pets)
- 15 pet entity and relationship models
- Source configuration for 17 tables

---

### Analytics Layer: October 27, 2025

| Field | Details |
|-------|---------|
| **Commit Hash** | `b2d7b4d1728bd2422bd7764cbc5b9f9da55bc3fe` |
| **Date** | Monday, October 27, 2025 at 15:14:12 UTC |
| **Author** | Anmar Abbas DataGo (anmar@8020datago.ai) |
| **Message** | "y" |
| **SuperApp Files** | 4 analytics models (2 intermediate, 2 fact) |
| **Total Changes** | 7 files changed, +493 lines, -4 lines |

**SuperApp Components Added**:
- 2 intermediate transformation models
- 2 fact tables for reporting
- Complete analytics layer

---

## Feature Architecture

### Project Structure

```
models/
├── 1_stg/SuperApp/                    # STAGING LAYER (17 models)
│   ├── Top/                           # Core Entities
│   │   ├── stg_users.sql             # User/customer master data
│   │   └── stg_pets.sql              # Pet profile master data
│   │
│   └── pet_entity_values/            # Reference & Relationship Data
│       ├── stg_activity_level.sql    # Activity level master
│       ├── stg_allergies.sql         # Allergy types master
│       ├── stg_dietary_preferences.sql  # Dietary options master
│       ├── stg_health_conditions.sql    # Health condition types
│       ├── stg_personality_traits.sql   # Personality trait types
│       ├── stg_pet_type.sql            # Pet types (dog/cat/etc)
│       ├── stg_pet_subtype.sql         # Pet breeds/subtypes
│       ├── stg_pet_allergies.sql       # Pet-allergy relationships
│       ├── stg_pet_dietary_preferences.sql  # Pet dietary links
│       ├── stg_pet_health_conditions.sql    # Pet health links
│       ├── stg_pet_personality_traits.sql   # Pet personality links
│       ├── stg_pet_vaccination.sql     # Vaccination records
│       ├── stg_pet_document.sql        # Document uploads
│       ├── stg_pet_document_groups.sql # Document organization
│       └── stg_pet_image.sql           # Pet profile images
│
├── 2_int/SuperApp/                    # INTERMEDIATE LAYER (2 models)
│   ├── int_superapp_pets.sql         # Pet analytics transformations
│   └── int_superapp_users.sql        # User engagement transformations
│
└── 3_fct/SuperApp/                    # FACT LAYER (2 models)
    ├── fct_superapp_pets.sql         # Pet analytics fact table
    └── fct_superapp_users.sql        # User engagement fact table
```

**Total Models**: 21
- **Staging**: 17 models
- **Intermediate**: 2 models
- **Fact**: 2 models

---

## Data Source Configuration

### Source System

- **Database**: `tps-data-386515` (Google BigQuery)
- **Schema**: `public`
- **Integration**: Fivetran CDC (Change Data Capture)
- **Sync Frequency**: Incremental replication
- **Tracking Fields**: `_fivetran_synced`, `_fivetran_deleted`

### Source Tables (17 Total)

#### Core Entities
1. **users** - Customer/owner accounts
2. **Pets** - Pet profile information

#### Master Reference Data
3. **ActivityLevel** - Activity level options (sedentary/moderate/active/very active)
4. **Allergies** - Allergy types master list
5. **DietaryPreference** - Dietary preference options
6. **HealthCondition** - Health condition types
7. **PersonalityTrait** - Personality trait options
8. **PetType** - Pet type classification (dog/cat/bird/etc.)
9. **PetSubType** - Breed and subtype details

#### Relationship Tables
10. **PetAllergies** - Many-to-many: Pets ↔ Allergies
11. **PetDietaryPreferences** - Many-to-many: Pets ↔ Dietary Preferences
12. **PetHealthConditions** - Many-to-many: Pets ↔ Health Conditions
13. **PetPersonalityTraits** - Many-to-many: Pets ↔ Personality Traits

#### Transactional/Detail Data
14. **PetVaccination** - Vaccination records and schedules
15. **PetDocument** - Document uploads (certificates, medical records)
16. **PetDocumentGroup** - Document organization groups
17. **PetImage** - Pet profile images

---

## Layer 1: Staging Models (17 Models)

### A. Core Entity Models

#### 1. stg_users.sql
**Path**: `models/1_stg/SuperApp/Top/stg_users.sql`

**Purpose**: Staging layer for Super App user/customer accounts

**Source**: `public.users`

**Key Fields** (44 columns):
- **Identifiers**: `id`, `shopify_id`, `moengageid`, `moegoid`, `openloyaltymemberid`
- **Personal Info**: `firstname`, `lastname`, `email`, `gender`, `birthdate`, `nationality`
- **Contact**: `country`, `internationalcode`, `phone`
- **Account Status**:
  - `isguest` - Guest account flag
  - `isactive` - Account active status
  - `isemailverified` - Email verification status
  - `isphoneverified` - Phone verification status
  - `isprofilecomplete` - Profile completion status
  - `profilecompletedat` - Profile completion timestamp
- **Preferences**:
  - `preferredlanguage` - Language preference
  - `isnotificationsenabled` - Push notification settings
- **Legal**: `privacypolicy`, `termsandconditions` - Acceptance flags
- **Loyalty Program**:
  - `openloyaltyprofilerewarded` - Profile completion reward claimed
  - `openloyaltyfirstpetrewarded` - First pet addition reward claimed
- **Timestamps**: `createdat`, `updatedat`, `deletedat`, `created_at`, `updated_at`
- **Metadata**: `_fivetran_deleted`, `_fivetran_synced`

**Data Lineage**: Source → Staging (simple rename/select pattern)

---

#### 2. stg_pets.sql
**Path**: `models/1_stg/SuperApp/Top/stg_pets.sql`

**Purpose**: Staging layer for pet profile information

**Source**: `public.Pets`

**Key Fields** (39 columns):
- **Identifiers**: `id`, `userid` (owner), `moegoid`
- **Basic Info**: `name`, `gender`, `birthdate`, `age`, `size`, `weight`
- **Health Status**:
  - `isvaccinated` - Vaccination status
  - `isneutered` - Neutered/spayed status
  - `hasmicrochip` - Microchip presence
  - `microchip` - Microchip number
- **Type Classification**:
  - `typeid` - Pet type (links to PetType)
  - `subtypeid` - Breed/subtype (links to PetSubType)
  - `customsubtype` - Custom breed if not in list
- **Activity**: `activitylevelid` - Activity level (links to ActivityLevel)
- **Free-text Attributes**:
  - `customdietarypreferences` - Custom dietary notes
  - `customallergies` - Custom allergy notes
  - `customhealthconditions` - Custom health notes
  - `notes` - General notes
- **Profile Status**:
  - `isprofilecomplete` - Profile completion flag
  - `profilecompletedat` - Completion timestamp
- **Timestamps**: `createdat`, `updatedat`, `deletedat`
- **Metadata**: `_fivetran_deleted`, `_fivetran_synced`

**Data Lineage**: Source → Staging (simple rename/select pattern)

---

### B. Master Reference Data Models

These models follow a consistent pattern for reference/lookup data:

#### 3. stg_activity_level.sql
**Source**: `public.ActivityLevel`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Activity level options (Sedentary, Moderate, Active, Very Active)

#### 4. stg_allergies.sql
**Source**: `public.Allergies`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Master list of allergy types (Food allergies, environmental, etc.)

#### 5. stg_dietary_preferences.sql
**Source**: `public.DietaryPreference`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Dietary preference options (Grain-free, organic, raw food, etc.)

#### 6. stg_health_conditions.sql
**Source**: `public.HealthCondition`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Health condition types (Arthritis, diabetes, heart disease, etc.)

#### 7. stg_personality_traits.sql
**Source**: `public.PersonalityTrait`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Personality trait options (Friendly, shy, energetic, calm, etc.)

#### 8. stg_pet_type.sql
**Source**: `public.PetType`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, timestamps
**Purpose**: Pet type classification (Dog, Cat, Bird, Fish, Rabbit, etc.)

#### 9. stg_pet_subtype.sql
**Source**: `public.PetSubType`
**Fields**: `id`, `value`, `label`, `icon`, `imageurl`, `isactive`, `sort_order`, `typeid`, timestamps
**Purpose**: Breed/subtype details (Golden Retriever, Persian Cat, etc.)

**Common Pattern**:
```sql
WITH source AS (
    SELECT * FROM {{ source('public', '[TableName]') }}
),
renamed AS (
    SELECT
        id,
        value,
        label,
        icon,
        imageurl,
        isactive,
        `order` AS sort_order,
        createdat,
        updatedat,
        _fivetran_deleted,
        _fivetran_synced
    FROM source
)
SELECT * FROM renamed
```

---

### C. Relationship/Bridge Models

These models implement many-to-many relationships between pets and their attributes:

#### 10. stg_pet_allergies.sql
**Source**: `public.PetAllergies`
**Fields**: `id`, `petid`, `allergyid`, timestamps
**Purpose**: Links pets to their specific allergies
**Cardinality**: Many-to-many (one pet can have multiple allergies)

#### 11. stg_pet_dietary_preferences.sql
**Source**: `public.PetDietaryPreferences`
**Fields**: `id`, `petid`, `dietarypreferenceid`, timestamps
**Purpose**: Links pets to dietary preferences
**Cardinality**: Many-to-many

#### 12. stg_pet_health_conditions.sql
**Source**: `public.PetHealthConditions`
**Fields**: `id`, `petid`, `healthconditionid`, timestamps
**Purpose**: Links pets to health conditions
**Cardinality**: Many-to-many

#### 13. stg_pet_personality_traits.sql
**Source**: `public.PetPersonalityTraits`
**Fields**: `id`, `petid`, `personalitytraitid`, timestamps
**Purpose**: Links pets to personality traits
**Cardinality**: Many-to-many

**Common Pattern**:
```sql
WITH source AS (
    SELECT * FROM {{ source('public', '[TableName]') }}
),
renamed AS (
    SELECT
        id,
        createdat,
        updatedat,
        petid,
        [attribute]id,
        _fivetran_deleted,
        _fivetran_synced
    FROM source
)
SELECT * FROM renamed
```

---

### D. Transactional/Detail Models

#### 14. stg_pet_vaccination.sql
**Source**: `public.PetVaccination`

**Fields**:
- `id` - Vaccination record ID
- `petid` - Pet identifier
- `vaccinename` - Vaccine name/type
- `vaccinatedon` - Date vaccine was administered
- `nextvaccinedate` - Next scheduled vaccination
- `notes` - Additional notes
- Timestamps: `createdat`, `updatedat`, `deletedat`
- Metadata: `_fivetran_deleted`, `_fivetran_synced`

**Purpose**: Track vaccination history and schedules for pets
**Use Cases**: Vaccination compliance, reminder scheduling, health tracking

---

#### 15. stg_pet_document.sql
**Source**: `public.PetDocument`

**Fields**:
- `id` - Document ID
- `petid` - Pet identifier
- `document` - Document file/URL
- `name` - Document name
- `type` - Document type/category
- `issuedate` - Document issue date
- `expirydate` - Document expiry date
- `notes` - Additional notes
- `vaccinationid` - Link to vaccination if applicable
- `groupid` - Document group classification
- Timestamps: `createdat`, `updatedat`, `deletedat`
- Metadata: `_fivetran_deleted`, `_fivetran_synced`

**Purpose**: Store vaccination certificates, medical records, ownership papers
**Use Cases**: Document management, compliance tracking, record keeping

---

#### 16. stg_pet_document_groups.sql
**Source**: `public.PetDocumentGroup`

**Fields**: Standard reference table pattern (id, value, label, icon, etc.)

**Purpose**: Categorize documents into groups (Medical, Legal, Insurance, etc.)

---

#### 17. stg_pet_image.sql
**Source**: `public.PetImage`

**Fields**:
- `id` - Image ID
- `petid` - Pet identifier
- `image` - Image file/URL
- `isprimary` - Primary/profile image flag
- Timestamps: `createdat`, `updatedat`, `deletedat`
- Metadata: `_fivetran_deleted`, `_fivetran_synced`

**Purpose**: Store pet profile images
**Use Cases**: Profile display, visual identification, user engagement

---

## Layer 2: Intermediate Models (2 Models)

### 1. int_superapp_pets.sql
**Path**: `models/2_int/SuperApp/int_superapp_pets.sql` (190 lines)

**Purpose**: Comprehensive pet profile combining all pet-related data with owner information and aggregated metrics

**Dependencies**:
- `stg_pets`
- `stg_users`
- `stg_pet_type`
- `stg_pet_subtype`
- `stg_activity_level`
- `stg_pet_health_conditions` (aggregated)
- `stg_pet_allergies` (aggregated)
- `stg_pet_personality_traits` (aggregated)
- `stg_pet_vaccination` (aggregated)
- `stg_pet_document` (aggregated)
- `stg_pet_image` (aggregated)

**Transformation Logic**:

1. **Aggregations**:
   - Count of health conditions per pet
   - Count of allergies per pet
   - Count of personality traits per pet
   - Count of documents per pet
   - Count of images per pet
   - Last vaccination date, next due date
   - Primary image flag

2. **Calculations**:
   - `calculated_age_years` - Age calculated from birthdate
   - `calculated_age_months` - Age in months
   - `pet_account_age_days` - Days since pet added
   - `owner_account_age_days` - Days since owner registered
   - `days_until_next_vaccination` - Days to next vaccine

3. **Business Logic**:
   - **Vaccination Status**:
     - "Overdue" - Next vaccine date in past
     - "Due Soon" - Next vaccine within 30 days
     - "No Schedule" - No upcoming vaccine scheduled
     - "Up to Date" - All vaccines current

   - **Pet Status**:
     - "Deleted" - Soft deleted
     - "Incomplete" - Profile not complete
     - "Active" - Active with complete profile

4. **Enrichment**:
   - Owner demographic information
   - Pet type/breed details
   - Activity level description
   - Custom text fields (dietary notes, allergy notes)

**Output Schema**: ~106 columns covering pet demographics, health, ownership, and calculated metrics

---

### 2. int_superapp_users.sql
**Path**: `models/2_int/SuperApp/int_superapp_users.sql` (139 lines)

**Purpose**: User engagement metrics, pet ownership statistics, and customer segmentation

**Dependencies**:
- `stg_users`
- `stg_pets` (aggregated by user)
- `stg_pet_vaccination` (aggregated by user)
- `stg_pet_document` (aggregated by user)

**Transformation Logic**:

1. **Pet Ownership Metrics** (from `pet_stats` CTE):
   - `total_pets` - All pets owned
   - `active_pets` - Non-deleted pets
   - `vaccinated_pets` - Count of vaccinated pets
   - `neutered_pets` - Count of neutered pets
   - `completed_pet_profiles` - Complete profiles count
   - `avg_pet_age` - Average age across all pets
   - `avg_pet_weight` - Average weight across all pets
   - `first_pet_added_date` - First pet creation date
   - `last_pet_added_date` - Most recent pet added
   - `last_pet_update_date` - Most recent pet update

2. **Vaccination Engagement** (from `vaccination_stats` CTE):
   - `total_vaccinations` - All vaccination records
   - `upcoming_vaccinations` - Future scheduled vaccines
   - `overdue_vaccinations` - Past due vaccines
   - `last_vaccination_date` - Most recent vaccine

3. **Document Engagement** (from `document_stats` CTE):
   - `total_documents` - All documents uploaded
   - `document_types_used` - Distinct document types
   - `last_document_upload_date` - Most recent upload

4. **User Metrics**:
   - `user_age` - Calculated from birthdate
   - `account_age_days` - Days since registration
   - `days_since_last_update` - Activity recency

5. **Segmentation Logic**:

   **Engagement Level**:
   - "Highly Engaged" - Active pets + no overdue vaccines + activity in last 30 days
   - "Moderately Engaged" - Active pets + activity in last 90 days
   - "Low Engagement" - Active pets but inactive
   - "Inactive" - No active pets

   **User Segment**:
   - "No Pets" - 0 pets
   - "Single Pet Owner" - 1 pet
   - "Multi Pet Owner" - 2-3 pets
   - "Pet Enthusiast" - 4+ pets

   **Vaccination Compliance**:
   - "No Vaccinations" - No vaccination records
   - "Non-Compliant" - Has overdue vaccinations
   - "Compliant" - Has upcoming vaccinations, none overdue
   - "Fully Vaccinated" - All vaccines current

**Output Schema**: ~70 columns covering user profile, engagement metrics, and segmentation

---

## Layer 3: Fact Models (2 Models)

### 1. fct_superapp_pets.sql
**Path**: `models/3_fct/SuperApp/fct_superapp_pets.sql` (86 lines)

**Purpose**: Production-ready pet analytics fact table for business intelligence and reporting

**Source**: `int_superapp_pets`

**Output Columns** (87 total):

**Pet Identifiers & Demographics** (19 fields):
- `pet_id`, `user_id`, `pet_name`
- `pet_type_name`, `pet_subtype_name`, `is_custom_subtype`, `custom_subtype_value`
- `pet_gender`, `pet_birthdate`, `pet_age`, `calculated_age_years`, `calculated_age_months`
- `pet_size`, `pet_weight`
- `activity_level_name`
- `profile_completion_status`, `pet_status`
- `pet_notes`

**Health Status** (7 fields):
- `is_vaccinated`, `is_neutered`, `has_microchip`, `microchip_number`
- `custom_dietary_preferences`, `custom_allergies`, `custom_health_conditions`

**Health Metrics** (3 fields):
- `total_health_conditions`, `total_allergies`, `total_personality_traits`

**Vaccination Management** (4 fields):
- `last_vaccination_date`, `next_vaccination_due`
- `days_until_next_vaccination`, `vaccination_status`

**Media & Documentation** (3 fields):
- `total_documents`, `total_images`, `has_primary_image`

**Owner Information** (10 fields):
- `owner_first_name`, `owner_last_name`, `owner_email`
- `owner_country`, `owner_phone`, `owner_gender`, `owner_birthdate`
- `owner_nationality`, `owner_preferred_language`
- `owner_is_active`, `owner_profile_complete`

**Owner Loyalty** (2 fields):
- `owner_loyalty_member_id`, `owner_moengage_id`

**Activity & Lifecycle** (7 fields):
- `pet_created_date`, `pet_updated_date`, `pet_deleted_date`, `profile_completed_date`
- `user_created_date`, `user_updated_date`
- `pet_account_age_days`, `owner_account_age_days`

**System Metadata** (3 fields):
- `snapshot_date`, `snapshot_timestamp`
- `report_last_updated_at` (UTC +4 hours)

**Business Use Cases**:
- Pet population demographics
- Health compliance reporting
- Vaccination schedule management
- Owner engagement analysis
- Profile completion tracking
- Document/image upload metrics

---

### 2. fct_superapp_users.sql
**Path**: `models/3_fct/SuperApp/fct_superapp_users.sql` (69 lines)

**Purpose**: Production-ready user engagement fact table for customer analytics

**Source**: `int_superapp_users`

**Output Columns** (70 total):

**User Identifiers** (4 fields):
- `user_id`, `user_email`, `user_full_name`, `loyalty_member_id`

**User Demographics** (5 fields):
- `country`, `nationality`, `preferred_language`, `user_gender`, `user_age`

**Account Status** (6 fields):
- `is_active`, `is_guest`, `is_email_verified`, `is_phone_verified`
- `is_profile_complete`, `notifications_enabled`

**Pet Ownership Metrics** (7 fields):
- `total_pets`, `active_pets`, `completed_pet_profiles`
- `vaccinated_pets`, `neutered_pets`
- `avg_pet_age`, `avg_pet_weight`

**Vaccination Management** (4 fields):
- `total_vaccinations`, `upcoming_vaccinations`, `overdue_vaccinations`
- `vaccination_compliance` (segmentation)

**Document Engagement** (2 fields):
- `total_documents`, `document_types_used`

**Loyalty Program** (2 fields):
- `profile_reward_claimed`, `first_pet_reward_claimed`

**Engagement Metrics** (9 fields):
- `account_age_days`, `days_since_last_update`
- `user_created_date`, `profile_completed_date`
- `first_pet_added_date`, `last_pet_added_date`, `last_pet_update_date`
- `last_vaccination_date`, `last_document_upload_date`

**Segmentation** (3 fields):
- `engagement_level` - 4 tiers (Highly Engaged → Inactive)
- `user_segment` - 4 categories (No Pets → Pet Enthusiast)
- `vaccination_compliance` - 4 levels (No Vaccinations → Fully Vaccinated)

**System Metadata** (2 fields):
- `snapshot_date`
- `report_last_updated_at` (UTC +4 hours)

**Business Use Cases**:
- Customer engagement analysis
- User lifecycle tracking
- Loyalty program effectiveness
- Vaccination compliance monitoring
- Customer segmentation for marketing
- Retention risk identification
- Feature adoption tracking

---

## Business Intelligence Use Cases

### 1. Customer Engagement & Retention

**Metrics Available**:
- User engagement levels (Highly/Moderately/Low/Inactive)
- Days since last activity
- Profile completion rates
- Feature adoption (documents, images, vaccinations)

**Analysis Opportunities**:
- Identify at-risk customers (low engagement)
- Retention campaign targeting
- Onboarding effectiveness
- Feature utilization trends

**Sample Questions Answered**:
- What % of users are highly engaged?
- How many inactive users haven't updated profiles in 90+ days?
- What's the average time to profile completion?
- Which user segments have highest vaccination compliance?

---

### 2. Pet Population Analytics

**Metrics Available**:
- Pet type/breed distribution
- Age and size demographics
- Health status (vaccinated, neutered, microchipped)
- Profile completion rates

**Analysis Opportunities**:
- Product assortment optimization (by pet type/breed)
- Service offerings (grooming, training by breed)
- Inventory planning
- Market segmentation

**Sample Questions Answered**:
- What's the most common pet type/breed?
- What's the average pet age by type?
- How many pets are puppies/kittens (<1 year)?
- What % of pets have complete health profiles?

---

### 3. Vaccination Management

**Metrics Available**:
- Vaccination records and schedules
- Compliance status (overdue/due soon/current)
- Days until next vaccination
- Vaccination trends over time

**Analysis Opportunities**:
- Automated reminder campaigns
- Vaccination service promotions
- Compliance improvement initiatives
- Seasonal vaccination patterns

**Sample Questions Answered**:
- How many pets have overdue vaccinations?
- What's the vaccination compliance rate?
- Which users have vaccinations due in next 30 days?
- What's the average time between vaccinations?

---

### 4. Document & Media Management

**Metrics Available**:
- Document upload counts by type
- Image upload counts
- Primary image presence
- Upload activity trends

**Analysis Opportunities**:
- Feature usage analysis
- Profile quality assessment
- Engagement correlation
- Mobile app UX optimization

**Sample Questions Answered**:
- What % of pets have profile images?
- Which document types are most common?
- How many documents per user on average?
- Does document upload correlate with engagement?

---

### 5. Owner Demographics & Behavior

**Metrics Available**:
- Geographic distribution
- Language preferences
- Multi-pet ownership rates
- Loyalty program participation

**Analysis Opportunities**:
- Market expansion planning
- Localization priorities
- Loyalty program optimization
- Cross-sell/upsell targeting

**Sample Questions Answered**:
- What % of users own multiple pets?
- Which countries have highest adoption?
- What's the loyalty program participation rate?
- How does engagement vary by language/country?

---

### 6. Health & Wellness Insights

**Metrics Available**:
- Allergy prevalence
- Health condition tracking
- Dietary preference trends
- Personality trait distribution

**Analysis Opportunities**:
- Product development (specialized foods)
- Content marketing (health topics)
- Partnership opportunities (vets, insurance)
- Predictive health modeling

**Sample Questions Answered**:
- What are the most common pet allergies?
- Which health conditions are most prevalent?
- What dietary preferences are trending?
- How do health metrics vary by breed/age?

---

## Technical Specifications

### Data Freshness

**Sync Strategy**: Fivetran incremental replication
- **Tracking**: `_fivetran_synced` timestamp on all tables
- **Soft Deletes**: `_fivetran_deleted` flag + `deletedat` timestamps
- **Expected Latency**: < 1 hour for most tables

### Data Quality Checks

**Recommended dbt Tests**:

```yaml
# For stg_users
tests:
  - unique:
      column_name: id
  - not_null:
      column_name: id
  - not_null:
      column_name: email
  - relationships:
      to: ref('stg_pets')
      field: userid

# For stg_pets
tests:
  - unique:
      column_name: id
  - not_null:
      column_name: id
  - not_null:
      column_name: userid
  - relationships:
      to: ref('stg_users')
      field: id

# For relationship tables
tests:
  - unique:
      column_name: id
  - not_null:
      column_name: petid
  - relationships:
      to: ref('stg_pets')
      field: id
```

### Performance Considerations

**Model Materialization Recommendations**:
- **Staging models**: `view` (lightweight, always fresh)
- **Intermediate models**: `ephemeral` or `view` (used for transformations)
- **Fact models**: `table` or `incremental` (optimized for querying)

**Incremental Strategy** (for fact tables):
```sql
{{ config(
    materialized='incremental',
    unique_key='pet_id',
    on_schema_change='sync_all_columns'
) }}
```

**Partition Strategy** (BigQuery):
- Partition fact tables by `snapshot_date` or `created_date`
- Cluster by frequently filtered columns (`user_id`, `pet_status`, `engagement_level`)

---

## Data Lineage

### Complete Flow Diagram

```
SOURCE TABLES (BigQuery: public schema)
│
├─ users ──────────────────────┐
├─ Pets ───────────────────────┤
├─ ActivityLevel ──────────────┤
├─ Allergies ──────────────────┤
├─ DietaryPreference ──────────┤
├─ HealthCondition ────────────┤
├─ PersonalityTrait ───────────┤
├─ PetType ────────────────────┤
├─ PetSubType ─────────────────┤
├─ PetAllergies ───────────────┤
├─ PetDietaryPreferences ──────┤
├─ PetHealthConditions ────────┤
├─ PetPersonalityTraits ───────┤
├─ PetVaccination ─────────────┤
├─ PetDocument ────────────────┤
├─ PetDocumentGroup ───────────┤
└─ PetImage ───────────────────┤
                                │
                                ↓
        STAGING LAYER (17 models)
        models/1_stg/SuperApp/
                │
                ├─ stg_users
                ├─ stg_pets
                ├─ stg_activity_level
                ├─ stg_allergies
                ├─ stg_dietary_preferences
                ├─ stg_health_conditions
                ├─ stg_personality_traits
                ├─ stg_pet_type
                ├─ stg_pet_subtype
                ├─ stg_pet_allergies
                ├─ stg_pet_dietary_preferences
                ├─ stg_pet_health_conditions
                ├─ stg_pet_personality_traits
                ├─ stg_pet_vaccination
                ├─ stg_pet_document
                ├─ stg_pet_document_groups
                └─ stg_pet_image
                                │
                                ↓
      INTERMEDIATE LAYER (2 models)
      models/2_int/SuperApp/
                │
                ├─ int_superapp_pets ───┐
                │   (joins + aggregations)
                │
                └─ int_superapp_users ──┤
                    (metrics + segmentation)
                                │
                                ↓
         FACT LAYER (2 models)
         models/3_fct/SuperApp/
                │
                ├─ fct_superapp_pets
                │   (87 columns - pet analytics)
                │
                └─ fct_superapp_users
                    (70 columns - user engagement)
                                │
                                ↓
              BUSINESS INTELLIGENCE
              - Dashboards
              - Reports
              - Analytics
              - ML Models
```

---

## Integration Points

### Existing dbt Models

**Potential Linkages**:
1. **Customer Integration**:
   - Join `fct_superapp_users` to `dim_customers` via email/phone
   - Analyze online vs. Super App customer overlap
   - Cross-channel behavior analysis

2. **Order Analysis**:
   - Link Super App users to `fact_orders` for purchase patterns
   - Pet type-based product recommendations
   - Loyalty program impact on purchase frequency

3. **Loyalty Program**:
   - Correlate `openloyaltymemberid` across systems
   - Analyze reward redemption by user segment
   - Track loyalty point accrual from app engagement

### External Systems

**Super App Mobile Backend**:
- RESTful API for real-time data
- User authentication integration
- Push notification targeting based on analytics

**Marketing Automation**:
- User segmentation exports for campaigns
- Vaccination reminder triggers
- Engagement score-based targeting

**CRM Systems**:
- Customer 360 view with pet information
- Support ticket enrichment with pet context
- Customer lifetime value modeling

---

## Migration & Deployment

### Deployment Checklist

**Phase 1: Staging Layer** ✅ (Completed Oct 25, 2025)
- [x] Configure source connections
- [x] Create 17 staging models
- [x] Validate row counts vs source
- [x] Test data freshness

**Phase 2: Analytics Layer** ✅ (Completed Oct 27, 2025)
- [x] Build intermediate transformations
- [x] Create fact tables
- [x] Validate business logic
- [x] Performance testing

**Phase 3: Testing** (Recommended)
- [ ] Add dbt tests (unique, not null, relationships)
- [ ] Create data quality monitors
- [ ] Set up freshness checks
- [ ] Validate segmentation logic

**Phase 4: Documentation** (In Progress)
- [x] Feature documentation (this document)
- [ ] Add schema.yml files with column descriptions
- [ ] Create data dictionary for business users
- [ ] Document known data quality issues

**Phase 5: Production** (Next Steps)
- [ ] Set up production scheduling
- [ ] Configure alerting for failures
- [ ] Create BI dashboards
- [ ] Train analytics team on new models

---

## Known Issues & Limitations

### Data Quality Considerations

1. **Custom Fields**:
   - Free-text fields (`customallergies`, `customdietarypreferences`, etc.) require NLP analysis for insights
   - No standardization on custom subtype values
   - Potential data quality issues in free text

2. **Age Calculations**:
   - Both `age` field and `birthdate` exist - may have inconsistencies
   - Recommended to use `birthdate` for calculations
   - Handle null birthdates gracefully

3. **Soft Deletes**:
   - Both `deletedat` and `_fivetran_deleted` exist
   - Need to handle both for complete deletion logic
   - Historical analysis may include deleted records

4. **Vaccination Schedules**:
   - `nextvaccinedate` may be null for completed series
   - No validation that dates are in logical order
   - Past vaccinations may have future dates (data entry errors)

---

## Future Enhancements

### Recommended Next Steps

1. **Advanced Analytics**:
   - Predictive modeling for customer churn
   - Health risk scoring for pets
   - Recommendation engine for products/services
   - Cohort analysis for user retention

2. **Real-Time Features**:
   - Streaming data for instant vaccination reminders
   - Real-time engagement scoring
   - Live dashboard updates
   - Event-driven notifications

3. **Machine Learning**:
   - Pet breed identification from images
   - Automated health condition detection from documents
   - Personalized content recommendations
   - Lifetime value prediction

4. **Data Enrichment**:
   - Weather data correlation with pet activity
   - Seasonal behavior patterns
   - Geographic pet ownership trends
   - Social media sentiment analysis

5. **Additional Metrics**:
   - Session activity tracking
   - Feature usage heatmaps
   - A/B test result integration
   - Customer satisfaction scores

---

## Testing & Validation Results

### Data Validation Summary

**Source Row Count Validation**:
```sql
-- Run these queries to validate staging models match source counts
SELECT COUNT(*) FROM {{ source('public', 'users') }} -- Compare to stg_users
SELECT COUNT(*) FROM {{ source('public', 'Pets') }} -- Compare to stg_pets
```

**Expected Results** (as of implementation):
- Source tables successfully syncing via Fivetran
- All 17 staging models created without errors
- Intermediate and fact models building successfully
- No data loss during transformations

---

## Support & Maintenance

### Model Ownership

**Primary Owner**: Anmar Abbas DataGo (anmar@8020datago.ai)
**Team**: DataGo Analytics Team
**Created**: October 25-27, 2025

### Monitoring

**Key Metrics to Monitor**:
1. Data freshness (`_fivetran_synced` timestamps)
2. Row count trends (sudden drops may indicate sync issues)
3. Null percentages in key fields
4. Fact table build times
5. Segmentation distribution changes

**Alerting Recommendations**:
- Alert if Fivetran sync > 2 hours old
- Alert if fact table row count drops > 10%
- Alert if dbt build fails
- Alert if key metrics have anomalies

---

## Appendix

### A. Complete Model List

**Staging Layer (17)**:
1. `stg_users`
2. `stg_pets`
3. `stg_activity_level`
4. `stg_allergies`
5. `stg_dietary_preferences`
6. `stg_health_conditions`
7. `stg_personality_traits`
8. `stg_pet_type`
9. `stg_pet_subtype`
10. `stg_pet_allergies`
11. `stg_pet_dietary_preferences`
12. `stg_pet_health_conditions`
13. `stg_pet_personality_traits`
14. `stg_pet_vaccination`
15. `stg_pet_document`
16. `stg_pet_document_groups`
17. `stg_pet_image`

**Intermediate Layer (2)**:
1. `int_superapp_pets` (190 lines)
2. `int_superapp_users` (139 lines)

**Fact Layer (2)**:
1. `fct_superapp_pets` (86 lines, 87 columns)
2. `fct_superapp_users` (69 lines, 70 columns)

---

### B. Sample Queries

**User Engagement Summary**:
```sql
SELECT
    engagement_level,
    user_segment,
    COUNT(*) as user_count,
    AVG(total_pets) as avg_pets,
    AVG(account_age_days) as avg_account_age,
    SUM(total_vaccinations) as total_vaccines
FROM {{ ref('fct_superapp_users') }}
GROUP BY engagement_level, user_segment
ORDER BY user_count DESC
```

**Pet Demographics**:
```sql
SELECT
    pet_type_name,
    pet_subtype_name,
    COUNT(*) as pet_count,
    AVG(calculated_age_years) as avg_age,
    AVG(pet_weight) as avg_weight,
    SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) as vaccinated_count
FROM {{ ref('fct_superapp_pets') }}
WHERE pet_status = 'Active'
GROUP BY pet_type_name, pet_subtype_name
ORDER BY pet_count DESC
```

**Vaccination Compliance**:
```sql
SELECT
    vaccination_status,
    COUNT(DISTINCT pet_id) as pet_count,
    COUNT(DISTINCT user_id) as owner_count,
    AVG(days_until_next_vaccination) as avg_days_until_due
FROM {{ ref('fct_superapp_pets') }}
WHERE pet_status = 'Active'
GROUP BY vaccination_status
```

---

### C. Change Log

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2025-10-25 | 1.0 | Initial staging layer implementation | Anmar Abbas DataGo |
| 2025-10-27 | 2.0 | Analytics layer (intermediate + fact) | Anmar Abbas DataGo |
| 2025-11-15 | 2.1 | Comprehensive documentation created | DataGo Team |

---

**Document Version**: 2.1
**Created**: November 15, 2025
**Last Updated**: November 15, 2025
**Status**: Production
**Classification**: Internal - Analytics Team
