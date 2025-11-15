# Super App Data Integration Feature
## Mobile Pet Management Platform Analytics

---

## Overview

The **Super App Data Integration** feature enables comprehensive analytics for The Pet Shop's mobile application, providing customer engagement tracking, pet profile management insights, and user behavior analysis.

**Implementation**: 21 dbt models processing 17 source tables
**Timeline**: October 25-27, 2025 (2 commits)
**Author**: Anmar Abbas DataGo

---

## Commit History

### Commit 1: Staging Layer - October 25, 2025
- **Commit Hash**: `7b86fa3505af23d0381ff0a5611b657cdc7cdf11`
- **Date**: Saturday, October 25, 2025 at 11:29:34 UTC
- **Changes**: 17 staging models created (+662 lines)
- **Components**: Core entities (users, pets) + reference/relationship data

### Commit 2: Analytics Layer - October 27, 2025
- **Commit Hash**: `b2d7b4d1728bd2422bd7764cbc5b9f9da55bc3fe`
- **Date**: Monday, October 27, 2025 at 15:14:12 UTC
- **Changes**: 4 analytics models created (+493 lines)
- **Components**: 2 intermediate models + 2 fact tables

---

## Model Architecture

### Staging Layer (17 models) - `models/1_stg/SuperApp/`

**Core Entities**:
- `stg_users` - User/customer master data (44 columns)
- `stg_pets` - Pet profile master data (39 columns)

**Reference Data** (7 models):
- `stg_activity_level`, `stg_allergies`, `stg_dietary_preferences`
- `stg_health_conditions`, `stg_personality_traits`
- `stg_pet_type`, `stg_pet_subtype`

**Relationship Tables** (4 models):
- `stg_pet_allergies`, `stg_pet_dietary_preferences`
- `stg_pet_health_conditions`, `stg_pet_personality_traits`

**Transactional Data** (4 models):
- `stg_pet_vaccination` - Vaccination records and schedules
- `stg_pet_document` - Document uploads (certificates, medical records)
- `stg_pet_document_groups` - Document categories
- `stg_pet_image` - Pet profile images

### Intermediate Layer (2 models) - `models/2_int/SuperApp/`

**int_superapp_pets** (190 lines):
- Joins all pet-related data with owner information
- Aggregates health metrics, vaccinations, documents, images
- Calculates age, days to vaccination, account age
- Applies business logic for vaccination status and pet status

**int_superapp_users** (139 lines):
- Aggregates pet ownership metrics per user
- Calculates engagement scores and activity metrics
- Applies segmentation logic (engagement, user type, compliance)

### Fact Layer (2 models) - `models/3_fct/SuperApp/`

**fct_superapp_pets** (87 columns):
- Pet demographics, health status, vaccination tracking
- Owner information and loyalty data
- Media/documentation metrics, lifecycle dates

**fct_superapp_users** (70 columns):
- User demographics, account status
- Pet ownership and engagement metrics
- Vaccination compliance, document activity
- Customer segmentation

---

## Data Sources

**Database**: `tps-data-386515` (BigQuery)
**Schema**: `public`
**Integration**: Fivetran CDC (incremental replication)

**17 Source Tables**:
- Core: `users`, `Pets`
- Reference: `ActivityLevel`, `Allergies`, `DietaryPreference`, `HealthCondition`, `PersonalityTrait`, `PetType`, `PetSubType`
- Relationships: `PetAllergies`, `PetDietaryPreferences`, `PetHealthConditions`, `PetPersonalityTraits`
- Transactional: `PetVaccination`, `PetDocument`, `PetDocumentGroup`, `PetImage`

---

## Key Metrics & Segmentation

### User Segmentation (fct_superapp_users)

**Engagement Levels** (4 tiers):
- **Highly Engaged**: Active pets + no overdue vaccines + activity in last 30 days
- **Moderately Engaged**: Active pets + activity in last 90 days
- **Low Engagement**: Active pets but inactive
- **Inactive**: No active pets

**User Segments** (4 categories):
- **No Pets**: 0 pets
- **Single Pet Owner**: 1 pet
- **Multi Pet Owner**: 2-3 pets
- **Pet Enthusiast**: 4+ pets

**Vaccination Compliance** (4 levels):
- **No Vaccinations**: No vaccination records
- **Non-Compliant**: Has overdue vaccinations
- **Compliant**: Has upcoming vaccinations, none overdue
- **Fully Vaccinated**: All vaccines current

### Pet Analytics (fct_superapp_pets)

**Vaccination Status** (4 categories):
- **Overdue**: Next vaccine date in past
- **Due Soon**: Next vaccine within 30 days
- **No Schedule**: No upcoming vaccine scheduled
- **Up to Date**: All vaccines current

**Pet Status** (3 categories):
- **Active**: Active with complete profile
- **Incomplete**: Profile not complete
- **Deleted**: Soft deleted

---

## Business Use Cases

### 1. Customer Engagement & Retention
- Identify at-risk customers (low engagement scores)
- Target retention campaigns based on engagement level
- Track feature adoption (documents, images, vaccinations)
- Monitor profile completion rates

### 2. Pet Population Analytics
- Analyze pet type/breed distribution
- Understand demographics (age, size, health status)
- Optimize product assortment by pet type
- Plan inventory based on population trends

### 3. Vaccination Management
- Identify pets with overdue vaccinations
- Automated reminder campaigns for due-soon vaccines
- Track compliance rates by user segment
- Analyze vaccination service opportunities

### 4. Loyalty & Engagement
- Correlate loyalty program participation with engagement
- Track reward claim rates (profile completion, first pet)
- Analyze multi-pet ownership impact
- Customer lifetime value modeling

### 5. Health & Wellness Insights
- Track allergy prevalence by breed
- Monitor health condition trends
- Identify dietary preference patterns
- Support product development (specialized foods)

---

## Sample Queries

**User Engagement Summary**:
```sql
SELECT
    engagement_level,
    user_segment,
    COUNT(*) as users,
    AVG(total_pets) as avg_pets,
    AVG(account_age_days) as avg_account_age
FROM {{ ref('fct_superapp_users') }}
GROUP BY 1, 2
```

**Vaccination Compliance**:
```sql
SELECT
    vaccination_status,
    COUNT(DISTINCT pet_id) as pets,
    COUNT(DISTINCT user_id) as owners
FROM {{ ref('fct_superapp_pets') }}
WHERE pet_status = 'Active'
GROUP BY 1
```

**Pet Demographics**:
```sql
SELECT
    pet_type_name,
    COUNT(*) as count,
    AVG(calculated_age_years) as avg_age,
    SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) as vaccinated
FROM {{ ref('fct_superapp_pets') }}
WHERE pet_status = 'Active'
GROUP BY 1
```

---

## Technical Specifications

**Model Materialization**:
- Staging: `view` (always fresh)
- Intermediate: `ephemeral` or `view`
- Fact: `table` or `incremental` (recommended)

**Performance Recommendations**:
- Partition fact tables by `snapshot_date`
- Cluster by `user_id`, `pet_status`, `engagement_level`
- Use incremental strategy for large datasets

**Data Quality**:
- Add unique/not null tests on primary keys
- Relationship tests between entities
- Freshness checks on Fivetran sync timestamps

---

## Integration Points

**Existing Systems**:
- **Customer Data**: Join to `dim_customers` via email for cross-channel analysis
- **Order Data**: Link to `fact_orders` for purchase behavior correlation
- **Loyalty Program**: Connect via `openloyaltymemberid` for reward tracking

**External Applications**:
- Marketing automation for segmented campaigns
- CRM systems for customer 360 view
- Mobile app for real-time personalization

---

## Next Steps

**Recommended Enhancements**:
1. Add dbt tests (unique, not null, relationships)
2. Create schema.yml with column descriptions
3. Build BI dashboards for key metrics
4. Set up data freshness monitoring
5. Implement incremental materialization for fact tables

**Future Analytics**:
- Predictive churn modeling
- Health risk scoring
- Personalized recommendations
- Cohort retention analysis

---

## Summary

**Total Implementation**:
- **Models**: 21 (17 staging + 2 intermediate + 2 fact)
- **Source Tables**: 17 BigQuery tables
- **Output Metrics**: 157 total columns (87 + 70)
- **Timeline**: 2 days (Oct 25-27, 2025)

**Business Value**:
- Customer 360 view with pet ownership insights
- Engagement tracking and retention analytics
- Vaccination compliance management
- Health and wellness trend analysis
- Loyalty program optimization
- Data-driven marketing segmentation

**Documentation**: Complete technical specs available in extended documentation
**Deployment Status**: Production-ready, committed to repository
**Owner**: Anmar Abbas DataGo (anmar@8020datago.ai)

---

*Document Version: 3.0 (Concise)*
*Created: November 15, 2025*
*Status: Production*
