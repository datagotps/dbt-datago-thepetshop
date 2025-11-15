# Feature Name: Super App Data Integration

## 1. Business Purpose

The Super App Data Integration enables comprehensive tracking and analytics of **pet owners** and their **pets** within the mobile app ecosystem. This feature supports:

- **User Engagement Analysis**: Track how actively users are managing their pets' profiles, vaccinations, and documents
- **Pet Health Management**: Monitor vaccination schedules, health conditions, and wellness metrics across all registered pets
- **Customer Segmentation**: Classify users into segments (Single Pet Owner, Multi Pet Owner, Pet Enthusiast) for targeted marketing
- **Loyalty Program Integration**: Track reward eligibility based on profile completion and pet additions
- **Vaccination Compliance**: Identify users with overdue vaccinations for proactive outreach campaigns

**Dashboard Use Cases**: User engagement dashboards, pet health analytics, vaccination reminder campaigns, customer retention analytics, loyalty program reporting.

## 2. Technical Overview (dbt)

### Models Created/Modified:
**Fact Models (2):**
- `fct_superapp_users` - User engagement metrics with 40+ KPIs (models/3_fct/SuperApp/)
- `fct_superapp_pets` - Pet snapshot analytics with 50+ attributes (models/3_fct/SuperApp/)

**Intermediate Models (2):**
- `int_superapp_users` - Aggregates user data with pet/vaccination/document stats (models/2_int/SuperApp/)
- `int_superapp_pets` - Creates comprehensive pet profiles with health metrics (models/2_int/SuperApp/)

**Staging Models (21):**
- Core: `stg_users`, `stg_pets` (models/1_stg/SuperApp/Top/)
- Entity Values: 19 models for pet types, health conditions, vaccinations, documents, etc. (models/1_stg/SuperApp/pet_entity_values/)

### Upstream Sources:
All source from `public` schema in `tps-data-386515` database:
- Core: `users`, `Pets`
- Health: `PetVaccination`, `PetHealthConditions`, `PetAllergies`
- Media: `PetImage`, `PetDocument`, `PetDocumentGroup`
- Reference: `PetType`, `PetSubType`, `ActivityLevel`, `PersonalityTrait`, `DietaryPreference`

### Downstream Consumers:
Currently used for analytics and reporting. No downstream dependencies identified.

### Key SQL Logic:
- **User Engagement Scoring**: 4-tier classification (Highly Engaged/Moderately Engaged/Low Engagement/Inactive) based on active pets, vaccination compliance, and last update date
- **User Segmentation**: Segments based on pet count (No Pets → Single → Multi → Enthusiast at 4+ pets)
- **Vaccination Compliance**: 4-level status (No Vaccinations/Non-Compliant/Compliant/Fully Vaccinated) tracking overdue vs. upcoming vaccinations
- **Pet Health Aggregations**: Counts of health conditions, allergies, personality traits per pet
- **Profile Completeness Tracking**: Boolean flags for profile completion with reward claim status

### KPI Definitions:
- **Engagement Level**: Active pets > 0 + no overdue vaccines + activity within 30 days = "Highly Engaged"
- **Vaccination Status**: Next vaccine < today = "Overdue"; within 30 days = "Due Soon"
- **Pet Status**: Deleted vs. Incomplete vs. Active based on deletion flag and profile completion
- **User Segment**: Based on total_pets count thresholds (0/1/2-3/4+)

## 3. Model Lineage (high-level)

```
SOURCE TABLES (public schema - BigQuery)
  ├─ users, Pets (core entities)
  ├─ PetVaccination, PetDocument, PetImage (activity data)
  └─ PetType, ActivityLevel, Allergies, etc. (reference data)
         ↓
STAGING LAYER (1_stg/SuperApp/)
  ├─ stg_users, stg_pets (core staging)
  └─ 19 entity staging models (pet attributes & associations)
         ↓
INTERMEDIATE LAYER (2_int/SuperApp/)
  ├─ int_superapp_users (user + aggregated pet metrics)
  └─ int_superapp_pets (pet + user + health aggregations)
         ↓
FACT LAYER (3_fct/SuperApp/)
  ├─ fct_superapp_users (user engagement analytics)
  └─ fct_superapp_pets (pet snapshot analytics)
```

## 4. Important Fields Added

**User Analytics (fct_superapp_users):**
- `engagement_level` - 4-tier user engagement classification
- `user_segment` - Pet ownership segmentation (No Pets → Pet Enthusiast)
- `vaccination_compliance` - Vaccination adherence status
- `total_pets`, `active_pets`, `vaccinated_pets` - Pet ownership metrics
- `overdue_vaccinations`, `upcoming_vaccinations` - Vaccination pipeline
- `account_age_days`, `days_since_last_update` - Activity recency

**Pet Analytics (fct_superapp_pets):**
- `vaccination_status` - Per-pet vaccine status (Overdue/Due Soon/Up to Date)
- `pet_status` - Active/Incomplete/Deleted classification
- `calculated_age_years`, `calculated_age_months` - Derived age metrics
- `total_health_conditions`, `total_allergies` - Health complexity indicators
- `days_until_next_vaccination` - Proactive reminder metric
- `owner_loyalty_member_id`, `owner_moengage_id` - Marketing integration fields

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `7b86fa3` | Anmar Abbas DataGo | 2025-10-25 | **Initial SuperApp staging layer** - Added 17 staging models (509 lines): core user/pet tables + 15 entity value tables for health, vaccination, documents |
| `b2d7b4d` | Anmar Abbas DataGo | 2025-10-27 | **SuperApp analytics layer** - Added intermediate (int_superapp_users, int_superapp_pets) and fact models (fct_superapp_users, fct_superapp_pets) with 484 lines of transformation logic |

## 6. Limitations / Assumptions

**Limitations:**
- No dbt tests defined (no singular or generic tests for data quality validation)
- No YAML schema documentation for column descriptions
- No incremental models - all models are full refresh
- Timezone handling: Reports add +4 hours to UTC via `DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR)`

**Assumptions:**
- Source data in `public` schema is clean and complete
- Pet `age` and `weight` fields can be safely cast to FLOAT64
- User email uniqueness maintained at source
- Deleted pets (deletedat IS NOT NULL) are retained in analytics for historical tracking
- Vaccination dates are accurate and current_date comparisons are valid

**Future Improvements:**
- Add data quality tests (not null, unique, relationships)
- Implement incremental models for performance optimization
- Add column-level documentation in schema.yml files
- Consider SCD Type 2 snapshots for tracking pet profile changes over time
- Add macro for engagement scoring logic reusability

---

**Documentation Generated:** 2025-11-15
**Feature Commit Range:** 7b86fa3 → b2d7b4d (2025-10-25 to 2025-10-27)
**Total Models:** 25 (21 staging, 2 intermediate, 2 fact)
**Total Lines of Code:** 993
