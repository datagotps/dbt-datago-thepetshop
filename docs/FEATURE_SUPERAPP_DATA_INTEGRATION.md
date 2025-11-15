# Super App Data Integration Feature

## Overview
Mobile pet management platform analytics enabling customer engagement tracking, pet profile insights, and user behavior analysis through 21 dbt models processing 17 BigQuery source tables.

**Author**: Anmar Abbas DataGo | **Timeline**: October 25-27, 2025 | **Status**: Production

---

## Commit History

**Commit 1 - Staging Layer** (Oct 25, 2025)
- Hash: `7b86fa3505af23d0381ff0a5611b657cdc7cdf11`
- 17 staging models created (+662 lines)

**Commit 2 - Analytics Layer** (Oct 27, 2025)
- Hash: `b2d7b4d1728bd2422bd7764cbc5b9f9da55bc3fe`
- 2 intermediate + 2 fact models (+493 lines)

---

## Model Architecture

| Layer | Path | Models | Purpose |
|-------|------|--------|---------|
| **Staging** | `models/1_stg/SuperApp/` | 17 | Raw data from BigQuery `public` schema |
| **Intermediate** | `models/2_int/SuperApp/` | 2 | Transformations, aggregations, segmentation |
| **Fact** | `models/3_fct/SuperApp/` | 2 | Production analytics tables |

**Key Models**:
- `fct_superapp_users` (70 columns) - User engagement, pet ownership, segmentation
- `fct_superapp_pets` (87 columns) - Pet demographics, health, vaccinations

**Data Sources**: 17 tables (users, Pets, ActivityLevel, Allergies, DietaryPreference, HealthCondition, PersonalityTrait, PetType, PetSubType, PetAllergies, PetDietaryPreferences, PetHealthConditions, PetPersonalityTraits, PetVaccination, PetDocument, PetDocumentGroup, PetImage)

---

## Key Metrics & Segmentation

**User Segmentation** (`fct_superapp_users`):
- **Engagement**: Highly Engaged | Moderately Engaged | Low Engagement | Inactive
- **User Type**: No Pets | Single Pet Owner | Multi Pet Owner | Pet Enthusiast (4+)
- **Vaccination Compliance**: No Vaccinations | Non-Compliant | Compliant | Fully Vaccinated

**Pet Analytics** (`fct_superapp_pets`):
- **Vaccination Status**: Overdue | Due Soon (30 days) | No Schedule | Up to Date
- **Pet Status**: Active | Incomplete | Deleted

---

## Business Use Cases

1. **Customer Retention** - Identify at-risk users, target re-engagement campaigns
2. **Pet Demographics** - Product optimization, inventory planning by breed/type
3. **Vaccination Management** - Automated reminders, compliance tracking
4. **Loyalty Programs** - Reward effectiveness, multi-pet owner analysis
5. **Health Insights** - Allergy trends, dietary preferences, product development

---

## Sample Query
```sql
SELECT engagement_level, user_segment, COUNT(*) as users, AVG(total_pets) as avg_pets
FROM {{ ref('fct_superapp_users') }}
GROUP BY 1, 2
```

---

## Technical Summary

**Integration**: Fivetran CDC → BigQuery (`tps-data-386515.public`) → dbt
**Materialization**: Staging (view) | Intermediate (ephemeral) | Fact (table/incremental)
**Performance**: Partition by `snapshot_date`, cluster by `user_id`, `engagement_level`

**Next Steps**: Add dbt tests, create BI dashboards, implement incremental models, monitor data freshness

---

## Business Value
- **Customer 360**: Pet ownership insights with user demographics
- **Engagement Tracking**: 4-tier scoring for retention analytics
- **Vaccination Management**: Compliance monitoring and reminder automation
- **Marketing Segmentation**: Data-driven customer targeting (4 segments)
- **Product Intelligence**: Health/dietary trends for inventory planning

**Deployment**: Production-ready | **Models**: 21 | **Metrics**: 157 columns | **Implementation**: 2 days
