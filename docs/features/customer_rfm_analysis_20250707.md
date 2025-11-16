# Feature Name: Customer RFM Analysis

## 1. Business Purpose

The Customer RFM Analysis enables **data-driven customer segmentation** and **churn prediction** based on purchasing behavior. This feature supports:

- **Customer Segmentation**: Classify customers into 7 strategic segments (Champions, Loyal Customers, Cant Lose Them, Potential Loyalists, New Customers, At Risk, Lost) based on RFM scores
- **Revenue Prioritization**: Identify top-value customers (Top 1%, Top 20%) for VIP treatment and retention programs
- **Churn Prevention**: Predictive churn risk scoring (0-100) with actionable risk levels (Low/Medium/High/Critical) to prevent customer attrition
- **Marketing Targeting**: Enable personalized campaigns based on recency, frequency, and monetary behavior patterns
- **Retention Analytics**: Monitor customer lifecycle stages and predict next purchase timing

**Dashboard Use Cases**: Customer segmentation dashboards, retention analytics, churn risk monitoring, marketing campaign targeting, customer lifetime value (CLV) analysis, VIP customer management.

## 2. Technical Overview (dbt)

### Models Created/Modified:
**Intermediate Layer (1):**
- `int_customers` - Core RFM calculation engine with 1193 lines (models/2_int/0_final/)
  - Materialized as table for performance
  - Processes unified customer IDs with multi-source tracking

**Dimension Layer (1):**
- `dim_customers` - Final customer dimension exposing 100+ attributes (models/3_fct/)
  - Filters to Online/Shop channels only
  - Exposes all RFM metrics, segments, and churn analytics

### Upstream Sources:
- `int_orders` - Order-level data aggregated from Online (OFS) and Offline (ERP) sources
- `int_order_lines` - Line-level order details (foundation for order aggregations)

### Downstream Consumers:
Currently used for customer analytics dashboards and marketing segmentation. No direct downstream dbt dependencies identified.

### Key SQL Logic:

**RFM Score Calculation (int_customers.sql:479-508):**
- **Recency Score (R)**: Business rule-based thresholds
  - Score 5: ≤30 days since last order (most recent)
  - Score 4: 31-60 days
  - Score 3: 61-90 days
  - Score 2: 91-180 days
  - Score 1: >180 days (least recent)
- **Frequency Score (F)**: Percentile-based on total order count
  - Score 5: Top 20% (≥80th percentile)
  - Score 4: 60-80th percentile
  - Score 3: 40-60th percentile
  - Score 2: 20-40th percentile
  - Score 1: Bottom 20% (<20th percentile)
- **Monetary Score (M)**: Percentile-based on total sales value
  - Score 5: Top 20% (≥80th percentile)
  - Score 4: 60-80th percentile
  - Score 3: 40-60th percentile
  - Score 2: 20-40th percentile
  - Score 1: Bottom 20% (<20th percentile)

**RFM Segment Classification (int_customers.sql:624-637):**
- **Champions**: R≥4 AND F≥4 AND M≥4 (high on all dimensions)
- **Loyal Customers**: R≥3 AND F≥3 AND M≥3 (consistent performers)
- **Cant Lose Them**: F≥4 AND M≥4 (high value, may be churning)
- **Potential Loyalists**: R≥4 AND (F≥2 OR M≥2) (recent, growing)
- **New Customers**: Customer tenure ≤90 days OR R≥4
- **At Risk**: F≥2 AND M≥2 OR M IN (2,3) (declining engagement)
- **Lost**: R=1 AND F=1 OR M=1 (minimal activity)

**Customer Value Segmentation (int_customers.sql:609-614):**
- Top 1%: ≤1st percentile by revenue
- Top 20%: ≤20th percentile
- Middle 30-60%: 20-60th percentile
- Bottom 40%: >60th percentile

**Churn Risk Analytics:**
- Churn risk score (0-100 scale) based on recency and order patterns
- Expected next order date prediction using avg_days_between_orders
- Overdue confidence levels (68%/95%/99.7% statistical thresholds)

### KPI Definitions:
- **Recency**: Days since last order (lower = better engagement)
- **Frequency**: Total order count (higher = more loyal)
- **Monetary**: Total sales value in AED (higher = more valuable)
- **RFM Segment Code**: 3-digit code (e.g., "555" = Champion, "111" = Lost)
- **Churn Risk Score**: 0-100 scale (0=no risk, 100=critical risk)
- **Customer Lifetime Value (Implied)**: Total sales value across all orders

## 3. Model Lineage (high-level)

```
SOURCE SYSTEMS
  ├─ MySQL OFS (Online Orders)
  └─ SQL Server ERP (Offline/Shop Orders)
         ↓
STAGING LAYER (1_stg/)
  ├─ Customer staging (OFS + ERP unified)
  ├─ Order staging models
  └─ Line-level staging models
         ↓
INTERMEDIATE LAYER (2_int/0_final/)
  ├─ int_order_lines (line-level aggregation)
  ├─ int_orders (order-level with customer unification)
  └─ int_customers (RFM CALCULATION ENGINE)
         ↓
DIMENSION LAYER (3_fct/)
  └─ dim_customers (final customer dimension with RFM)
```

**Data Flow for RFM:**
1. int_orders provides unified_customer_id, order dates, order counts, sales values
2. int_customers aggregates by customer and calculates RFM scores
3. dim_customers exposes final RFM segments and churn analytics for reporting

## 4. Important Fields Added

**RFM Core Metrics (dim_customers.sql:60-67):**
- `recency_days` - Days since last order (fact)
- `frequency_orders` - Total order count (fact)
- `monetary_total_value` - Total lifetime spend in AED (fact)
- `monetary_avg_order_value` - Average order value in AED (fact)
- `customer_tenure_days` - Days since first order (fact)

**RFM Scores (dim_customers.sql:103-105):**
- `r_score` - Recency score 1-5 (5=most recent)
- `f_score` - Frequency score 1-5 (5=highest frequency)
- `m_score` - Monetary score 1-5 (5=highest value)

**RFM Segments (dim_customers.sql:98-102):**
- `rfm_segment` - 3-digit code combining R-F-M scores (e.g., "555")
- `customer_rfm_segment` - Named segment (Champions, Loyal, At Risk, Lost, etc.)
- `customer_value_segment` - Revenue tier (Top 1%, Top 20%, Middle, Bottom)

**Churn Analytics (dim_customers.sql:114-120):**
- `churn_risk_score` - 0-100 risk score (100=critical churn risk)
- `churn_risk_level` - New Customer, Low, Medium, High, Critical
- `days_until_expected_order` - Predicted days to next purchase
- `is_overdue` - Yes/No flag if customer is late for expected order
- `overdue_confidence` - Statistical confidence (68%/95%/99.7%)
- `churn_action_required` - Action flag (On Track, At Risk, Severely Overdue)

**Behavioral Segmentation:**
- `customer_recency_segment` - Active, Recent, At Risk, Churn, Inactive, Lost
- `purchase_frequency_bucket` - 1 Order, 2-3, 4-6, 7-10, 11+ Orders
- `purchase_frequency_type` - New, One-Time, Weekly, Monthly, Quarterly, Annual, Inconsistent
- `customer_pattern_type` - Pattern consistency (Perfect, Highly Consistent, Variable, etc.)

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `0db360a` | Anmar Abbas DataGo | 2025-07-07 | **Initial customer dimension** - Created dim_customers.sql (127 lines) with basic customer attributes |
| `2d85313` | Anmar Abbas DataGo | 2025-07-07 | **Enhancement** - Added 5 new fields to dim_customers |
| `f95f266` | Anmar Abbas DataGo | 2025-07-18 | **Minor update** - Added report_last_updated_at timestamp field |
| `3b3d38b` | Anmar Abbas DataGo | 2025-08-08 | **Major RFM implementation** - Created int_customers.sql (1,193 lines) with full RFM scoring engine, customer segmentation, and churn analytics |
| `fcc4e4c` | Anmar Abbas DataGo | 2025-08-14 | **RFM expansion** - Enhanced both models (+543 lines to dim_customers, +8 lines to int_customers) with additional segmentation logic |
| `6f818cd` | Anmar Abbas DataGo | 2025-10-06 | **Optimization** - Refactored and simplified both models (-1,379 lines removed, +576 lines added) for performance and maintainability |
| `6cd3a1f` | Anmar Abbas DataGo | 2025-10-15 | **Refinement** - Fine-tuned RFM logic (+27 lines to int_customers, +4 to dim_customers) |

**Total Development Effort:** 7 commits over 100 days (Jul 7 - Oct 15, 2025)

## 6. Limitations / Assumptions

**Limitations:**
- No dbt tests defined for RFM score validation or data quality
- No YAML schema documentation for RFM fields and business logic
- RFM scores recalculate on every run (no incremental logic) - full table refresh
- Percentile calculations run across entire customer base (may be slow at scale)
- No historical tracking of RFM segment changes over time (no SCD Type 2)
- Churn prediction is statistical (based on patterns) not ML-based

**Assumptions:**
- `int_orders` provides clean, unified customer IDs (unified_customer_id)
- Order dates are accurate and not future-dated
- Sales values are in AED and exclude returns/cancellations (or appropriately adjusted)
- Customers with multiple source_no_ are correctly unified by unified_customer_id
- Recency thresholds (30/60/90/180 days) are business-validated and appropriate for the pet retail context
- Percentile-based F/M scores are stable across customer base size changes
- Timezone handling: Reports use UTC+4 (Dubai time) via `DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR)`

**Business Rule Clarifications Needed:**
- Should "New Customers" be excluded from certain RFM segments? (Currently customers ≤90 days tenure are always classified as "New")
- Are the 7 RFM segments sufficient, or should more granular segments be added?
- Should churn risk thresholds be adjusted based on product category (e.g., food vs. accessories)?

**Future Improvements:**
- Add dbt tests for RFM score ranges (1-5), segment membership, and data completeness
- Implement incremental model strategy for int_customers (keyed on unified_customer_id)
- Create RFM segment transition analysis (snapshot changes over time)
- Add YAML documentation with business definitions for all RFM fields
- Consider ML-based churn prediction model for higher accuracy
- Add macro for RFM scoring logic to enable reusability and easier maintenance
- Create alerting for customers moving into "At Risk" or "Lost" segments
- Build cohort analysis to track RFM segment migration patterns

---

**Documentation Generated:** 2025-11-15
**Feature Commit Range:** 0db360a → 6cd3a1f (2025-07-07 to 2025-10-15)
**Total Models:** 2 (1 intermediate, 1 dimension)
**Lines of Code:** ~1,300 (current state after optimizations)
