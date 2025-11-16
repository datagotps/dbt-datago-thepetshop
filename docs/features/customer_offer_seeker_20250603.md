# Feature Name: Customer Offer Seeker Segmentation

## 1. Business Purpose
Enables **customer-level segmentation based on discount dependency behavior**, identifying which customers are price-sensitive "offer seekers" vs. loyal full-price buyers, to optimize marketing spend and pricing strategy.

**Why it was built**: To answer strategic customer behavior questions:
- Which customers only purchase during promotions vs. buy at full price?
- What is the Customer Lifetime Value (CLV) difference between discount-dependent and full-price customers?
- How should we target different customer segments with personalized offers?
- Which customers have the highest promotional responsiveness?
- Can we reduce discounts for certain customers without losing them?
- What's the retention rate for customers acquired through promotions?

**Business Value**:
- **Targeted Marketing**: Send promotions only to discount-responsive customers, saving on blanket discounting
- **CLV Optimization**: Focus acquisition on full-price buyers with higher lifetime value
- **Margin Protection**: Identify customers willing to pay full price and stop over-discounting to them
- **Personalization**: Tailor offers based on price sensitivity (deep discounts vs. premium loyalty rewards)
- **Retention Strategy**: Different retention tactics for offer seekers vs. brand loyalists
- **Pricing Power**: Understand which customer segments have pricing elasticity

**Dashboard Usage**: Powers customer segmentation dashboards, marketing campaign targeting, personalized offer engines, CLV models by segment, and pricing strategy simulations.

## 2. Technical Overview (dbt)

### Foundation Reference
This feature builds on top of the **Commercial Discount Analysis** feature documented in:
`/docs/features/commercial_discount_analysis_20250603.md`

That feature provides the order line-level discount data. This feature aggregates it to the customer level.

**Models to be Modified** (Implementation Required):

*Customer Aggregation Layer:*
- `int_customers` - **Add discount behavior metrics** (not yet implemented)
- `dim_customers` - **Add offer seeker segments** (not yet implemented)

**Data Flow**:
```
int_order_lines (has_discount, discount_amount, sales_amount_gross)
        │
        ▼
    GROUP BY unified_customer_id
        │
        ▼
int_customers (NEW FIELDS: discount_usage_rate, avg_discount_pct, offer_seeker_segment)
        │
        ▼
dim_customers (customer segments for BI)
```

**Upstream Sources**:
- `int_order_lines` - Order line discount flags and amounts
- `int_orders` - Order-level aggregations
- Existing `int_customers` - Customer master with RFM

**Downstream Consumers**:
- Customer segmentation dashboards
- Marketing automation (campaign targeting)
- Personalized offer engines
- CLV prediction models
- Pricing strategy tools

**Proposed SQL Logic** (To be Implemented):

### 1. Discount Behavior Metrics (int_customers)

Add to the customer aggregation CTEs:

```sql
customer_discount_metrics AS (
    SELECT
        unified_customer_id,

        -- Order counts
        COUNT(DISTINCT unified_order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN has_discount = 1 THEN unified_order_id END) AS discounted_orders,
        COUNT(DISTINCT CASE WHEN has_discount = 0 THEN unified_order_id END) AS full_price_orders,

        -- Discount usage rate
        ROUND(
            COUNT(DISTINCT CASE WHEN has_discount = 1 THEN unified_order_id END) /
            NULLIF(COUNT(DISTINCT unified_order_id), 0),
            3
        ) AS discount_usage_rate,

        -- Average discount percentage when used
        ROUND(
            AVG(
                CASE
                    WHEN has_discount = 1 AND sales_amount_gross > 0
                    THEN discount_amount / NULLIF(sales_amount_gross, 0)
                END
            ),
            3
        ) AS avg_discount_percentage,

        -- Revenue breakdown
        SUM(CASE WHEN has_discount = 1 THEN sales_amount__actual_ ELSE 0 END) AS discounted_revenue,
        SUM(CASE WHEN has_discount = 0 THEN sales_amount__actual_ ELSE 0 END) AS full_price_revenue,

        -- Total discount amount given to this customer
        SUM(CASE WHEN has_discount = 1 THEN discount_amount ELSE 0 END) AS total_discount_received,

        -- First order discount flag
        MAX(CASE WHEN customer_order_sequence = 1 AND has_discount = 1 THEN 1 ELSE 0 END) AS first_order_was_discounted,

        -- Most recent order discount flag
        MAX(CASE WHEN customer_order_sequence = total_lifetime_orders AND has_discount = 1 THEN 1 ELSE 0 END) AS last_order_was_discounted,

        -- Consecutive discount streaks
        MAX(consecutive_discount_orders) AS max_consecutive_discount_orders,

        -- Channel-specific discount usage
        ROUND(
            COUNT(DISTINCT CASE WHEN has_discount = 1 AND sales_channel = 'Online' THEN unified_order_id END) /
            NULLIF(COUNT(DISTINCT CASE WHEN sales_channel = 'Online' THEN unified_order_id END), 0),
            3
        ) AS online_discount_usage_rate,

        ROUND(
            COUNT(DISTINCT CASE WHEN has_discount = 1 AND sales_channel = 'Shop' THEN unified_order_id END) /
            NULLIF(COUNT(DISTINCT CASE WHEN sales_channel = 'Shop' THEN unified_order_id END), 0),
            3
        ) AS offline_discount_usage_rate

    FROM {{ ref('int_orders') }}
    WHERE unified_customer_id IS NOT NULL
        AND transaction_type = 'Sale'
    GROUP BY unified_customer_id
)
```

### 2. Offer Seeker Segmentation (int_customers)

Add segmentation logic:

```sql
customer_offer_seeker_segments AS (
    SELECT
        cdm.*,

        -- Primary Offer Seeker Segment
        CASE
            WHEN cdm.discount_usage_rate >= 0.80 THEN 'High Discount Dependency'
            WHEN cdm.discount_usage_rate >= 0.50 THEN 'Moderate Offer Seeker'
            WHEN cdm.discount_usage_rate >= 0.20 THEN 'Occasional Discount User'
            WHEN cdm.discount_usage_rate < 0.20 THEN 'Full Price Buyer'
            ELSE 'Unknown'
        END AS offer_seeker_segment,

        -- Sort order for segments
        CASE
            WHEN cdm.discount_usage_rate >= 0.80 THEN 1
            WHEN cdm.discount_usage_rate >= 0.50 THEN 2
            WHEN cdm.discount_usage_rate >= 0.20 THEN 3
            WHEN cdm.discount_usage_rate < 0.20 THEN 4
            ELSE 5
        END AS offer_seeker_segment_order,

        -- Detailed segment (combines frequency + intensity)
        CASE
            WHEN cdm.discount_usage_rate >= 0.80 AND cdm.avg_discount_percentage >= 0.25
                THEN 'Heavy Discount Seeker (High Frequency + Deep Discounts)'
            WHEN cdm.discount_usage_rate >= 0.80 AND cdm.avg_discount_percentage < 0.25
                THEN 'Frequent Discount User (High Frequency + Modest Discounts)'
            WHEN cdm.discount_usage_rate >= 0.50 AND cdm.avg_discount_percentage >= 0.25
                THEN 'Moderate Discount Seeker (Mid Frequency + Deep Discounts)'
            WHEN cdm.discount_usage_rate >= 0.50 AND cdm.avg_discount_percentage < 0.25
                THEN 'Occasional Promo Buyer (Mid Frequency + Modest Discounts)'
            WHEN cdm.discount_usage_rate >= 0.20
                THEN 'Opportunistic Discount User'
            WHEN cdm.discount_usage_rate < 0.20
                THEN 'Premium Full Price Buyer'
            ELSE 'Unknown'
        END AS offer_seeker_segment_detailed,

        -- Promotional responsiveness score (0-100)
        CAST(
            (cdm.discount_usage_rate * 60) +
            (COALESCE(cdm.avg_discount_percentage, 0) * 40)
            AS INT64
        ) AS promotional_responsiveness_score,

        -- Acquisition type flag
        CASE
            WHEN cdm.first_order_was_discounted = 1 THEN 'Promo Acquired'
            ELSE 'Organic Acquired'
        END AS customer_acquisition_type,

        -- Discount dependency trend
        CASE
            WHEN cdm.first_order_was_discounted = 0 AND cdm.last_order_was_discounted = 1
                THEN 'Increasing Discount Dependency'
            WHEN cdm.first_order_was_discounted = 1 AND cdm.last_order_was_discounted = 0
                THEN 'Decreasing Discount Dependency'
            WHEN cdm.first_order_was_discounted = 1 AND cdm.last_order_was_discounted = 1
                THEN 'Consistent Discount User'
            ELSE 'Stable Full Price Buyer'
        END AS discount_dependency_trend,

        -- Channel preference alignment
        CASE
            WHEN cdm.online_discount_usage_rate > cdm.offline_discount_usage_rate + 0.2
                THEN 'Online Discount Seeker'
            WHEN cdm.offline_discount_usage_rate > cdm.online_discount_usage_rate + 0.2
                THEN 'Offline Discount Seeker'
            ELSE 'Omnichannel Discount User'
        END AS discount_channel_preference

    FROM customer_discount_metrics cdm
)
```

### 3. Marketing Action Recommendations (Optional Enhancement)

```sql
customer_marketing_actions AS (
    SELECT
        coss.*,

        -- Recommended action
        CASE
            -- High value + full price = VIP treatment
            WHEN coss.offer_seeker_segment = 'Full Price Buyer'
                 AND coss.monetary_total_value >= (SELECT PERCENTILE_CONT(monetary_total_value, 0.8) FROM int_customers)
            THEN 'VIP Program - Premium Perks, No Discounts Needed'

            -- Full price but low value = growth potential
            WHEN coss.offer_seeker_segment = 'Full Price Buyer'
                 AND coss.monetary_total_value < (SELECT PERCENTILE_CONT(monetary_total_value, 0.8) FROM int_customers)
            THEN 'Loyalty Rewards - Build Value Without Discounts'

            -- Occasional user = strategic promotions
            WHEN coss.offer_seeker_segment = 'Occasional Discount User'
            THEN 'Seasonal Promotions - Limited Time Offers'

            -- Moderate seeker = targeted campaigns
            WHEN coss.offer_seeker_segment = 'Moderate Offer Seeker'
            THEN 'Targeted Campaigns - Category-Specific Offers'

            -- High dependency = at-risk, needs intervention
            WHEN coss.offer_seeker_segment = 'High Discount Dependency'
                 AND coss.customer_recency_segment IN ('Active', 'Recent')
            THEN 'Intervention Required - Test Price Tolerance'

            -- High dependency + churned = win-back
            WHEN coss.offer_seeker_segment = 'High Discount Dependency'
                 AND coss.customer_recency_segment NOT IN ('Active', 'Recent')
            THEN 'Win-Back Campaign - Deep Discount Offer'

            ELSE 'Standard Marketing'
        END AS recommended_marketing_action,

        -- Discount tolerance (how much can we reduce discounts?)
        CASE
            WHEN coss.offer_seeker_segment = 'Full Price Buyer' THEN 'High - No Discounts Needed'
            WHEN coss.offer_seeker_segment = 'Occasional Discount User' THEN 'Medium - Reduce Frequency'
            WHEN coss.offer_seeker_segment = 'Moderate Offer Seeker' THEN 'Low - Reduce Depth'
            WHEN coss.offer_seeker_segment = 'High Discount Dependency' THEN 'Very Low - Risk of Churn'
            ELSE 'Unknown'
        END AS discount_reduction_tolerance

    FROM customer_offer_seeker_segments coss
)
```

**KPI Definitions to be Created**:

*Discount Behavior Metrics:*
- `discount_usage_rate` - % of orders with discounts (0.0 to 1.0)
- `avg_discount_percentage` - Average % off when discount used (0.0 to 1.0)
- `discounted_orders` - Count of orders with discounts
- `full_price_orders` - Count of orders at full price
- `total_discount_received` - Total AED value of discounts given to customer

*Acquisition & Trend:*
- `first_order_was_discounted` - 1 if first order had discount, 0 otherwise
- `last_order_was_discounted` - 1 if most recent order had discount
- `customer_acquisition_type` - "Promo Acquired" vs "Organic Acquired"
- `discount_dependency_trend` - Increasing, Decreasing, Consistent, Stable

*Segmentation:*
- `offer_seeker_segment` - Primary segment (4 categories)
- `offer_seeker_segment_detailed` - Detailed segment (6+ categories)
- `promotional_responsiveness_score` - 0-100 score
- `discount_channel_preference` - Online, Offline, or Omnichannel discount behavior

*Marketing Actions:*
- `recommended_marketing_action` - Suggested campaign type
- `discount_reduction_tolerance` - Risk level for reducing discounts

## 3. Model Lineage (high-level)

```
Foundation (Already Implemented):
────────────────────────────────────
int_order_lines
    ├─ has_discount (1/0 flag)
    ├─ discount_amount (AED)
    ├─ sales_amount_gross (AED)
    └─ sales_amount__actual_ (AED)
        │
        ▼
    int_orders
    ├─ unified_order_id
    ├─ unified_customer_id
    ├─ customer_order_sequence
    └─ total_lifetime_orders


Customer Aggregation (To Be Implemented):
──────────────────────────────────────────
int_orders (with discount fields)
        │
        ▼
    ┌─────────────────────────────────┐
    │ customer_discount_metrics CTE   │
    │                                 │
    │ GROUP BY unified_customer_id    │
    │                                 │
    │ → discount_usage_rate           │
    │ → avg_discount_percentage       │
    │ → discounted_orders             │
    │ → full_price_orders             │
    │ → total_discount_received       │
    │ → first_order_was_discounted    │
    │ → last_order_was_discounted     │
    │ → online_discount_usage_rate    │
    │ → offline_discount_usage_rate   │
    └─────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────┐
    │ customer_offer_seeker_segments  │
    │                                 │
    │ CASE WHEN logic:                │
    │                                 │
    │ → offer_seeker_segment          │
    │   - High Discount Dependency    │
    │   - Moderate Offer Seeker       │
    │   - Occasional Discount User    │
    │   - Full Price Buyer            │
    │                                 │
    │ → promotional_responsiveness    │
    │ → customer_acquisition_type     │
    │ → discount_dependency_trend     │
    └─────────────────────────────────┘
        │
        ▼
    ┌─────────────────────────────────┐
    │ customer_marketing_actions      │
    │ (Optional)                      │
    │                                 │
    │ → recommended_marketing_action  │
    │ → discount_reduction_tolerance  │
    └─────────────────────────────────┘
        │
        ▼
    int_customers (enriched)
        │
        ▼
    dim_customers (BI layer)
```

## 4. Customer Segments Defined

### Primary Segments (4 Categories)

| Segment | Discount Usage Rate | Characteristics | Typical Behavior |
|---------|---------------------|-----------------|------------------|
| **Full Price Buyer** | < 20% | Brand loyal, quality-focused | Shops without waiting for sales |
| **Occasional Discount User** | 20% - 49% | Value-conscious, strategic | Uses discounts opportunistically |
| **Moderate Offer Seeker** | 50% - 79% | Price-sensitive, promo-aware | Actively looks for deals |
| **High Discount Dependency** | ≥ 80% | Discount-driven, rarely pays full price | Only purchases during promotions |

### Detailed Segments (6 Categories)

Combines **frequency** (how often) with **intensity** (how deep):

| Detailed Segment | Usage Rate | Avg Discount % | Marketing Strategy |
|------------------|------------|----------------|---------------------|
| **Premium Full Price Buyer** | < 20% | Any | VIP program, exclusive access |
| **Opportunistic Discount User** | 20-49% | Any | Seasonal campaigns, flash sales |
| **Occasional Promo Buyer** | 50-79% | < 25% | Targeted category offers |
| **Moderate Discount Seeker** | 50-79% | ≥ 25% | Regular promotional calendar |
| **Frequent Discount User** | ≥ 80% | < 25% | Reduce discount depth gradually |
| **Heavy Discount Seeker** | ≥ 80% | ≥ 25% | Risk of churn, test price tolerance |

### Acquisition Type

| Type | Definition | Implications |
|------|------------|--------------|
| **Promo Acquired** | First order had discount | May be discount-dependent, lower CLV |
| **Organic Acquired** | First order at full price | Higher brand affinity, better retention |

### Discount Dependency Trend

| Trend | Definition | Action |
|-------|------------|--------|
| **Increasing Dependency** | Started full price, now uses discounts | Investigate category shifts |
| **Decreasing Dependency** | Started with discounts, now pays full price | Success! Reward loyalty |
| **Consistent Discount User** | Always uses discounts | Segment permanently |
| **Stable Full Price Buyer** | Never uses discounts | Protect, don't over-promote |

## 5. Business Use Cases

### Use Case 1: Targeted Marketing Campaigns

**Scenario**: Launch a new product category

**Segment-Specific Strategy**:
- **Full Price Buyers**: Early access, no discount needed
- **Occasional Users**: Limited-time 15% off launch offer
- **Moderate Seekers**: Bundle deals, category discounts
- **High Dependency**: Deep 30% off to drive trial

**Expected Outcome**: Higher margin from full price buyers, conversion from discount seekers

### Use Case 2: Discount Optimization

**Scenario**: Marketing budget under pressure

**Action**:
1. Identify "Full Price Buyers" - exclude from blanket promotions (save 20% of discount budget)
2. Reduce discount depth for "Occasional Users" from 25% to 15%
3. Maintain discounts for "High Dependency" to avoid churn
4. A/B test discount reduction tolerance

**Expected Outcome**: 15-25% reduction in discount costs while maintaining 95%+ revenue

### Use Case 3: Customer Acquisition Quality

**Scenario**: Evaluate marketing channel performance

**Analysis**:
```sql
SELECT
    customer_acquisition_channel,
    customer_acquisition_type,
    offer_seeker_segment,
    COUNT(*) as customer_count,
    AVG(monetary_total_value) as avg_clv,
    AVG(total_discount_received) as avg_discount_cost
FROM dim_customers
GROUP BY 1, 2, 3
ORDER BY avg_clv DESC
```

**Insight**: If Google Ads acquires mostly "High Dependency" customers but Facebook acquires "Full Price Buyers", shift budget to Facebook

### Use Case 4: Churn Prevention

**Scenario**: High-value customer shows increasing discount dependency

**Alert**:
- Was "Full Price Buyer" with $5,000 CLV
- Now "Moderate Offer Seeker"
- Last 3 orders all discounted

**Action**: Personal outreach, understand why behavior changed, offer loyalty perks instead of discounts

## 6. Implementation Roadmap

### Phase 1: Foundation Metrics (Week 1-2)
- [ ] Add `customer_discount_metrics` CTE to `int_customers.sql`
- [ ] Calculate discount_usage_rate, avg_discount_percentage
- [ ] Add to `dim_customers.sql` for BI access
- [ ] Create dbt tests for data quality

### Phase 2: Segmentation (Week 3)
- [ ] Add `customer_offer_seeker_segments` CTE
- [ ] Implement 4-category primary segments
- [ ] Add detailed 6-category segments
- [ ] Document segment thresholds

### Phase 3: BI Integration (Week 4)
- [ ] Add fields to dim_customers
- [ ] Create Looker/Tableau dashboard templates
- [ ] Build segment distribution reports
- [ ] Create CLV-by-segment analysis

### Phase 4: Marketing Activation (Week 5-6)
- [ ] Export segments to marketing automation
- [ ] Create audience lists per segment
- [ ] Build segment-specific email templates
- [ ] A/B test discount reduction

### Phase 5: Advanced Analytics (Ongoing)
- [ ] Cohort analysis by offer seeker segment
- [ ] Retention rates by segment
- [ ] Price elasticity by segment
- [ ] Predictive models for segment migration

## 7. dbt Tests to Add

```yaml
# models/2_int/0_final/int_customers.yml

models:
  - name: int_customers
    columns:
      - name: discount_usage_rate
        description: "Percentage of orders with discounts"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1

      - name: avg_discount_percentage
        description: "Average discount percentage when used"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1

      - name: offer_seeker_segment
        description: "Customer segment based on discount usage"
        tests:
          - not_null
          - accepted_values:
              values:
                - 'Full Price Buyer'
                - 'Occasional Discount User'
                - 'Moderate Offer Seeker'
                - 'High Discount Dependency'
                - 'Unknown'

      - name: total_discount_received
        description: "Total discount amount given to customer"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

## 8. Limitations / Assumptions

**Assumptions**:
- **Segment Thresholds**: 20%, 50%, 80% are business-defined, may need adjustment per market
- **Discount Depth Threshold**: 25% separates "modest" from "deep" discounts
- **Equal Weighting**: Frequency and intensity weighted 60/40 in responsiveness score
- **Static Segments**: Customers don't migrate between segments frequently
- **Purchase Frequency**: Assumes minimum 3 orders for reliable segmentation

**Limitations**:

*Data Requirements:*
- **Minimum Order Count**: Customers with 1-2 orders may be misclassified
- **Time Window**: No time-decay weighting (old behavior = recent behavior)
- **Seasonal Effects**: Holiday shopping may skew discount usage rates

*Segment Accuracy:*
- **Intentional Discount Users**: Can't distinguish between "planned to use coupon" vs "found coupon by chance"
- **Gift Buyers**: Different behavior for personal vs. gift purchases not separated
- **Basket Composition**: Discount usage may vary by product category purchased

*Marketing Limitations:*
- **Channel Access**: Requires integration with marketing automation platforms
- **Real-Time Updates**: Segments calculated in batch, not real-time
- **Consent**: GDPR/privacy regulations may limit marketing activation

**Not Yet Implemented**:
❌ Customer-level discount metrics in int_customers
❌ Offer seeker segment fields
❌ Promotional responsiveness scoring
❌ Marketing action recommendations
❌ dbt tests for segment logic
❌ BI dashboard templates

**Future Enhancements**:
- **Dynamic Thresholds**: Calculate segment boundaries from data distribution
- **Time-Weighted Metrics**: Give more weight to recent behavior
- **Category-Specific Segments**: Different segments for different product categories
- **Predictive Segment Migration**: ML model to predict if customer will change segments
- **Lifetime Discount Efficiency**: Discount ROI by customer segment
- **Competitive Benchmarking**: Compare discount usage to industry standards

## 9. Success Metrics

After implementation, track:

**Segmentation Quality**:
- Distribution: Target 40% Full Price, 30% Occasional, 20% Moderate, 10% High Dependency
- Stability: < 10% customers change segments month-over-month
- Predictive Power: Segment explains 60%+ variance in future purchase behavior

**Business Impact**:
- Discount Cost Reduction: 15-20% reduction in total discount expense
- Margin Improvement: 2-3% gross margin improvement
- Revenue Maintenance: < 2% revenue decline from reduced discounting
- Targeting Efficiency: 50%+ increase in promotional ROI

**Customer Value**:
- CLV by Segment: Full Price Buyers 3-5x higher CLV than High Dependency
- Retention: Full Price Buyers 20%+ higher 12-month retention
- Acquisition Quality: Shift acquisition mix toward higher-value segments

## References

This feature depends on and extends:
- **Commercial Discount Analysis** (`/docs/features/commercial_discount_analysis_20250603.md`) - Order line discount tracking
- **Customer Cohort Analysis** (`/docs/features/customer_cohort_analysis_20250808.md`) - Customer lifecycle tracking
- **int_customers** model - RFM segmentation and customer master
