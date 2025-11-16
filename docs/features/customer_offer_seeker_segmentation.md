# Offer Seeker Segmentation - Technical Documentation

## Overview
Added "Offer Seeker" segmentation to `dim_customers` model to identify customers who frequently use discounts and offers for targeted promotional campaigns.

---

## Business Requirement
The CRM team requested a way to identify customers who have used offers/discounts at least 3 times to:
- Target price-sensitive customers with promotional campaigns
- Optimize discount strategy and ROI
- Segment customers by offer responsiveness
- Identify customers who may not need discounts (focus on value messaging)

---

## Business Definition
**Offer Seeker**: Customers who have shopped using offers/discounts at least 3 times

---

## Implementation Summary

### Data Source
- **Model**: `int_order_lines`
- **Key Fields**: 
  - `has_discount` (1 = order line has discount)
  - `discount_amount` (total discount value in AED)
  - `online_offer_no_` and `offline_offer_no_` (offer codes)

### New CTE Added
**`customer_offer_usage`** in `int_customers.sql`
- Aggregates discount data from `int_order_lines` at customer level
- Counts distinct orders with discounts (not line items)
- Sums total discount amounts
- Counts unique offer codes used

---

## New Fields in dim_customers (5 fields)

| Field Name | Type | Description | Values |
|------------|------|-------------|--------|
| `orders_with_discount_count` | Fact | Count of orders where customer used a discount/offer | Integer (0+) |
| `total_discount_amount` | Fact | Total AED amount saved through discounts | Decimal (AED) |
| `distinct_offers_used` | Fact | Count of unique offer codes used | Integer (0+) |
| `offer_seeker_segment` | Dimension | Customer segment based on offer usage | See below |
| `offer_seeker_segment_order` | Dimension | Sort order for segment | 1-3 |

---

## Segment Definitions

| Segment | Criteria | Sort Order | Use Case |
|---------|----------|------------|----------|
| **Offer Seeker** | Used discounts on ≥3 orders | 1 | Target with promotional campaigns |
| **Occasional Offer User** | Used discounts on 1-2 orders | 2 | Nurture with selective offers |
| **Non-Offer User** | Never used discounts | 3 | Focus on value/quality messaging |

---

## Technical Implementation

### Step 1: Calculate Offer Usage (in `int_customers`)
New CTE `customer_offer_usage` aggregates discount data:

```sql
customer_offer_usage AS (
    SELECT 
        ol.unified_customer_id,
        
        -- Count orders with discounts/offers
        COUNT(DISTINCT CASE 
            WHEN ol.has_discount = 1 
            THEN ol.unified_order_id 
        END) AS orders_with_discount_count,
        
        -- Total discount amount
        SUM(CASE 
            WHEN ol.transaction_type = 'Sale' 
            THEN ABS(COALESCE(ol.discount_amount, 0))
            ELSE 0
        END) AS total_discount_amount,
        
        -- Count distinct offers used
        COUNT(DISTINCT CASE 
            WHEN ol.online_offer_no_ IS NOT NULL AND ol.online_offer_no_ != '' 
            THEN ol.online_offer_no_
        END) + COUNT(DISTINCT CASE 
            WHEN ol.offline_offer_no_ IS NOT NULL AND ol.offline_offer_no_ != '' 
            THEN ol.offline_offer_no_
        END) AS distinct_offers_used
        
    FROM {{ ref('int_order_lines') }} ol
    WHERE ol.unified_customer_id IS NOT NULL
        AND ol.transaction_type = 'Sale'
    GROUP BY ol.unified_customer_id
)
```

### Step 2: Add Segmentation Logic
In `customer_calculated_metrics` CTE:

```sql
-- Offer Seeker Segment
CASE 
    WHEN orders_with_discount_count >= 3 THEN 'Offer Seeker'
    WHEN orders_with_discount_count BETWEEN 1 AND 2 THEN 'Occasional Offer User'
    ELSE 'Non-Offer User'
END AS offer_seeker_segment,

CASE 
    WHEN orders_with_discount_count >= 3 THEN 1
    WHEN orders_with_discount_count BETWEEN 1 AND 2 THEN 2
    ELSE 3
END AS offer_seeker_segment_order
```

### Step 3: Expose in `dim_customers`
All fields flow through to the final dimension table.

---

## Data Flow

```
int_order_lines (has_discount, discount_amount, offer codes)
        ↓
customer_offer_usage CTE (aggregates per customer)
        ↓
customer_combined (joins to customer data)
        ↓
customer_calculated_metrics (applies segmentation logic)
        ↓
customer_segments (final output)
        ↓
dim_customers (exposed for BI/CRM)
```

---

## Key Logic Points
✅ Uses `DISTINCT unified_order_id` to count orders (not line items)  
✅ Filters `transaction_type = 'Sale'` to exclude refunds  
✅ Tracks both online and offline offer codes  
✅ NULL-safe with COALESCE defaults  
✅ Threshold: **≥3 orders = Offer Seeker**  

---

## CRM Usage Examples

### Example 1: Count Offer Seekers
```sql
SELECT 
    offer_seeker_segment,
    COUNT(*) as customer_count,
    SUM(total_sales_value) as total_revenue,
    AVG(total_discount_amount) as avg_discount_per_customer
FROM dim_customers
GROUP BY offer_seeker_segment
ORDER BY offer_seeker_segment_order
```

### Example 2: Offer Seekers by Channel
```sql
SELECT 
    customer_channel_distribution,
    COUNT(*) as offer_seeker_count,
    AVG(orders_with_discount_count) as avg_discount_orders
FROM dim_customers
WHERE offer_seeker_segment = 'Offer Seeker'
GROUP BY customer_channel_distribution
```

### Example 3: RFM Analysis of Offer Seekers
```sql
SELECT 
    customer_rfm_segment,
    offer_seeker_segment,
    COUNT(*) as customer_count,
    AVG(monetary_avg_order_value) as avg_order_value
FROM dim_customers
GROUP BY customer_rfm_segment, offer_seeker_segment
ORDER BY customer_rfm_segment_order, offer_seeker_segment_order
```

### Example 4: High-Value Offer Seekers
```sql
SELECT 
    source_no_,
    customer_name,
    orders_with_discount_count,
    total_discount_amount,
    total_sales_value,
    customer_rfm_segment
FROM dim_customers
WHERE offer_seeker_segment = 'Offer Seeker'
  AND total_sales_value > 5000
ORDER BY total_sales_value DESC
```

### Example 5: Non-Offer Users (Premium Segment)
```sql
SELECT 
    source_no_,
    customer_name,
    total_sales_value,
    total_order_count,
    customer_rfm_segment
FROM dim_customers
WHERE offer_seeker_segment = 'Non-Offer User'
  AND total_sales_value > 3000
ORDER BY total_sales_value DESC
```

---

## CRM Campaign Strategies

### For Offer Seekers (≥3 discount orders)
**Strategy**: Leverage price sensitivity

**Tactics**:
- Send exclusive offer codes
- Early access to sales
- Bundle deals and volume discounts
- Loyalty program enrollment with points/rewards
- Flash sales notifications
- Seasonal promotion alerts

### For Occasional Offer Users (1-2 discount orders)
**Strategy**: Test offer responsiveness

**Tactics**:
- Targeted seasonal promotions
- Birthday/anniversary offers
- Category-specific discounts
- Free shipping thresholds
- First-time category purchase discounts
- Limited-time offers

### For Non-Offer Users
**Strategy**: Value-based messaging

**Tactics**:
- Product quality highlights
- Premium service features
- Expert recommendations
- Convenience benefits
- New product launches
- Educational content
- VIP experiences

---

## Data Quality Notes

1. **Discount Detection**: Based on `has_discount` flag in `int_order_lines`
2. **Offer Codes**: Tracks both online and offline offer numbers
3. **Transaction Type**: Only counts 'Sale' transactions (excludes refunds)
4. **Historical Data**: Includes all historical orders since customer acquisition
5. **Order-Level Counting**: Uses DISTINCT `unified_order_id` to avoid double-counting

---

## Validation Queries

### Check Segment Distribution
```sql
SELECT 
    offer_seeker_segment,
    COUNT(*) as customers,
    ROUND(AVG(orders_with_discount_count), 2) as avg_discount_orders,
    ROUND(AVG(total_discount_amount), 2) as avg_discount_amount,
    ROUND(AVG(total_sales_value), 2) as avg_lifetime_value
FROM dim_customers
GROUP BY offer_seeker_segment
ORDER BY offer_seeker_segment_order
```

### Verify Segmentation Logic
```sql
-- Check that Offer Seekers have >= 3 discount orders
SELECT 
    source_no_,
    offer_seeker_segment,
    orders_with_discount_count
FROM dim_customers
WHERE offer_seeker_segment = 'Offer Seeker'
  AND orders_with_discount_count < 3
-- Should return 0 rows
```

### Cross-Check with Source Data
```sql
-- Compare customer-level aggregation with source
WITH customer_discount_check AS (
    SELECT 
        source_no_,
        COUNT(DISTINCT CASE 
            WHEN has_discount = 1 
            THEN unified_order_id 
        END) AS calculated_discount_orders
    FROM fact_commercial
    WHERE transaction_type = 'Sale'
    GROUP BY source_no_
)
SELECT 
    dc.source_no_,
    dc.orders_with_discount_count as dim_count,
    cdc.calculated_discount_orders as source_count,
    dc.orders_with_discount_count - cdc.calculated_discount_orders as difference
FROM dim_customers dc
JOIN customer_discount_check cdc ON dc.source_no_ = cdc.source_no_
WHERE dc.orders_with_discount_count != cdc.calculated_discount_orders
-- Should return 0 rows
```

---

## Files Modified

1. **`models/2_int/0_final/int_customers.sql`**
   - Added `customer_offer_usage` CTE (lines 221-251)
   - Added offer fields to `customer_combined` (lines 338-341)
   - Added segmentation logic to `customer_calculated_metrics` (lines 574-585)
   - Exposed fields in `customer_segments` (lines 768-773)

2. **`models/3_fct/dim_customers.sql`**
   - Added 5 new fields (lines 89-94)
   - Added inline comments for documentation

---

## Deployment Steps

```bash
# Run the updated models
dbt run --models int_customers dim_customers

# Test the output
dbt test --models dim_customers

# Validate data
dbt run-operation validate_offer_seeker_logic

# Generate documentation
dbt docs generate
```

---

## Performance Impact
- Minimal overhead: Single aggregation pass through `int_order_lines`
- All calculations at build time, not query time
- No impact on existing queries or dashboards
- Indexed joins on `unified_customer_id`

---

## Business Impact
- Enables targeted promotional campaigns for price-sensitive customers
- Supports segmented marketing strategies based on offer responsiveness
- Provides insights into discount effectiveness and customer behavior
- Helps optimize discount budget allocation
- Identifies premium customers who don't need discounts

---

## Future Enhancements
1. **Time-based Segments**: Recent offer usage vs historical
2. **Offer Type Analysis**: Percentage discount vs fixed amount vs free shipping
3. **Category-Specific Offers**: Track which product categories drive offer usage
4. **Offer Effectiveness**: Measure incremental revenue from offers
5. **Propensity Scoring**: Predict likelihood to respond to offers

---

## Support & Questions
For questions or modifications, contact the Data Engineering team.

**Last Updated**: November 9, 2025  
**Version**: 1.0  
**Author**: Data Engineering Team
