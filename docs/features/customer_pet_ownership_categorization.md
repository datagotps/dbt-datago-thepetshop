# Pet Ownership Categorization - Technical Documentation

## Overview
Added pet ownership analysis to the `dim_customers` model to enable the CRM team to categorize and target customers based on the types of pets they own.

---

## Business Requirement
The CRM team needs to identify and segment customers by pet type (Dog, Cat, Fish, Bird, Small Pet, Reptile) to:
- Send targeted product recommendations
- Create pet-specific marketing campaigns
- Identify cross-sell opportunities for multi-pet owners

---

## Implementation Summary

### Data Source
- **Model**: `int_order_lines`
- **Key Field**: `division` (DOG, CAT, FISH, BIRD, SMALL PET, REPTILE)
- **Logic**: Aggregates purchase history by customer to determine pet ownership

### New CTE Added
**`customer_pet_ownership`** in `int_customers.sql`
- Calculates revenue and order counts per pet type
- Creates ownership flags (1 = has purchased, 0 = has not)
- Filters for Sale transactions only

---

## New Fields in dim_customers (28 fields)

### 1. Revenue Metrics (6 fields)
| Field | Type | Description |
|-------|------|-------------|
| `dog_revenue` | Fact | Total AED spent on dog products |
| `cat_revenue` | Fact | Total AED spent on cat products |
| `fish_revenue` | Fact | Total AED spent on fish products |
| `bird_revenue` | Fact | Total AED spent on bird products |
| `small_pet_revenue` | Fact | Total AED spent on small pet products |
| `reptile_revenue` | Fact | Total AED spent on reptile products |

### 2. Order Count Metrics (6 fields)
| Field | Type | Description |
|-------|------|-------------|
| `dog_orders` | Fact | Count of orders containing dog products |
| `cat_orders` | Fact | Count of orders containing cat products |
| `fish_orders` | Fact | Count of orders containing fish products |
| `bird_orders` | Fact | Count of orders containing bird products |
| `small_pet_orders` | Fact | Count of orders containing small pet products |
| `reptile_orders` | Fact | Count of orders containing reptile products |

### 3. Ownership Flags (6 fields)
| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `is_dog_owner` | Dimension | 0, 1 | 1 = has purchased dog products |
| `is_cat_owner` | Dimension | 0, 1 | 1 = has purchased cat products |
| `is_fish_owner` | Dimension | 0, 1 | 1 = has purchased fish products |
| `is_bird_owner` | Dimension | 0, 1 | 1 = has purchased bird products |
| `is_small_pet_owner` | Dimension | 0, 1 | 1 = has purchased small pet products |
| `is_reptile_owner` | Dimension | 0, 1 | 1 = has purchased reptile products |

### 4. Categorization Fields (4 fields)

#### `pet_types_count` (Fact)
- Count of different pet types owned (0-6)
- Example: Customer buys dog and cat products = 2

#### `primary_pet_type` (Dimension)
- The pet type with highest revenue
- **Values**: Dog, Cat, Fish, Bird, Small Pet, Reptile, No Pet Products
- **Use Case**: Personalize homepage banners

#### `pet_owner_profile` (Dimension) ⭐ PRIMARY CRM FIELD
- Main segmentation field for CRM campaigns
- **Values**:
  - `Dog Owner` - Only purchases dog products
  - `Cat Owner` - Only purchases cat products
  - `Fish Owner` - Only purchases fish products
  - `Bird Owner` - Only purchases bird products
  - `Small Pet Owner` - Only purchases small pet products
  - `Reptile Owner` - Only purchases reptile products
  - `Multi-Pet Owner` - Purchases 2+ pet types
  - `No Pet Products` - No pet-related purchases

#### `multi_pet_detail` (Dimension)
- Additional detail for multi-pet owners
- **Values**:
  - `Dog + Cat Owner` - Only dog and cat products
  - `Dog + Cat + Others` - Dog, cat, plus other pet types
  - `Multi-Pet Owner (3+)` - 3 or more pet types
  - `NULL` - Single-pet or no-pet customers

---

## CRM Use Cases & SQL Examples

### Use Case 1: Target Dog Owners Only
**Campaign**: Dog food promotion
```sql
SELECT 
    source_no_,
    customer_name,
    std_phone_no_,
    dog_revenue,
    dog_orders
FROM dim_customers
WHERE pet_owner_profile = 'Dog Owner'
ORDER BY dog_revenue DESC
```

### Use Case 2: Target All Dog Owners (Including Multi-Pet)
**Campaign**: New dog toy launch
```sql
SELECT 
    source_no_,
    customer_name,
    pet_owner_profile,
    dog_revenue
FROM dim_customers
WHERE is_dog_owner = 1
ORDER BY dog_revenue DESC
```

### Use Case 3: Multi-Pet Owners for Cross-Sell
**Campaign**: Bundle deals across pet categories
```sql
SELECT 
    source_no_,
    customer_name,
    pet_types_count,
    multi_pet_detail,
    total_sales_value
FROM dim_customers
WHERE pet_owner_profile = 'Multi-Pet Owner'
ORDER BY pet_types_count DESC, total_sales_value DESC
```

### Use Case 4: Dog Owners Without Cats (Upsell Opportunity)
**Campaign**: Introduce cat products to dog owners
```sql
SELECT 
    source_no_,
    customer_name,
    dog_revenue,
    dog_orders
FROM dim_customers
WHERE is_dog_owner = 1 
  AND is_cat_owner = 0
  AND dog_revenue > 1000
ORDER BY dog_revenue DESC
```

### Use Case 5: Segment Distribution Analysis
**Purpose**: Understand customer base composition
```sql
SELECT 
    pet_owner_profile,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(total_sales_value), 2) as avg_lifetime_value,
    ROUND(AVG(pet_types_count), 2) as avg_pet_types
FROM dim_customers
GROUP BY pet_owner_profile
ORDER BY customer_count DESC
```

### Use Case 6: High-Value Multi-Pet Owners
**Campaign**: VIP loyalty program invitation
```sql
SELECT 
    source_no_,
    customer_name,
    pet_owner_profile,
    multi_pet_detail,
    pet_types_count,
    total_sales_value,
    customer_rfm_segment
FROM dim_customers
WHERE pet_types_count >= 2
  AND total_sales_value > 5000
ORDER BY total_sales_value DESC
LIMIT 100
```

---

## Business Logic Details

### How Pet Ownership is Determined
1. System analyzes all historical orders from `int_order_lines`
2. Filters for `transaction_type = 'Sale'` (excludes refunds)
3. Groups by `division` field (DOG, CAT, FISH, BIRD, SMALL PET, REPTILE)
4. Aggregates revenue and order counts per customer per pet type
5. Creates binary flags (1/0) for each pet type
6. Determines primary pet type based on highest revenue
7. Categorizes into pet owner profile

### Example Customer Journey
**Customer: C000000034**

**Purchase History:**
- Order 1: Dog food (500 AED)
- Order 2: Cat litter (200 AED)
- Order 3: Dog toys (300 AED)
- Order 4: Dog food (500 AED)
- Order 5: Cat food (150 AED)

**Resulting Fields:**
- `dog_revenue`: 1,300 AED
- `cat_revenue`: 350 AED
- `dog_orders`: 3
- `cat_orders`: 2
- `is_dog_owner`: 1
- `is_cat_owner`: 1
- `is_fish_owner`: 0
- `pet_types_count`: 2
- `primary_pet_type`: "Dog" (highest revenue)
- `pet_owner_profile`: "Multi-Pet Owner"
- `multi_pet_detail`: "Dog + Cat Owner"

---

## Technical Implementation

### Files Modified
1. **`models/2_int/0_final/int_customers.sql`**
   - Added `customer_pet_ownership` CTE (lines 256-289)
   - Joined pet ownership data in `customer_combined` (line 367)
   - Added categorization logic in `customer_calculated_metrics` (lines 587-641)
   - Exposed fields in `customer_segments` (lines 775-797)

2. **`models/3_fct/dim_customers.sql`**
   - Added 28 new fields with inline documentation (lines 96-118)

### Deployment Steps
```bash
# Run the updated models
dbt run --models int_customers dim_customers

# Test the output
dbt test --models dim_customers

# Generate documentation
dbt docs generate
```

---

## Data Quality Checks

### Validation Query 1: Check Distribution
```sql
SELECT 
    pet_owner_profile,
    COUNT(*) as customers,
    ROUND(AVG(total_sales_value), 2) as avg_ltv
FROM dim_customers
GROUP BY pet_owner_profile
ORDER BY customers DESC
```

### Validation Query 2: Verify Logic
```sql
-- Check that multi-pet owners have pet_types_count >= 2
SELECT 
    source_no_,
    pet_owner_profile,
    pet_types_count,
    is_dog_owner + is_cat_owner + is_fish_owner + 
    is_bird_owner + is_small_pet_owner + is_reptile_owner as calculated_count
FROM dim_customers
WHERE pet_owner_profile = 'Multi-Pet Owner'
  AND pet_types_count < 2
-- Should return 0 rows
```

### Validation Query 3: Revenue Consistency
```sql
-- Check that primary_pet_type matches highest revenue
SELECT 
    source_no_,
    primary_pet_type,
    dog_revenue,
    cat_revenue,
    fish_revenue
FROM dim_customers
WHERE primary_pet_type = 'Dog'
  AND (cat_revenue > dog_revenue OR fish_revenue > dog_revenue)
-- Should return 0 rows
```

---

## Performance Considerations
- Pet ownership CTE adds minimal overhead (single pass through order lines)
- All aggregations happen at build time, not query time
- Indexed on `unified_customer_id` for efficient joins
- No impact on existing queries

---

## Future Enhancements
1. **Time-based Analysis**: Add "active pet owner" vs "lapsed pet owner" based on recency
2. **Product Category Depth**: Extend to item_category level (Food vs Accessories)
3. **Predictive Scoring**: Add propensity scores for pet type acquisition
4. **Lifecycle Stages**: Identify new pet owners vs established owners based on purchase patterns

---

## Support & Questions
For questions or modifications, contact the Data Engineering team.

**Last Updated**: November 9, 2025  
**Version**: 1.0  
**Author**: Data Engineering Team
