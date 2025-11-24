# MBA & Pareto dbt Implementation Documentation

## Overview
This document summarizes the implemented Market Basket Analysis (MBA) and Pareto item logic in dbt. The implementation moves heavy transformations out of Power BI, delivers curated models, and exposes consistent metrics for downstream consumption.

## Scope Highlights
- Full MBA pipeline: staging order items, generating unique product pairs, computing support/confidence metrics, and surfacing product-level support in `dim_items`.
- Pareto fact table: 18 revenue-based metrics (revenue, dense rank, cumulative %) across six merchandising dimensions (global, primary_sales_channel, division, category, subcategory, brand).
- Comprehensive YAML documentation with descriptions and tests for direct Power BI consumption.

## Implementation Details

### 1. int_mba_order_items (`models/2_int/5_item/`)
   - **Source**: `fact_commercial`
   - **Filters**: `transaction_type = 'Sale'`, removes NULL `unified_order_id` and `item_no_`
   - **Deduplication**: Uses `ROW_NUMBER()` partitioned by `(unified_order_id, product_id)`
   - **Output**: One row per unique product per order with division/category/subcategory/brand attributes
   - **Schema**: `models/2_int/5_item/schema.yml` with not-null tests and unique combination test
   
### 2. fct_product_pairs (`models/3_fct/mba-pareto/`)
   - **Logic**: Self-join on `unified_order_id` with `product_id < product_id` to avoid duplicates
   - **Output**: Product pairs with all attributes for both products plus `pair_order_count`
   - **Schema**: Documented in `models/3_fct/mba-pareto/schema.yml` with not-null tests

### 3. fct_product_pair_metrics (`models/3_fct/mba-pareto/`)
   - **Calculations**: 
     - `total_orders`: Count of distinct orders
     - `support`: `pair_order_count / total_orders`
     - `confidence_p1_to_p2`: `pair_order_count / product1_order_count`
     - `confidence_p2_to_p1`: `pair_order_count / product2_order_count`
   - **Safety**: Uses `NULLIF` to prevent division by zero
   - **Schema**: Full documentation with metric descriptions

### 4. dim_items Update (`models/3_fct/`)
   - **MBA Integration**: Left joins `mba_support_score` from product pair metrics
   - **Logic**: Unions product occurrences from both pair positions, aggregates to get max order count
   - **Output**: `mba_support_score` with `COALESCE(0)` for items without pair data

### 5. fct_pareto_items (`models/3_fct/mba-pareto/`)
   - **CTE Flow**: `base_revenue` → `base_items` → `ranking_window_calculations` → `final_output`
   - **Dimensions**: Calculates metrics across 6 dimensions (global, channel, division, category, subcategory, brand)
   - **Metrics per Dimension**: revenue, dense_rank, cumulative_pct (18 columns total)
   - **Schema**: Comprehensive documentation with unique test on `item_id`

## Testing & Documentation
- **Schema Files**:
  - `models/2_int/5_item/schema.yml` - Documents `int_mba_order_items`
  - `models/3_fct/mba-pareto/schema.yml` - Documents all MBA-Pareto fact models
- **Running Models**:
  ```bash
  # Run MBA pipeline and dependencies
  dbt run --select +int_mba_order_items+
  
  # Run Pareto analysis
  dbt run --select fct_pareto_items
  
  # Run all MBA-Pareto models
  dbt run --select int_mba_order_items fct_product_pairs fct_product_pair_metrics dim_items fct_pareto_items
  
  # Run tests
  dbt test --select int_mba_order_items fct_product_pairs fct_product_pair_metrics dim_items fct_pareto_items
  ```

## Power BI Consumption
Power BI should import the following tables:
  
- **`int_mba_order_items`** - Order-level product list for basket analysis
  - Use for: Order-item counts, product co-occurrence lists
  
- **`fct_product_pair_metrics`** - Product pair association metrics
  - Use for: Market basket visuals, cross-sell recommendations
  - Key columns: `support`, `confidence_p1_to_p2`, `confidence_p2_to_p1`
  
- **`fct_pareto_items`** - Multi-dimensional Pareto analysis
  - Use for: 80/20 analysis, top performer identification across dimensions
  - Key columns: `rank_*`, `cumulative_pct_*` for each dimension
  
- **`dim_items`** (updated) - Product dimension with MBA scoring
  - Use for: Product filtering and `mba_support_score` ranking
  - MBA integration: Exposes which products appear most frequently in baskets

## Model Dependencies
```
fact_commercial
    ↓
int_mba_order_items
    ↓
fct_product_pairs
    ↓
fct_product_pair_metrics
    ↓
dim_items (mba_support_score)

fact_commercial + dim_items
    ↓
fct_pareto_items
```

This implementation keeps Power BI models lean while centralizing complex logic in dbt with fully documented, tested contracts.

