# MBA & Pareto dbt Plan Documentation

## Overview
This document summarizes the agreed plan to move Market Basket Analysis (MBA) and Pareto item logic into dbt. The goal is to take heavy transformations out of Power BI, deliver curated models, and expose consistent metrics for downstream consumption.

## Scope Highlights
- Full MBA pipeline: staging order items, generating unique product pairs, computing support/confidence metrics, and surfacing product-level support in `dim_items`.
- Pareto mart: 18 revenue-based metrics (revenue, dense rank, cumulative %) across six merchandising dimensions (global, primary_sales_channel, division, category, subcategory, brand).
- Deliver SQL + YAML artifacts with descriptions/tests so Power BI can connect directly to dbt outputs.

## Implementation Steps
1. **stg_mba_order_items**
   - Source `fact_commercial`.
   - Keep `transaction_type = 'Sale'`, drop NULL order/product IDs, dedupe `(unified_order_id, product_id)`.
   - Output: one row per unique product per order with division/category/subcategory/brand attributes.
   - Document via YAML with not-null tests on `unified_order_id` and `product_id`.

2. **fct_product_pairs**
   - Self-join `stg_mba_order_items` on `unified_order_id` with `product_id_left < product_id_right`.
   - Emit both product attribute sets and `pair_order_count = COUNT DISTINCT unified_order_id`.
   - Add YAML describing pair fields and ensuring not-null pair IDs/counts.

3. **fct_product_pair_metrics + dim_items update**
   - Calculate `total_orders` and product-level order counts.
   - Join with pair counts to derive `support`, `confidence_p1_to_p2`, `confidence_p2_to_p1`.
   - Union product occurrences (from both pair positions) to compute `mba_support_score` per item and left join into `dim_items` with `COALESCE(0)`.
   - Document new metrics/column in YAML.

4. **mart_pareto_items**
   - Required CTE flow: `base_items`, `aggregated_revenue_per_dimension`, `ranking_window_calculations`, `final`.
   - Calculate revenue, dense rank, cumulative sums, and cumulative percentages for each of the six dimensions (global, channel, division, category, subcategory, brand), yielding 18 columns.
   - Provide YAML with column descriptions and not-null tests where applicable.

## Testing & Documentation
- Use `models/mba/mba_models.yml` to house descriptions and tests for all new models plus the `dim_items` enhancement.
- Run `dbt run --select stg_mba_order_items fct_product_pairs fct_product_pair_metrics dim_items mart_pareto_items` followed by `dbt test` to validate.

## Power BI Consumption
- Power BI should import:
  - `stg_mba_order_items` (order-level product list) for order-item counts.
  - `fct_product_pair_metrics` (support/confidence) for basket visuals.
  - `mart_pareto_items` for Pareto visualizations across multiple dimensions.
  - Updated `dim_items` to expose `mba_support_score` for ranking.

This plan keeps Power BI models lean while centralizing complex logic in dbt with fully documented contracts.

