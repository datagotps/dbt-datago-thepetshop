# Commit Documentation: October 6, 2025
## Pethaus Data Integration & Major dbt Refactoring

---

## Commit Information

| Field | Details |
|-------|---------|
| **Commit Hash** | `6f818cd1b3e148fd8cb6995a493932f46e15956d` |
| **Date** | Monday, October 6, 2025 at 11:09:36 UTC |
| **Author** | Anmar Abbas DataGo (anmar@8020datago.ai) |
| **Message** | "6 Oct 2025" |
| **Files Changed** | 49 files |
| **Insertions** | +4,698 lines |
| **Deletions** | -5,155 lines |
| **Net Change** | -457 lines (code consolidation and cleanup) |

---

## Executive Summary

This commit represents a major milestone in the dbt-datago-thepetshop project, introducing **Pethaus retail operations data integration** into the analytics platform. The update consolidates two Pethaus business units (Domestic Grooming and General Trading) into unified staging models, enabling comprehensive reporting across all The Pet Shop retail operations.

Additionally, this commit includes significant refactoring across the dbt project, improving code organization, removing deprecated models, and enhancing data quality through better staging layer design.

---

## 1. NEW FEATURE: Pethaus Data Integration

### 1.1 Overview

Pethaus is a retail operations division within The Pet Shop ecosystem. This implementation creates staging models to consolidate data from two separate Pethaus entities:

1. **Pethaus Domestic Grooming** - Grooming services operations
2. **Pethaus General Trading** - Retail trading operations

### 1.2 New Models Created

Three new staging models were added to `/models/1_stg/_pethaus/`:

#### **A. stg_pethaus_staff.sql**
- **Purpose**: Unified staff/employee master data across both Pethaus entities
- **Data Sources**:
  - `pethaus_domestic_grooming_staff_5ecfc871_5d82_43f1_9c54_59685e82318d`
  - `pethaus_general_trading_staff_5ecfc871_5d82_43f1_9c54_59685e82318d`
- **Key Fields** (96 columns total):
  - Employee identifiers: `id`, `payroll_no_`, `sales_person`
  - Personal information: `first_name`, `last_name`, `address`, `city`, `post_code`
  - Contact details: `work_phone_no_`, `home_phone_no_`
  - Employment: `employment_type`, `hourly_rate`, `store_no_`
  - POS permissions: `manager_privileges`, `permission_group`, `pos_menu_profile`
  - Security settings: `password`, `blocked`, `privacy_blocked`
  - Operational permissions: Multiple flags for POS operations (void transaction, price override, add payment, etc.)
  - Reporting: `last_y_report`, `last_z_report`
  - Metadata: `_fivetran_deleted`, `_fivetran_synced`, `timestamp`

- **Design Pattern**: UNION ALL approach to combine both entities into single unified view
- **Use Cases**:
  - Staff performance reporting
  - Labor cost analysis
  - POS access control
  - Staff scheduling and productivity

---

#### **B. stg_pethaus_trans__sales_entry.sql**
- **Purpose**: Consolidated Point of Sale (POS) transaction line items
- **Data Sources**:
  - `pethaus_domestic_grooming_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d`
  - `pethaus_general_trading_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d`
- **Key Fields** (127 columns total):
  - Transaction identifiers: `transaction_no_`, `receipt_no_`, `line_no_`, `store_no_`, `pos_terminal_no_`
  - Date/time: `date`, `trans__date`, `trans__time`, `shift_date`, `shift_no_`
  - Item details: `item_no_`, `barcode_no_`, `item_category_code`, `variant_code`
  - Quantities: `quantity`, `uom_quantity`, `refund_qty_`
  - Pricing: `price`, `net_price`, `net_amount`, `uom_price`, `cost_amount`
  - Discounts: `discount_amount`, `total_discount`, `line_discount`, `periodic_discount`, `customer_discount`, `coupon_discount`
  - Tax/VAT: `vat_amount`, `vat_code`, `vat_calculation_type`, `vat_bus__posting_group`
  - Staff tracking: `staff_id`, `sales_staff`, `created_by_staff_id`
  - Customer info: `customer_no_`, `cust__invoice_discount`
  - Promotions: `deal_line`, `promotion_no_`, `deal_header_line_no_`
  - Returns/refunds: `refunded_trans__no_`, `refunded_line_no_`, `refunded_store_no_`
  - Loyalty: `member_points`, `member_points_type`
  - Metadata: `_fivetran_deleted`, `_fivetran_synced`, `timestamp`

- **Design Pattern**: UNION ALL to consolidate transaction lines from both entities
- **Use Cases**:
  - Daily sales reporting
  - Item-level sales analysis
  - Discount effectiveness tracking
  - Staff performance by sales
  - Promotion analysis
  - Returns and refunds tracking
  - Customer purchase behavior

---

#### **C. stg_pethaus_value_entry.sql**
- **Purpose**: Inventory valuation and financial entries with dimension enrichment
- **Data Sources**:
  - `pethaus_domestic_grooming_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`
  - `pethaus_general_trading_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`
  - Dimension value tables for enrichment
- **Key Fields** (106 columns total):
  - Entry identifiers: `entry_no_`, `item_ledger_entry_no_`, `document_no_`
  - Item details: `item_no_`, `description`, `variant_code`, `location_code`
  - Dates: `posting_date`, `document_date`, `valuation_date`
  - Quantities: `item_ledger_entry_quantity`, `invoiced_quantity`, `valued_quantity`
  - Costs: `cost_amount__actual_`, `cost_amount__expected_`, `cost_per_unit`
  - Sales amounts: `sales_amount__actual_`, `sales_amount__expected_`
  - Purchase amounts: `purchase_amount__actual_`, `purchase_amount__expected_`
  - G/L posting: `cost_posted_to_g_l`, `expected_cost_posted_to_g_l`
  - Document info: `document_type`, `order_no_`, `order_line_no_`, `external_document_no_`
  - Categorization: `entry_type`, `item_ledger_entry_type`, `source_type`, `source_code`
  - Dimensions: `global_dimension_1_code`, `global_dimension_2_code`, `dimension_set_id`
  - **Enriched fields**:
    - `dimension_code`
    - `global_dimension_2_code_name`
    - `clc_global_dimension_2_code_name` - Clean dimension name with business rule: "Now Now" → "Noon"
  - Posting groups: `gen__bus__posting_group`, `gen__prod__posting_group`, `inventory_posting_group`
  - Job tracking: `job_no_`, `job_task_no_`, `job_ledger_entry_no_`
  - Adjustments: `adjustment`, `variance_type`, `partial_revaluation`
  - Financial: `discount_amount`, `inventoriable`, `valued_by_average_cost`
  - Metadata: `_fivetran_deleted`, `_fivetran_synced`, `timestamp`, `user_id`

- **Design Pattern**:
  - UNION ALL for consolidation
  - LEFT JOIN to dimension tables for enrichment
  - Business logic transformation (Now Now → Noon)
- **Use Cases**:
  - Inventory valuation reporting
  - COGS (Cost of Goods Sold) analysis
  - Margin analysis by dimension
  - Financial statement preparation
  - Inventory movement tracking
  - Job costing
  - Variance analysis

---

### 1.3 Data Architecture Pattern

All three Pethaus models follow a consistent architectural pattern:

```sql
WITH source_domestic AS (
    SELECT * FROM {{ source('sql_erp_prod_dbo', 'pethaus_domestic_grooming_[table]') }}
),

source_general AS (
    SELECT * FROM {{ source('sql_erp_prod_dbo', 'pethaus_general_trading_[table]') }}
),

renamed AS (
    SELECT [columns] FROM source_domestic
    UNION ALL
    SELECT [columns] FROM source_general
)

SELECT * FROM renamed
```

**Benefits**:
- Single source of truth for Pethaus data
- Consistent schema across both entities
- Simplified downstream transformations
- Centralized data quality rules
- Efficient query patterns

---

### 1.4 Source System Integration

The Pethaus data is sourced from the ERP system via Fivetran:

- **Database**: `tps-data-386515` (BigQuery)
- **Schema**: `sql_erp_prod_dbo`
- **Sync Method**: Fivetran incremental replication
- **CDC Tracking**: `_fivetran_synced`, `_fivetran_deleted` fields

---

## 2. Additional Major Changes

### 2.1 Budget Management Models (NEW)

Two new budget-related models were added:

- **`stg_budget.sql`** - Staging layer for budget data
- **`int_budget.sql`** - Intermediate budget transformations
- **`fct_budget.sql`** - Budget fact table for reporting

**Source**: `tps_budget_target` table

---

### 2.2 Enhanced Item Management

New and improved item models:

- **`dim_items.sql`** (NEW) - Item dimension table for analytics
- **`int_items_2.sql`** (NEW) - Alternative item transformation logic
- **`int_purchase_line.sql`** (NEW) - Enhanced purchase line intermediate model

---

### 2.3 Value Entry Refactoring

Significant restructuring of value entry models:

**Removed**:
- `stg_erp_value_entry.sql` (deleted)
- `stg_erp_value_entry.yml` (deleted)

**Added**:
- `discount_ledger_entry.sql` - Dedicated discount tracking
- `stg_value_entry_2.sql` - Alternative value entry staging

**Modified**:
- `stg_value_entry.sql` - Enhanced with additional logic
- `int_value_entry.sql` - Significant refactoring (377 lines modified)

---

### 2.4 Customer Models Refactoring

**Removed deprecated models**:
- `customergold.sql` (deleted)
- `customergold2.sql` (deleted)

**Simplified**:
- `int_customers.sql` - Reduced complexity from 1,293 lines
- `dim_customers.sql` - Streamlined customer dimension (662 lines reduction)

---

### 2.5 Order Processing Enhancements

**New models**:
- `int_order_lines.sql` - Dedicated order line transformations
- `int_inbound_sales_header.sql` - Inbound sales header intermediate model

**Modified**:
- `int_orders.sql` - Refactored order logic
- `fact_orders.sql` - Updated fact table
- `int_occ_order_items.sql` - Online order items improvements

---

### 2.6 Procurement Models

**Removed**:
- `int_petshop_purchase_header.sql`
- `int_petshop_purchase_line.sql`

**Replaced with**:
- `int_purchase_line.sql` - Consolidated purchase line logic
- `fct_procurement.sql` - New procurement fact table

---

### 2.7 New Fact Tables

Three new fact tables were added:

1. **`fct_budget.sql`** - Budget vs. actual analysis
2. **`fct_daily_transactions.sql`** - Daily transaction summary
3. **`fct_procurement.sql`** - Procurement analytics

---

### 2.8 Commercial & Analysis Models

**Removed**:
- `analyses/_commercial_model.sql` (321 lines deleted)
- `analyses/customer_model.sql` (1,255 lines deleted)

**Simplified**:
- `int_commercial.sql` - Reduced complexity
- `fact_commercial.sql` - Updated commercial fact table

---

### 2.9 Sources Configuration

Updated `models/_sources.yml` to include new Pethaus source tables:

```yaml
- name: pethaus_domestic_grooming_staff_5ecfc871_5d82_43f1_9c54_59685e82318d
- name: pethaus_general_trading_staff_5ecfc871_5d82_43f1_9c54_59685e82318d
- name: pethaus_domestic_grooming_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d
- name: pethaus_general_trading_trans__sales_entry_5ecfc871_5d82_43f1_9c54_59685e82318d
- name: pethaus_domestic_grooming_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972
- name: pethaus_general_trading_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972
- name: pethaus_domestic_grooming_dimension_value_437dbf0e_84ff_417a_965d_ed2bb9650972
- name: pethaus_general_trading_dimension_value_437dbf0e_84ff_417a_965d_ed2bb9650972
```

---

## 3. Technical Impact Analysis

### 3.1 Model Count Changes

| Category | Before | After | Change |
|----------|--------|-------|--------|
| Staging Models | Unknown | +5 | New: Pethaus (3) + Budget (2) |
| Intermediate Models | Unknown | +3 | New order/item models |
| Fact Models | Unknown | +4 | New: dim_items, 3 fact tables |
| Analysis Files | 2+ | 0 | Moved to proper models |
| **Total Files** | - | - | Net: -457 lines |

### 3.2 Code Quality Improvements

**Deletions Summary**:
- Removed 2 large analysis files (1,576 lines)
- Deleted deprecated customer models (156 lines)
- Removed old value entry staging (184 lines + 271 lines code)
- Consolidated purchase models

**Result**: Cleaner, more maintainable codebase with better separation of concerns

---

### 3.3 Data Coverage Expansion

New data domains now available:

1. **Pethaus Retail Operations**
   - Staff management and labor costs
   - POS transactions (grooming + trading)
   - Inventory valuation for Pethaus entities

2. **Budget Management**
   - Budget planning
   - Budget vs. actual variance analysis

3. **Enhanced Procurement**
   - Dedicated procurement analytics
   - Purchase order tracking improvements

---

### 3.4 Performance Considerations

**Potential Impacts**:
- Union operations in Pethaus models may increase query time for large datasets
- New fact tables provide pre-aggregated views for faster reporting
- Dimension enrichment in `stg_pethaus_value_entry` adds join overhead but improves usability

**Recommendations**:
- Monitor execution time for Pethaus staging models
- Consider incremental materialization for large fact tables
- Add appropriate indexes if supported by target warehouse

---

## 4. Downstream Dependencies

### 4.1 Pethaus Models Usage

The three new Pethaus staging models can be referenced downstream as:

```sql
{{ ref('stg_pethaus_staff') }}
{{ ref('stg_pethaus_trans__sales_entry') }}
{{ ref('stg_pethaus_value_entry') }}
```

### 4.2 Potential Use Cases

**Finance Team**:
- Consolidated P&L across all retail entities (PetShop + Pethaus)
- COGS and margin analysis by business unit
- Budget tracking and variance reporting

**Operations Team**:
- Labor cost analysis (staff productivity)
- Sales performance by store/staff/shift
- Inventory turnover analysis

**Analytics Team**:
- Customer segmentation across all channels
- Product performance comparison
- Promotion effectiveness analysis

---

## 5. Testing & Validation Recommendations

### 5.1 Data Quality Checks

**For Pethaus Models**:
1. Verify row counts match sum of individual entity tables
2. Check for duplicate records after UNION ALL
3. Validate primary keys (staff ID, transaction numbers, entry numbers)
4. Confirm all fiscal periods represented
5. Test Fivetran sync status fields

### 5.2 Business Logic Validation

**stg_pethaus_value_entry**:
- Verify "Now Now" → "Noon" transformation works correctly
- Validate dimension joins return expected values
- Check cost calculations match source system

**stg_pethaus_trans__sales_entry**:
- Validate net_amount calculations
- Verify discount aggregations
- Test refund/return linkages

---

## 6. Migration Notes

### 6.1 Breaking Changes

**Removed Models** (downstream references will break):
- `analyses/_commercial_model`
- `analyses/customer_model`
- `customergold`
- `customergold2`
- `int_petshop_purchase_header`
- `int_petshop_purchase_line`
- `stg_erp_value_entry`

**Action Required**: Update any dashboards or downstream processes referencing these models

---

### 6.2 New Model Adoption

Teams should begin using:
- `dim_items` instead of older item references
- `fct_budget` for budget reporting
- `fct_daily_transactions` for daily operational reports
- `fct_procurement` for procurement analytics

---

## 7. Documentation & Metadata

### 7.1 Missing Documentation

The following should be added in future commits:

1. **Schema documentation** for Pethaus models
2. **Column descriptions** in YAML files
3. **Business logic documentation** for transformations
4. **Data lineage diagrams**
5. **Testing specifications**

---

### 7.2 Recommended Next Steps

1. Add dbt tests for Pethaus models:
   - Unique/not null tests on primary keys
   - Relationship tests to dimension tables
   - Accepted values tests for categorical fields
   - Custom data quality tests

2. Create schema YAML files:
   - `models/1_stg/_pethaus/schema.yml`
   - Document all 3 models with column descriptions

3. Add data freshness tests:
   - Monitor Fivetran sync timestamps
   - Alert on stale data

4. Build intermediate/fact models:
   - `int_pethaus_sales` - Aggregated sales metrics
   - `fct_pethaus_operations` - Operational KPIs
   - Integration with existing customer/item dimensions

---

## 8. Related Commits

This commit builds upon and may be related to:
- Previous commits setting up Pethaus source connections
- Future commits that will leverage these staging models
- Ongoing refactoring initiative to improve dbt project structure

---

## 9. Appendix

### 9.1 File Change Summary

**Added Files** (15):
```
models/1_stg/_pethaus/stg_pethaus_staff.sql
models/1_stg/_pethaus/stg_pethaus_trans__sales_entry.sql
models/1_stg/_pethaus/stg_pethaus_value_entry.sql
models/1_stg/2_value_entry/discount_ledger_entry.sql
models/1_stg/2_value_entry/stg_value_entry_2.sql
models/1_stg/8_procurement/1_purchase_orders/int_purchase_line.sql
models/1_stg/int_budget.sql
models/1_stg/stg_budget.sql
models/2_int/0_final/int_order_lines.sql
models/2_int/5_item/int_items_2.sql
models/2_int/int_inbound_sales_header.sql
models/3_fct/dim_items.sql
models/3_fct/fct_budget.sql
models/3_fct/fct_daily_transactions.sql
models/3_fct/fct_procurement.sql
```

**Deleted Files** (8):
```
analyses/_commercial_model.sql
analyses/customer_model.sql
models/1_stg/2_value_entry/stg_erp_value_entry.sql
models/1_stg/2_value_entry/stg_erp_value_entry.yml
models/1_stg/4_customer/customergold.sql
models/1_stg/4_customer/customergold2.sql
models/1_stg/8_procurement/1_purchase_orders/int_petshop_purchase_header.sql
models/1_stg/8_procurement/1_purchase_orders/int_petshop_purchase_line.sql
```

**Modified Files** (26):
```
models/1_stg/1_order/1_order_online/1_ofs/stg_ofs_inboundpaymentline.sql
models/1_stg/1_order/1_order_online/1_ofs/stg_ofs_inboundsalesheader.sql
models/1_stg/1_order/1_order_online/1_ofs/stg_ofs_inboundsalesline.sql
models/1_stg/1_order/1_order_online/1_ofs/stg_ofs_orderdataanalysis.sql
models/1_stg/1_order/1_order_online/2_erp/stg_erp_inbound_sales_header.sql
models/1_stg/1_order/1_order_online/2_erp/stg_erp_inbound_sales_line.sql
models/1_stg/1_order/3_order_store_&_petgr/stg_erp_trans__sales_entry.sql
models/1_stg/1_order/3_order_store_&_petgr/stg_erp_transaction_header.sql
models/1_stg/2_value_entry/int_value_entry.sql
models/1_stg/2_value_entry/stg_value_entry.sql
models/1_stg/4_customer/2_erp/int_erp_customer.sql
models/1_stg/5_item/stg_petshop_item.sql
models/1_stg/8_procurement/1_purchase_orders/stg_petshop_purchase_line.sql
models/2_int/0_final/int_commercial.sql
models/2_int/0_final/int_customers.sql
models/2_int/0_final/int_orders.sql
models/2_int/1_order/1_order_online/int_occ_order_items.sql
models/2_int/2_value_entry/int_erp_occ_invoice_items.sql
models/2_int/2_value_entry/int_erp_occ_invoices.sql
models/2_int/5_item/int_items.sql
models/2_int/7_operation/int_ofs_location_level.sql
models/3_fct/dim_customers.sql
models/3_fct/fact_commercial.sql
models/3_fct/fact_orders.sql
models/3_fct/mysql_ofs/fct_occ_order_items.sql
models/_sources.yml
```

---

### 9.2 Contact & Support

**For questions about this commit:**
- Author: Anmar Abbas DataGo (anmar@8020datago.ai)
- Date: October 6, 2025

**For technical support:**
- DataGo Analytics Team
- dbt Project: dbt-datago-thepetshop
- Repository: datagotps/dbt-datago-thepetshop

---

**Document Version**: 1.0
**Created**: November 15, 2025
**Last Updated**: November 15, 2025
**Status**: Final
