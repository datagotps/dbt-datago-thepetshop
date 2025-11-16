# Feature Name: Pethaus, TPS Café, and Service Revenue Data Integration

## 1. Business Purpose
This feature consolidates sales data from **three separate sales channels** into a unified commercial data model, enabling complete visibility of company-wide revenue across multiple systems:

- **Pethaus**: Sales from two separate ERP systems (Domestic Grooming and General Trading)
- **TPS Café**: Sales from an independent café ERP system
- **Service Revenue**: Non-stockable service products (grooming services) that don't appear in the value entry table

**Why it was built**: The company operates multiple business entities with separate ERP systems. Previously, service revenue was missing from reporting because service products are non-stockable and don't create inventory value entries.

**Business Value**: Provides complete financial picture across all revenue streams in one master commercial model (`fact_commercial`), enabling accurate cross-channel reporting, revenue analysis, and performance tracking.

**Dashboard Usage**: Powers commercial dashboards showing sales by channel, location, service type, and consolidated revenue metrics across all business units.

## 2. Technical Overview (dbt)

**Models Created/Modified**:

*Staging Layer - Pethaus (Separate ERPs):*
- `stg_pethaus_value_entry` - Unions value entries from Domestic Grooming + General Trading ERPs
- `stg_pethaus_trans__sales_entry` - Transaction-level sales from both Pethaus ERPs
- `stg_pethaus_staff` - Staff dimension for Pethaus

*Staging Layer - TPS Café (Separate ERP):*
- `stg_petshop_cafe_value_entry` - Café value entries with dimension joins
- `stg_petshop_cafe_dimension_value` - Café dimension master data

*Staging Layer - Service Revenue:*
- `stg_erp_trans__sales_entry` - Base sales entry extraction
- `int_erp_trans__sales_entry` - Filters service products (retail codes: 31024, 31010, 31011, 31012, 31113, 31114)

*Integration & Mart Layer:*
- `stg_value_entry` - **Main integration point**: UNION ALL of 4 sources (Petshop, Pethaus, Café, Service Revenue)
- `int_value_entry` - Enrichment with customer, item, order data
- `int_order_lines` - Order-level transformations and metrics
- `int_commercial` - Commercial business logic
- `fact_commercial` - Final analytical model

**Upstream Sources**:
- `pethaus_domestic_grooming_value_entry` + `pethaus_general_trading_value_entry`
- `pethaus_domestic_grooming_trans__sales_entry` + `pethaus_general_trading_trans__sales_entry`
- `the_petshop_cafe_value_entry` + `the_petshop_cafe_dimension_value`
- `petshop_trans__sales_entry` (for service revenue)
- `petshop_value_entry` (main Petshop ERP)

**Downstream Consumers**:
- Commercial BI dashboards
- Revenue reporting and analytics
- Cross-channel performance analysis

**Key SQL Logic**:
- **4-way UNION**: Petshop + Pethaus + Café + Service Revenue sources unified via `stg_value_entry`
- **Service Revenue Logic**: Maps `trans__sales_entry` fields to value entry schema using extensive CAST transformations
- **Company Source Tagging**: Each source tagged with `company_source` ('Pet Shop', 'Pet Haus', 'TPS Café', 'Pet Shop Services')
- **Dimension Mapping**: Special handling for dimension 2 (profit centers), including "Now Now" → "Noon" normalization
- **Service Product Filtering**: WHERE `retail_product_code IN ('31024','31010','31011','31012','31113','31114')`
- **Store Classification**: MOBILE → 'Mobile Grooming', GRM → 'Shop Grooming'
- **Cost Exclusion**: Service revenue (categories 310, 311) set `cost_amount__actual_ = 0` to avoid COGS on non-stockable items

**KPI Definitions Created**:
- `company_source` - Channel identifier for multi-ERP reporting
- `sales_channel` - Online, Shop, Affiliate, B2B, Service
- `transaction_type` - Sale vs Refund classification
- `sales_amount_gross` / `sales_amount__actual_` - Revenue metrics across all channels
- `clc_global_dimension_2_code_name` - Unified profit center dimension

## 3. Model Lineage (high-level)

```
┌─ Source: petshop_value_entry (Main ERP)
│
├─ Source: pethaus_value_entry (2 ERPs) ──► stg_pethaus_value_entry
│
├─ Source: cafe_value_entry ──────────────► stg_petshop_cafe_value_entry
│                                                        │
└─ Source: petshop_trans__sales_entry ────► int_erp_trans__sales_entry (Service)
                                                         │
                                                         ▼
                                             ┌─── stg_value_entry (UNION ALL)
                                             │
                                             ▼
                                         int_value_entry (+ joins: customer, items, orders)
                                             │
                                             ▼
                                         int_order_lines (+ discount, gross sales calc)
                                             │
                                             ▼
                                         int_commercial
                                             │
                                             ▼
                                         fact_commercial (BI Layer)
```

## 4. Important Fields Added

| Field | Description |
|-------|-------------|
| `company_source` | Source ERP identifier: 'Pet Shop', 'Pet Haus', 'TPS Café', 'Pet Shop Services' |
| `sales_channel` | Categorizes transactions: Online, Shop, Affiliate, B2B, Service |
| `clc_global_dimension_2_code_name` | Unified profit center/channel name (e.g., Noon, Instashop, Mobile Grooming) |
| `clc_store_no_` | Service location: 'Mobile Grooming' or 'Shop Grooming' |
| `retail_product_code` | Service type identifier (31024=Add-on, 31010=Bird, 31011=Cat, 31012=Dog, 31113=Mobile Cat, 31114=Mobile Dog) |
| `sales_amount_gross` | Gross sales before discounts across all channels |
| `offline_order_channel` | Store code for POS transactions |

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `6f818cd` | Anmar Abbas DataGo | 2025-10-06 | **Initial implementation**: Created Pethaus staging models, service revenue integration, added sources, modified stg_value_entry to union all 4 sources |
| `6cd3a1f` | Anmar Abbas DataGo | 2025-10-15 | Added TPS Café staging models (value_entry, dimension_value), updated int_value_entry and fact_commercial |
| `2fc13e8` | Anmar Abbas DataGo | 2025-10-15 | Refinements to value entry integration logic |
| `7b86fa3` | Anmar Abbas DataGo | 2025-10-25 | Latest updates to commercial model integration |

**Key Changes in Commits**:
- **6f818cd**: Added 10+ source tables for Pethaus (2 ERPs), created service revenue mapping from trans_sales_entry, updated stg_value_entry with 4-source union
- **6cd3a1f**: Integrated TPS Café as 3rd channel, updated 18 models including int_order_lines, fact_commercial, dim_customers
- Service revenue logic maps transaction table to value entry schema with extensive type casting

## 6. Limitations / Assumptions

**Assumptions**:
- Service products identified by specific retail codes (31024, 31010-31012, 31113-31114) remain consistent
- "Now Now" affiliate partner name mapped to standardized "Noon"
- Service revenue from `trans__sales_entry` contains complete sales data (no missing transactions)
- All three ERP systems use compatible chart of accounts and dimension structures
- MOBILE and GRM store codes reliably identify mobile vs shop grooming

**Limitations**:
- Service revenue has `item_ledger_entry_no_ = NULL` (no inventory ledger link)
- Cost tracking limited for service products (categories 310, 311 excluded from COGS)
- No real-time sync - depends on Fivetran replication schedules for each ERP
- Pethaus combines two separate legal entities (Domestic Grooming + General Trading) - may need separate reporting

**Future Improvements**:
- Add data quality tests for cross-system customer matching
- Implement service-specific KPIs (appointment rates, grooming service mix)
- Add reconciliation checks between trans_sales_entry and value_entry for stockable items
- Consider implementing incremental models for performance at scale
- Add generic tests for company_source values and retail_product_code coverage
